// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/flight/transport/ucx/ucx_internal.h"

#include <atomic>
#include <mutex>
#include <queue>
#include <thread>

#include <arpa/inet.h>
#include <ucp/api/ucp.h>

#include "arrow/buffer.h"
#include "arrow/flight/server.h"
#include "arrow/flight/transport_impl.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

namespace {
class UcxServerCallContext : public flight::ServerCallContext {
 public:
  const std::string& peer_identity() const override { return peer_; }
  const std::string& peer() const override { return peer_; }
  ServerMiddleware* GetMiddleware(const std::string& key) const override {
    return nullptr;
  }
  bool is_cancelled() const override { return false; }

 private:
  std::string peer_;
};
}  // namespace

void HandleIncomingConnection(ucp_conn_request_h connection_request, void* server);

void StreamRecvCallback(void* request, ucs_status_t status, size_t length,
                        void* user_data) {
  ARROW_LOG(WARNING) << "Got message of length " << length;
}

class ARROW_FLIGHT_EXPORT UcxServerImpl
    : public arrow::flight::internal::ServerTransportImpl {
 public:
  UcxServerImpl() : service_(nullptr) {}

  virtual ~UcxServerImpl() {
    // TODO: ensure UCX is shut down
  }

  Status Init(const FlightServerOptions& options, const arrow::internal::Uri& uri,
              FlightServerBase* server) {
    service_ = server;

    // Init UCX
    {
      ucp_config_t* ucp_config;
      ucp_params_t ucp_params;
      ucs_status_t status;

      RETURN_NOT_OK(FromUcsStatus("ucp_config_read",
                                  ucp_config_read(nullptr, nullptr, &ucp_config)));

      std::memset(&ucp_params, 0, sizeof(ucp_params));
      ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
      // NOTE: sending data hangs without WAKEUP, why?
      ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM | UCP_FEATURE_WAKEUP;

      status = ucp_init(&ucp_params, ucp_config, &ucp_context_);
      ucp_config_release(ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_init", status));

      ucp_worker_params_t worker_params;
      std::memset(&worker_params, 0, sizeof(worker_params));
      worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
      worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

      // Create one worker to listen for incoming connections.
      status = ucp_worker_create(ucp_context_, &worker_params, &worker_conn_);
      RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));

      // Create another worker to actually service requests.
      status = ucp_worker_create(ucp_context_, &worker_params, &worker_service_);
      RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));
    }

    // Start listening for connections.
    {
      ucp_listener_params_t params;
      ucs_status_t status;

      sockaddr listen_addr;
      UriToSockaddr(uri, &listen_addr);

      params.field_mask =
          UCP_LISTENER_PARAM_FIELD_SOCK_ADDR | UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
      params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&listen_addr);
      params.sockaddr.addrlen = sizeof(listen_addr);
      params.conn_handler.cb = HandleIncomingConnection;
      params.conn_handler.arg = this;

      status = ucp_listener_create(worker_conn_, &params, &listener_);
      RETURN_NOT_OK(FromUcsStatus("ucp_listener_create", status));

      // Get the real address/port
      ucp_listener_attr_t attr;
      attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
      status = ucp_listener_query(listener_, &attr);
      RETURN_NOT_OK(FromUcsStatus("ucp_listener_query", status));

      std::string raw_uri = "ucx://";
      raw_uri += uri.host();
      raw_uri += ":";
      raw_uri += std::to_string(
          ntohs(reinterpret_cast<const sockaddr_in*>(&attr.sockaddr)->sin_port));
      RETURN_NOT_OK(Location::Parse(raw_uri, &location_));
    }

    {
      running_.test_and_set();
      std::thread listener_thread(&UcxServerImpl::DriveWorker, this);
      listener_thread_.swap(listener_thread);
    }

    return Status::OK();
  }

  Status Shutdown() override {
    // TODO: determine if server was running in the first place
    running_.clear();
    ucp_listener_destroy(listener_);
    RETURN_NOT_OK(Wait());
    // ucp_state_.Close();
    return Status::OK();
  }

  Status Wait() override {
    try {
      listener_thread_.join();
    } catch (const std::system_error& e) {
      if (e.code() == std::errc::invalid_argument) {
        return Status::Invalid("Cannot Wait() on server that is not running: ", e.what());
      }
      return Status::UnknownError("Could not Wait(): ", e.what());
    }
    return Status::OK();
  }

  Location location() const override { return location_; }

 private:
  friend void HandleIncomingConnection(ucp_conn_request_h, void*);

  Status CompleteRequestBlocking(void* request, const std::string& context) {
    if (UCS_PTR_IS_ERR(request)) {
      return FromUcsStatus(context, UCS_PTR_STATUS(request));
    } else if (UCS_PTR_IS_PTR(request)) {
      // TODO: callback based mode
      while (true) {
        auto status = ucp_request_check_status(request);
        if (status == UCS_OK) {
          break;
        } else if (status != UCS_INPROGRESS) {
          ucp_request_release(request);
          return FromUcsStatus("ucp_request_check_status", status);
        }
        ucp_worker_progress(worker_service_);
      }
      ucp_request_release(request);
    } else {
      // Send was completed instantly
      DCHECK(!request);
    }
    return Status::OK();
  }

  Status HandleOneCall(ucp_ep_h client_endpoint) {
    UcpCallDriver driver(worker_service_, client_endpoint);
    UcxServerCallContext context;

    // Get method
    ARROW_ASSIGN_OR_RAISE(auto payload, driver.ReadNextPayload());
    if (payload->ToString() != "arrow.flight.protocol.FlightService/GetFlightInfo") {
      // TODO: send error to client
      return Status::NotImplemented(payload->ToString());
    }

    // Get payload
    ARROW_ASSIGN_OR_RAISE(payload, driver.ReadNextPayload());
    FlightDescriptor descriptor;
    RETURN_NOT_OK(FlightDescriptor::Deserialize(payload->ToString(), &descriptor));
    ARROW_LOG(WARNING) << "Descriptor: " << descriptor.ToString();

    std::unique_ptr<FlightInfo> info;
    // TODO: send error to client
    RETURN_NOT_OK(service_->GetFlightInfo(context, descriptor, &info));

    // Send response to client
    std::string response;
    RETURN_NOT_OK(info->SerializeToString(&response));

    RETURN_NOT_OK(driver.SendPayload(reinterpret_cast<const uint8_t*>(response.data()),
                                     static_cast<int64_t>(response.size())));
    return Status::OK();
  }

  void DriveWorker() {
    while (running_.test_and_set()) {
      ucp_worker_progress(worker_conn_);

      // Check for connect requests in queue
      std::unique_lock<std::mutex> guard(pending_connections_mutex_);
      while (!pending_connections_.empty()) {
        ucp_conn_request_h request = pending_connections_.front();
        pending_connections_.pop();

        // Create an endpoint to the client, using the data worker
        ucp_ep_params_t params;
        params.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST;
        params.conn_request = request;
        ucs_status_t status;
        ucp_ep_h client_endpoint;

        status = ucp_ep_create(worker_service_, &params, &client_endpoint);
        if (status != UCS_OK) {
          ReportError(FromUcsStatus("ucp_ep_create", status));
          continue;
        }

        // Drive the connection (TODO: what would *actually* happen is
        // we hand this all off to another thread)

        {
          auto status = HandleOneCall(client_endpoint);
          if (!status.ok()) {
            ReportError(std::move(status));
          }
        }

        {
          // Close the connection
          ucp_request_param_t request_param;
          void* request = ucp_ep_flush_nbx(client_endpoint, &request_param);
          if (UCS_PTR_IS_ERR(request)) {
            ReportError(FromUcsStatus("ucp_ep_flush_nbx", UCS_PTR_STATUS(request)));
            continue;
          } else if (UCS_PTR_IS_PTR(request)) {
            ucs_status_t status;
            do {
              ucp_worker_progress(worker_service_);
              status = ucp_request_check_status(request);
            } while (status == UCS_INPROGRESS);
            ucp_request_free(request);
          } else {
            DCHECK(!request);
          }
          request = ucp_ep_close_nb(client_endpoint, UCP_EP_CLOSE_MODE_FLUSH);
          if (UCS_PTR_IS_ERR(request)) {
            ReportError(FromUcsStatus("ucp_ep_close_nb", UCS_PTR_STATUS(request)));
            continue;
          } else if (UCS_PTR_IS_PTR(request)) {
            ucs_status_t status;
            do {
              ucp_worker_progress(worker_service_);
              status = ucp_request_check_status(request);
            } while (status == UCS_INPROGRESS);
            ucp_request_free(request);
          } else {
            DCHECK(!request);
          }
        }
        ARROW_LOG(WARNING) << "Server closed connection";
      }
    }
  }

  void EnqueueClient(ucp_conn_request_h connection_request) {
    std::unique_lock<std::mutex> guard(pending_connections_mutex_);
    pending_connections_.push(connection_request);
  }

  /// Handle errors during server worker loop execution
  void ReportError(Status st) {
    ARROW_LOG(WARNING) << "Error in Flight UCX server loop: " << st.ToString();
  }

  ucp_context_h ucp_context_;
  // Listen for and handle incoming connections
  ucp_worker_h worker_conn_;
  ucp_listener_h listener_;
  // Service RPC requests
  ucp_worker_h worker_service_;
  Location location_;

  FlightServerBase* service_;
  std::atomic_flag running_;
  // TODO: use Arrow pool?
  std::thread listener_thread_;

  std::mutex pending_connections_mutex_;
  std::queue<ucp_conn_request_h> pending_connections_;
};

/// Callback handler. A new client has connected to the server.
void HandleIncomingConnection(ucp_conn_request_h connection_request, void* data) {
  UcxServerImpl* server = reinterpret_cast<UcxServerImpl*>(data);
  server->EnqueueClient(connection_request);
}

std::unique_ptr<arrow::flight::internal::ServerTransportImpl> MakeUcxServerImpl() {
  return arrow::internal::make_unique<UcxServerImpl>();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
