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
#include <list>
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
#include "arrow/util/thread_pool.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

// Send an error to the client and return OK.
// Statuses returned up to the main server loop trigger a kReset instead.
#define SERVER_RETURN_NOT_OK(driver, status) \
  do {                                       \
    ::arrow::Status s = (status);            \
    if (!s.ok()) {                           \
      RETURN_NOT_OK(driver->SendStatus(s));  \
      return ::arrow::Status::OK();          \
    }                                        \
  } while (false)

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

class ARROW_FLIGHT_EXPORT UcxServerImpl
    : public arrow::flight::internal::ServerTransportImpl {
 public:
  UcxServerImpl() : service_(nullptr) {}

  virtual ~UcxServerImpl() {
    if (ucp_context_) {
      auto st = Shutdown();
      if (!st.ok()) {
        ARROW_LOG(WARNING) << "Server did not shut down properly: " << st.ToString();
      }
    }
  }

  Status Init(const FlightServerOptions& options, const arrow::internal::Uri& uri,
              FlightServerBase* server) {
    service_ = server;
    ARROW_ASSIGN_OR_RAISE(rpc_pool_, arrow::internal::ThreadPool::Make(8));

    // Init UCX
    {
      ucp_config_t* ucp_config;
      ucp_params_t ucp_params;
      ucs_status_t status;

      status = ucp_config_read(nullptr, nullptr, &ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_config_read", status));

      std::memset(&ucp_params, 0, sizeof(ucp_params));
      ucp_params.field_mask =
          UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
      // NOTE: sending data hangs without WAKEUP, why?
      ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM | UCP_FEATURE_WAKEUP;
      ucp_params.mt_workers_shared = UCS_THREAD_MODE_MULTI;

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
    Status status;

    // Wait for current RPCs to finish
    running_.clear();
    status &= Wait();

    {
      // Reject all pending connections
      std::unique_lock<std::mutex> guard(pending_connections_mutex_);
      while (!pending_connections_.empty()) {
        status &=
            FromUcsStatus("ucp_listener_reject",
                          ucp_listener_reject(listener_, pending_connections_.front()));
        pending_connections_.pop();
      }
      ucp_listener_destroy(listener_);
      ucp_worker_destroy(worker_conn_);
    }

    // Force cancellation of anything remaining
    ucp_worker_destroy(worker_service_);

    status &= rpc_pool_->Shutdown();
    rpc_pool_.reset();

    ucp_cleanup(ucp_context_);
    ucp_context_ = nullptr;
    return status;
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

  Status HandleGetFlightInfo(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ARROW_ASSIGN_OR_RAISE(auto frame, driver->ReadNextFrame());
    RETURN_NOT_OK(driver->ExpectFrameType(frame, FrameType::kPayload));
    FlightDescriptor descriptor;
    SERVER_RETURN_NOT_OK(
        driver, FlightDescriptor::Deserialize(frame.buffer->ToString(), &descriptor));

    std::unique_ptr<FlightInfo> info;
    // TODO: need to read client's trailers (for cancellations and such), asynchronously
    SERVER_RETURN_NOT_OK(driver, service_->GetFlightInfo(context, descriptor, &info));
    // Send response to client
    std::string response;
    SERVER_RETURN_NOT_OK(driver, info->SerializeToString(&response));
    RETURN_NOT_OK(driver->SendPayload(reinterpret_cast<const uint8_t*>(response.data()),
                                      static_cast<int64_t>(response.size())));
    RETURN_NOT_OK(driver->SendStatus(Status::OK()));
    return Status::OK();
  }

  Status HandleDoGet(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ARROW_ASSIGN_OR_RAISE(auto frame, driver->ReadNextFrame());
    RETURN_NOT_OK(driver->ExpectFrameType(frame, FrameType::kPayload));
    Ticket ticket;
    // TODO: don't allocate a new string
    SERVER_RETURN_NOT_OK(driver, Ticket::Deserialize(frame.buffer->ToString(), &ticket));

    std::unique_ptr<FlightDataStream> response;
    SERVER_RETURN_NOT_OK(driver, service_->DoGet(context, ticket, &response));
    if (!response) {
      RETURN_NOT_OK(driver->SendStatus(Status::KeyError("Flight not found")));
      return Status::OK();
    }

    // Write the schema as the first message in the stream
    FlightPayload payload;
    SERVER_RETURN_NOT_OK(driver, response->GetSchemaPayload(&payload));
    RETURN_NOT_OK(driver->SendFlightPayload(payload));

    // Consume data stream and write out payloads
    while (true) {
      SERVER_RETURN_NOT_OK(driver, response->Next(&payload));
      // End of stream
      if (payload.ipc_message.metadata == nullptr) break;
      RETURN_NOT_OK(driver->SendFlightPayload(payload));
    }
    RETURN_NOT_OK(driver->SendStatus(Status::OK()));
    return Status::OK();
  }

  Status HandleOneCall(UcpCallDriver* driver, Frame frame) {
    RETURN_NOT_OK(driver->ExpectFrameType(frame, FrameType::kHeaders));
    ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Parse(std::move(frame.buffer)));
    ARROW_ASSIGN_OR_RAISE(auto method, headers.Get(":method:"));
    if (method == "arrow.flight.protocol.FlightService/GetFlightInfo") {
      return HandleGetFlightInfo(driver);
    } else if (method == "arrow.flight.protocol.FlightService/DoGet") {
      return HandleDoGet(driver);
    }
    RETURN_NOT_OK(driver->SendStatus(Status::NotImplemented(method)));
    // TODO: must drain messages before continuing
    return Status::OK();
  }

  void WaitForRequestAsync(std::list<UcpCallDriver>::iterator driver) {
    CallbackOptions options;
    options.should_schedule = ShouldSchedule::Always;
    options.executor = rpc_pool_.get();

    struct {
      void operator()(const Status& st) {
        if (st.code() == StatusCode::Cancelled) {
          impl->DisconnectClient(driver);
          return;
        } else if (!st.ok()) {
          impl->ReportError(st);
          impl->DisconnectClient(driver);
          return;
        } else {
          auto status = impl->HandleOneCall(&*driver, driver->MoveLastFrame());
          if (!status.ok()) {
            impl->ReportError(std::move(status));
            impl->DisconnectClient(driver);
            return;
          }
        }
        impl->WaitForRequestAsync(driver);
      }

      UcxServerImpl* impl;
      std::list<UcpCallDriver>::iterator driver;
    } HandleRpc;

    HandleRpc.impl = this;
    HandleRpc.driver = driver;
    HandleRpc.driver->ReadFrameAsync().AddCallback(std::move(HandleRpc), options);
  }

  void DriveWorker() {
    while (running_.test_and_set()) {
      ucp_worker_progress(worker_conn_);
      // TODO: separate thread to progress worker
      ucp_worker_progress(worker_service_);

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

        active_connections_.emplace_back(worker_service_, client_endpoint);
        WaitForRequestAsync(--active_connections_.end());
      }
      while (!pending_close_.empty()) {
        auto client = pending_close_.front();
        pending_close_.pop();
        auto status = client->Close();
        if (!status.ok()) {
          ReportError(std::move(status));
        }
        active_connections_.erase(client);
      }
    }
  }

  void EnqueueClient(ucp_conn_request_h connection_request) {
    std::unique_lock<std::mutex> guard(pending_connections_mutex_);
    pending_connections_.push(connection_request);
  }

  void DisconnectClient(std::list<UcpCallDriver>::iterator client) {
    // Shutting down an endpoint has to be done from the same thread
    // as it was created(?) so just enqueue it here
    std::unique_lock<std::mutex> guard(pending_connections_mutex_);
    pending_close_.push(client);
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
  std::shared_ptr<arrow::internal::ThreadPool> rpc_pool_;
  std::thread listener_thread_;

  std::mutex pending_connections_mutex_;
  std::queue<ucp_conn_request_h> pending_connections_;
  std::list<UcpCallDriver> active_connections_;
  std::queue<std::list<UcpCallDriver>::iterator> pending_close_;
};

/// Callback handler. A new client has connected to the server.
void HandleIncomingConnection(ucp_conn_request_h connection_request, void* data) {
  UcxServerImpl* server = reinterpret_cast<UcxServerImpl*>(data);
  // TODO: enable shedding load above some threshold (which is a
  // pitfall with gRPC/Java)
  server->EnqueueClient(connection_request);
}

std::unique_ptr<arrow::flight::internal::ServerTransportImpl> MakeUcxServerImpl() {
  return arrow::internal::make_unique<UcxServerImpl>();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
