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
#include <unordered_map>

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

class UcxTransportDataStream : public internal::TransportDataStream {
 public:
  constexpr static size_t kBackpressureThreshold = 32;

  explicit UcxTransportDataStream(UcpCallDriver* driver)
      : driver_(driver), writes_done_(false) {}

  bool Read(internal::FlightData* data) override { return false; }

  Status Write(const FlightPayload& payload) override {
    if (writes_done_) {
      return Status::Invalid("Writing to this stream is finished");
    }
    if (requests_.size() >= kBackpressureThreshold) {
      auto& next = requests_.front();
      while (!next.is_finished()) {
        // Progress implicitly made by main server loop
      }
      RETURN_NOT_OK(next.status());
      requests_.pop();
    }
    Future<> pending_send = driver_->SendFlightPayload(payload);
    if (!pending_send.is_finished()) {
      requests_.push(std::move(pending_send));
    }
    // Else, request completed instantly
    return Status::OK();
  }

  Status WritesDone() {
    while (!requests_.empty()) {
      auto& next = requests_.front();
      while (!next.is_finished()) {
        // Progress implicitly made by main server loop
      }
      RETURN_NOT_OK(next.status());
      requests_.pop();
    }
    writes_done_ = true;
    return Status::OK();
  }

 private:
  UcpCallDriver* driver_;
  bool writes_done_;
  std::queue<Future<>> requests_;
};

class ClientWorker : public std::enable_shared_from_this<ClientWorker> {
 public:
  ucs_status_t HandleIncomingActiveMessage(const void* header, size_t header_length,
                                           void* data, size_t data_length,
                                           const ucp_am_recv_param_t* param) {
    DCHECK(driver);
    auto self = shared_from_this();
    driver->RecvActiveMessage(header, header_length, data, data_length, param)
        .Then([self](const std::shared_ptr<Frame>& frame) { self->driver->Push(frame); },
              [self](const Status& status) { self->driver->Push(status); });
    return UCS_OK;
  }

  static void HandlePeerError(void* arg, ucp_ep_h ep, ucs_status_t status) {
    auto* self = reinterpret_cast<ClientWorker*>(arg);
    if (status == UCS_ERR_CONNECTION_RESET) {
      ARROW_UNUSED(self->driver->Close());
      // TODO: return this worker to the pool
    } else if (status != UCS_OK) {
      ARROW_LOG(WARNING) << FromUcsStatus("HandlePeerError", status);
      ARROW_LOG(WARNING) << self->driver->Close().ToString();
    }
  }

  ucp_worker_h worker = nullptr;
  std::unique_ptr<UcpCallDriver> driver;
};
}  // namespace

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
              internal::FlightServiceImpl* service) {
    service_ = service;
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
      ucp_params.features = UCP_FEATURE_AM | UCP_FEATURE_WAKEUP;
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
      listening_.test_and_set();
      std::thread listener_thread(&UcxServerImpl::DriveConnections, this);
      listener_thread_.swap(listener_thread);
    }

    return Status::OK();
  }

  Status Shutdown() override {
    Status status;

    // Wait for current RPCs to finish
    listening_.clear();
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

      // Tear down all workers
      while (!workers_.empty()) {
        ucp_worker_destroy(workers_.front()->worker);
        workers_.pop();
      }
    }

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
  Status HandleGetFlightInfo(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ARROW_ASSIGN_OR_RAISE(auto frame, driver->ReadNextFrame());
    RETURN_NOT_OK(driver->ExpectFrameType(*frame, FrameType::kPayload));
    FlightDescriptor descriptor;
    SERVER_RETURN_NOT_OK(
        driver, FlightDescriptor::Deserialize(frame->buffer->ToString(), &descriptor));

    std::unique_ptr<FlightInfo> info;
    // TODO: need to read client's trailers (for cancellations and such), asynchronously
    SERVER_RETURN_NOT_OK(driver,
                         service_->base()->GetFlightInfo(context, descriptor, &info));
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
    RETURN_NOT_OK(driver->ExpectFrameType(*frame, FrameType::kPayload));
    Ticket ticket;
    // TODO: don't allocate a new string
    SERVER_RETURN_NOT_OK(driver, Ticket::Deserialize(frame->buffer->ToString(), &ticket));

    UcxTransportDataStream stream(driver);
    auto status = service_->DoGet(context, ticket, &stream);
    RETURN_NOT_OK(driver->SendStatus(status));
    return Status::OK();
  }

  Status HandleOneCall(UcpCallDriver* driver, Frame* frame) {
    RETURN_NOT_OK(driver->ExpectFrameType(*frame, FrameType::kHeaders));
    ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Parse(std::move(frame->buffer)));
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

  void WaitForRequestAsync(std::shared_ptr<ClientWorker> worker) {
    CallbackOptions options;
    options.should_schedule = ShouldSchedule::Always;
    options.executor = rpc_pool_.get();

    auto fut = worker->driver->ReadFrameAsync();
    fut.AddCallback(
        [=](const arrow::Result<std::shared_ptr<Frame>>& maybe_frame) {
          ARROW_LOG(WARNING) << "Got frame";
          if (!maybe_frame.ok()) {
            if (maybe_frame.status().code() != StatusCode::Cancelled) {
              this->ReportError(maybe_frame.status());
            }
            // this->DisconnectClient(connection_id);
            return;
          }
          auto status = this->HandleOneCall(worker->driver.get(), maybe_frame->get());
          if (!status.ok()) {
            this->ReportError(std::move(status));
            // this->DisconnectClient(connection_id);
            return;
          }
          this->WaitForRequestAsync(std::move(worker));
        },
        options);
  }

  Status WaitForRequest(std::shared_ptr<ClientWorker> worker) {
    while (true) {
      auto maybe_frame = worker->driver->ReadNextFrame();
      if (!maybe_frame.ok() && maybe_frame.status().IsCancelled()) {
        return Status::OK();
      }
      RETURN_NOT_OK(maybe_frame.status());
      RETURN_NOT_OK(HandleOneCall(worker->driver.get(), maybe_frame->get()));
    }
    RETURN_NOT_OK(worker->driver->Close());
    return Status::OK();
  }

  void DriveConnections() {
    while (listening_.test_and_set()) {
      ucp_worker_progress(worker_conn_);

      // Check for connect requests in queue
      std::unique_lock<std::mutex> guard(pending_connections_mutex_);
      while (!pending_connections_.empty()) {
        ucp_conn_request_h request = pending_connections_.front();
        pending_connections_.pop();

        auto maybe_worker = GetWorker(guard);
        if (!maybe_worker.ok()) {
          ReportError(maybe_worker.status());
          auto status = ucp_listener_reject(listener_, pending_connections_.front());
          if (status != UCS_OK) {
            ReportError(FromUcsStatus("ucp_listener_reject", status));
            continue;
          }
        }
        std::shared_ptr<ClientWorker> worker = std::move(maybe_worker).MoveValueUnsafe();

        // Create an endpoint to the client, using the data worker
        ucp_ep_params_t params;
        std::memset(&params, 0, sizeof(params));
        params.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST |
                            UCP_EP_PARAM_FIELD_ERR_HANDLER |
                            UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
        params.conn_request = request;
        params.err_handler.cb = ClientWorker::HandlePeerError;
        params.err_handler.arg = worker.get();
        // err_mode must be set to same value on both sides
        params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
        ucs_status_t status;
        ucp_ep_h client_endpoint;

        status = ucp_ep_create(worker->worker, &params, &client_endpoint);
        if (status != UCS_OK) {
          ReportError(FromUcsStatus("ucp_ep_create", status));
          ReturnWorker(guard, std::move(worker));
          continue;
        }

        worker->driver.reset(new UcpCallDriver(worker->worker, client_endpoint));

        // TODO: add worker to an epoll set and cycle it until the future completes
        // then transfer call to a thread pool and handle it synchronously
        // then transfer back to epoll set and wait for new request
        // on disconnect, return worker to queue

        auto st = WaitForRequest(worker);
        if (!st.ok()) {
          ReportError(st);
          // disconnect
        }
        worker->driver.reset(nullptr);
        ReturnWorker(guard, std::move(worker));
      }
    }
  }

  void EnqueueClient(ucp_conn_request_h connection_request) {
    std::unique_lock<std::mutex> guard(pending_connections_mutex_);
    pending_connections_.push(connection_request);
    guard.unlock();
  }

  /// Handle errors during server worker loop execution
  void ReportError(Status st) {
    ARROW_LOG(WARNING) << "Error in Flight UCX server loop: " << st.ToString();
  }

  arrow::Result<std::shared_ptr<ClientWorker>> CreateWorker() {
    auto worker = std::make_shared<ClientWorker>();

    ucp_worker_params_t worker_params;
    std::memset(&worker_params, 0, sizeof(worker_params));
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SERIALIZED;

    auto status = ucp_worker_create(ucp_context_, &worker_params, &worker->worker);
    RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));

    // Set up Active Message (AM) handler
    ucp_am_handler_param_t handler_params;
    std::memset(&handler_params, 0, sizeof(handler_params));
    handler_params.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                                UCP_AM_HANDLER_PARAM_FIELD_CB |
                                UCP_AM_HANDLER_PARAM_FIELD_ARG;
    handler_params.id = kUcpAmHandlerId;
    handler_params.cb = HandleIncomingActiveMessage;
    handler_params.arg = worker.get();

    status = ucp_worker_set_am_recv_handler(worker->worker, &handler_params);
    RETURN_NOT_OK(FromUcsStatus("ucp_worker_set_am_recv_handler", status));
    return worker;
  }

  arrow::Result<std::shared_ptr<ClientWorker>> GetWorker(
      const std::unique_lock<std::mutex>&) {
    if (workers_.empty()) {
      ARROW_ASSIGN_OR_RAISE(auto worker, CreateWorker());
      return worker;
    }
    auto worker = std::move(workers_.front());
    workers_.pop();
    return worker;
  }

  void ReturnWorker(const std::unique_lock<std::mutex>&,
                    std::shared_ptr<ClientWorker> worker) {
    // TODO: ensure worker's call driver is nullptr
    workers_.push(std::move(worker));
  }

  /// Callback handler. A new client has connected to the server.
  static void HandleIncomingConnection(ucp_conn_request_h connection_request,
                                       void* data) {
    UcxServerImpl* server = reinterpret_cast<UcxServerImpl*>(data);
    // TODO: enable shedding load above some threshold (which is a
    // pitfall with gRPC/Java)
    server->EnqueueClient(connection_request);
  }

  static ucs_status_t HandleIncomingActiveMessage(void* self, const void* header,
                                                  size_t header_length, void* data,
                                                  size_t data_length,
                                                  const ucp_am_recv_param_t* param) {
    ClientWorker* worker = reinterpret_cast<ClientWorker*>(self);
    return worker->HandleIncomingActiveMessage(header, header_length, data, data_length,
                                               param);
  }

  ucp_context_h ucp_context_;
  // Listen for and handle incoming connections
  ucp_worker_h worker_conn_;
  ucp_listener_h listener_;
  Location location_;

  std::queue<std::shared_ptr<ClientWorker>> workers_;

  internal::FlightServiceImpl* service_;
  std::shared_ptr<arrow::internal::ThreadPool> rpc_pool_;
  std::atomic_flag listening_;
  std::thread listener_thread_;

  std::mutex pending_connections_mutex_;
  std::queue<ucp_conn_request_h> pending_connections_;
};

std::unique_ptr<arrow::flight::internal::ServerTransportImpl> MakeUcxServerImpl() {
  return arrow::internal::make_unique<UcxServerImpl>();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
