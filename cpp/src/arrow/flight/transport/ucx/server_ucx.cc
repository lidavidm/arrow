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
#include "arrow/util/io_util.h"
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

#define FLIGHT_LOG(LEVEL) (ARROW_LOG(LEVEL) << "[server] ")
#define FLIGHT_LOG_PEER(LEVEL, PEER) \
  (ARROW_LOG(LEVEL) << "[server]"    \
                    << "[peer=" << peer << "] ")

namespace {
// TODO: many of these could go into an internal header for testing

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
  // TODO: backpressure threshold should be dynamic (ideally
  // auto-adjusted, or at least configurable)
  constexpr static size_t kBackpressureThreshold = 32;

  explicit UcxTransportDataStream(UcpCallDriver* driver)
      : driver_(driver), writes_done_(false) {}

  bool ReadData(internal::FlightData* data) override { return false; }

  Status WriteData(const FlightPayload& payload) override {
    if (writes_done_) {
      return Status::Invalid("Writing to this stream is finished");
    }
    if (requests_.size() >= kBackpressureThreshold) {
      auto& next = requests_.front();
      while (!next.is_finished()) {
        driver_->MakeProgress();
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
        driver_->MakeProgress();
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

arrow::Result<std::string> SockaddrToString(const struct sockaddr_storage& address) {
  std::string result;
  if (address.ss_family != AF_INET && address.ss_family != AF_INET6) {
    return Status::NotImplemented("Unknown address family");
  }

  uint16_t port = 0;
  if (address.ss_family == AF_INET) {
    result.resize(INET_ADDRSTRLEN + 1);
    port = ntohs(reinterpret_cast<const struct sockaddr_in*>(&address)->sin_port);
    result[INET_ADDRSTRLEN] = ':';
    result += std::to_string(port);
  } else {
    result.resize(INET6_ADDRSTRLEN + 1);
    port = ntohs(reinterpret_cast<const struct sockaddr_in6*>(&address)->sin6_port);
    result[INET_ADDRSTRLEN] = ':';
    result += std::to_string(port);
  }
  if (!inet_ntop(address.ss_family, &address, &result[0], result.size())) {
    return arrow::internal::IOErrorFromErrno(errno,
                                             "Could not convert address to string");
  }

  return result;
}
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
    }

    {
      // Create one worker to listen for incoming connections.
      ucp_worker_params_t worker_params;
      ucs_status_t status;

      std::memset(&worker_params, 0, sizeof(worker_params));
      worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
      worker_params.thread_mode = UCS_THREAD_MODE_MULTI;
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
      listening_.store(true);
      std::thread listener_thread(&UcxServerImpl::DriveConnections, this);
      listener_thread_.swap(listener_thread);
    }

    return Status::OK();
  }

  Status Shutdown() override {
    Status status;

    // Wait for current RPCs to finish
    listening_.store(false);
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
      // Tear down all workers. TODO: this needs to include in-progress
      // workers (we should break out of Wait() given a timeout)
      ucp_worker_destroy(worker_conn_);
    }

    status &= rpc_pool_->Shutdown();
    rpc_pool_.reset();

    ucp_cleanup(ucp_context_);
    ucp_context_ = nullptr;
    return status;
  }

  Status Shutdown(const std::chrono::system_clock::time_point& deadline) override {
    // TODO:
    return Shutdown();
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
  struct ClientWorker {
    ucp_worker_h worker = nullptr;
    std::unique_ptr<UcpCallDriver> driver;
  };

  Status HandleGetFlightInfo(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ARROW_ASSIGN_OR_RAISE(auto frame, driver->ReadNextFrame());
    SERVER_RETURN_NOT_OK(driver, driver->ExpectFrameType(*frame, FrameType::kBuffer));
    FlightDescriptor descriptor;
    SERVER_RETURN_NOT_OK(driver,
                         FlightDescriptor::Deserialize(util::string_view(*frame->buffer))
                             .Value(&descriptor));

    std::unique_ptr<FlightInfo> info;
    std::string response;
    // TODO: need to read client's trailers (for cancellations and such), asynchronously
    SERVER_RETURN_NOT_OK(driver,
                         service_->base()->GetFlightInfo(context, descriptor, &info));
    SERVER_RETURN_NOT_OK(driver, info->SerializeToString().Value(&response));
    RETURN_NOT_OK(driver->SendFrame(FrameType::kBuffer,
                                    reinterpret_cast<const uint8_t*>(response.data()),
                                    static_cast<int64_t>(response.size())));
    RETURN_NOT_OK(driver->SendStatus(Status::OK()));
    return Status::OK();
  }

  Status HandleDoGet(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ARROW_ASSIGN_OR_RAISE(auto frame, driver->ReadNextFrame());
    SERVER_RETURN_NOT_OK(driver, driver->ExpectFrameType(*frame, FrameType::kBuffer));
    Ticket ticket;
    SERVER_RETURN_NOT_OK(
        driver, Ticket::Deserialize(util::string_view(*frame->buffer)).Value(&ticket));

    UcxTransportDataStream stream(driver);
    auto status = service_->DoGet(context, std::move(ticket), &stream);
    RETURN_NOT_OK(driver->SendStatus(status));
    return Status::OK();
  }

  Status HandleOneCall(UcpCallDriver* driver, Frame* frame) {
    SERVER_RETURN_NOT_OK(driver, driver->ExpectFrameType(*frame, FrameType::kHeaders));
    ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Parse(std::move(frame->buffer)));
    ARROW_ASSIGN_OR_RAISE(auto method, headers.Get(":method:"));
    if (method == kMethodGetFlightInfo) {
      return HandleGetFlightInfo(driver);
    } else if (method == kMethodDoGet) {
      return HandleDoGet(driver);
    }
    RETURN_NOT_OK(driver->SendStatus(Status::NotImplemented(method)));
    return Status::OK();
  }

  void WorkerLoop(ucp_conn_request_h request) {
    // TODO: we should track workers at the server level still, so we
    // can reuse them for new clients?

    std::string peer = "unknown:" + std::to_string(counter_++);
    ucp_conn_request_attr_t request_attr;
    std::memset(&request_attr, 0, sizeof(request_attr));
    request_attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    if (ucp_conn_request_query(request, &request_attr) == UCS_OK) {
      ARROW_UNUSED(SockaddrToString(request_attr.client_address).Value(&peer));
    }
    FLIGHT_LOG_PEER(DEBUG, peer) << "Received connection request";

    auto maybe_worker = CreateWorker();
    if (!maybe_worker.ok()) {
      FLIGHT_LOG_PEER(WARNING, peer)
          << "Failed to create worker" << maybe_worker.status().ToString();
      auto status = ucp_listener_reject(listener_, request);
      if (status != UCS_OK) {
        FLIGHT_LOG_PEER(WARNING, peer)
            << FromUcsStatus("ucp_listener_reject", status).ToString();
      }
      return;
    }
    auto worker = maybe_worker.MoveValueUnsafe();

    // Create an endpoint to the client, using the data worker
    {
      ucs_status_t status;
      ucp_ep_params_t params;
      std::memset(&params, 0, sizeof(params));
      params.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST;
      params.conn_request = request;

      ucp_ep_h client_endpoint;

      status = ucp_ep_create(worker->worker, &params, &client_endpoint);
      if (status != UCS_OK) {
        FLIGHT_LOG_PEER(WARNING, peer)
            << "Failed to create endpoint: "
            << FromUcsStatus("ucp_ep_create", status).ToString();
        return;
      }
      worker->driver.reset(
          new UcpCallDriver(ucp_context_, worker->worker, client_endpoint));
    }

    while (listening_.load()) {
      auto maybe_frame = worker->driver->ReadNextFrame();
      if (!maybe_frame.ok()) {
        if (!maybe_frame.status().IsCancelled()) {
          FLIGHT_LOG_PEER(WARNING, peer)
              << "Failed to read next message: " << maybe_frame.status().ToString();
        }
        break;
      }

      auto status = HandleOneCall(worker->driver.get(), maybe_frame->get());
      if (!status.ok()) {
        FLIGHT_LOG_PEER(WARNING, peer) << "Call failed: " << status.ToString();
        break;
      }
    }

    // Clean up
    auto status = worker->driver->Close();
    if (!status.ok()) {
      FLIGHT_LOG_PEER(WARNING, peer) << "Failed to close worker: " << status.ToString();
    }
    ucp_worker_destroy(worker->worker);
    FLIGHT_LOG_PEER(DEBUG, peer) << "Disconnected";
  }

  void DriveConnections() {
    while (listening_.load()) {
      while (ucp_worker_progress(worker_conn_)) {
      }

      // Check for connect requests in queue
      std::unique_lock<std::mutex> guard(pending_connections_mutex_);
      while (!pending_connections_.empty()) {
        ucp_conn_request_h request = pending_connections_.front();
        pending_connections_.pop();

        auto submitted = rpc_pool_->Submit([this, request]() { WorkerLoop(request); });
        if (!submitted.ok()) {
          ARROW_LOG(WARNING) << "Failed to submit task to handle client "
                             << submitted.status().ToString();
        }
      }

      // TODO: see if we can use ucp_worker_wait to reduce CPU load
      // (it seems to just make the loop hang though)
    }
  }

  void EnqueueClient(ucp_conn_request_h connection_request) {
    std::unique_lock<std::mutex> guard(pending_connections_mutex_);
    pending_connections_.push(connection_request);
    guard.unlock();
  }

  arrow::Result<std::shared_ptr<ClientWorker>> CreateWorker() {
    auto worker = std::make_shared<ClientWorker>();

    ucp_worker_params_t worker_params;
    std::memset(&worker_params, 0, sizeof(worker_params));
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

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
    DCHECK(worker->driver);
    return worker->driver->RecvActiveMessage(header, header_length, data, data_length,
                                             param);
  }

  ucp_context_h ucp_context_;
  // Listen for and handle incoming connections
  ucp_worker_h worker_conn_;
  ucp_listener_h listener_;
  Location location_;

  // Counter for identifying peers when UCX doesn't give us a way
  std::atomic<size_t> counter_;

  internal::FlightServiceImpl* service_;
  std::shared_ptr<arrow::internal::ThreadPool> rpc_pool_;
  std::atomic<bool> listening_;
  std::thread listener_thread_;

  std::mutex pending_connections_mutex_;
  std::queue<ucp_conn_request_h> pending_connections_;
};

std::unique_ptr<arrow::flight::internal::ServerTransportImpl> MakeUcxServerImpl() {
  return arrow::internal::make_unique<UcxServerImpl>();
}

#undef SERVER_RETURN_NOT_OK
#undef FLIGHT_LOG

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
