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

#include <mutex>

#include <arpa/inet.h>
#include <ucp/api/ucp.h>

#include "arrow/buffer.h"
#include "arrow/flight/client.h"
#include "arrow/flight/transport_impl.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

class UcxClientDataStream : public internal::ClientDataStream {
 public:
  explicit UcxClientDataStream(UcpCallDriver* driver)
      : driver_(driver), finished_(false) {}

  bool Read(internal::FlightData* data) {
    if (finished_) return false;

    bool success = true;
    io_status_ = ReadImpl(data).Value(&success);

    if (!io_status_.ok() || !success) {
      finished_ = true;
      return false;
    }
    return true;
  }

  ::arrow::Result<bool> ReadImpl(internal::FlightData* data) {
    ARROW_ASSIGN_OR_RAISE(auto frame, driver_->ReadNextFrame());

    if (frame->type == FrameType::kHeaders) {
      // Trailers, stream is over
      ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Parse(std::move(frame->buffer)));
      ARROW_ASSIGN_OR_RAISE(auto code_str, headers.Get("flight-status-code"));
      ARROW_ASSIGN_OR_RAISE(auto message_str, headers.Get("flight-status-message"));
      auto code = std::strtol(code_str.data(), nullptr, /*base=*/10);
      auto status_code = static_cast<StatusCode>(code);
      if (status_code == StatusCode::OK) {
        server_status_ = Status::OK();
      } else {
        server_status_ = Status(status_code, std::string(message_str), nullptr);
      }
      return false;
    }

    RETURN_NOT_OK(driver_->ExpectFrameType(*frame, FrameType::kPayloadHeader));
    data->metadata = std::move(frame->buffer);
    ARROW_ASSIGN_OR_RAISE(auto message, ipc::Message::Open(data->metadata, nullptr));

    if (ipc::Message::HasBody(message->type())) {
      ARROW_ASSIGN_OR_RAISE(frame, driver_->ReadNextFrame());
      RETURN_NOT_OK(driver_->ExpectFrameType(*frame, FrameType::kPayloadBody));
      data->body = std::move(frame->buffer);
    }
    return true;
  }

  Status Write(const FlightPayload& payload) { return Status::NotImplemented("NYI"); }
  Status WritesDone() { return Status::NotImplemented("NYI"); }
  Status Finish(Status st) {
    if (finished_) {
      return MergeStatus(std::move(st));
    }

    internal::FlightData message;
    while (Read(&message)) {
    }

    // TODO: frankly, this can get refactored back out into client.cc
    finished_ = true;
    return MergeStatus(std::move(st));
  }
  void TryCancel() {
    // TODO: not implemented
  }

 private:
  Status MergeStatus(Status&& st) {
    if (server_status_.ok() && io_status_.ok()) {
      return std::move(st);
    } else if (server_status_.ok()) {
      return Status::FromDetailAndArgs(io_status_.code(), io_status_.detail(),
                                       io_status_.message(),
                                       ". Client context: ", st.ToString());
    }
    return Status::FromDetailAndArgs(server_status_.code(), server_status_.detail(),
                                     server_status_.message(),
                                     ". Client context: ", st.ToString(),
                                     ". Transport context: ", io_status_.ToString());
  }

  UcpCallDriver* driver_;
  bool finished_;
  Status io_status_;
  Status server_status_;
};

class ARROW_FLIGHT_EXPORT UcxClientImpl
    : public arrow::flight::internal::ClientTransportImpl {
 public:
  UcxClientImpl()
      : ucp_context_(nullptr),
        ucp_worker_(nullptr),
        remote_endpoint_(nullptr),
        driver_(nullptr) {}

  virtual ~UcxClientImpl() {
    if (!ucp_context_) return;
    auto status = Close();
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "UcxClientImpl errored in Close() in destructor: "
                         << status.ToString();
    }
  }

  Status Init(const FlightClientOptions& options, const Location& location,
              const arrow::internal::Uri& uri) override {
    {
      ucp_config_t* ucp_config;
      ucp_params_t ucp_params;
      ucs_status_t status;

      status = ucp_config_read(nullptr, nullptr, &ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_config_read", status));

      std::memset(&ucp_params, 0, sizeof(ucp_params));
      ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
      ucp_params.features = UCP_FEATURE_AM | UCP_FEATURE_WAKEUP;

      status = ucp_init(&ucp_params, ucp_config, &ucp_context_);
      ucp_config_release(ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_init", status));

      ucp_worker_params_t worker_params;
      std::memset(&worker_params, 0, sizeof(worker_params));
      worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
      worker_params.thread_mode = UCS_THREAD_MODE_SERIALIZED;

      status = ucp_worker_create(ucp_context_, &worker_params, &ucp_worker_);
      RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));

      // Set up Active Message (AM) handler
      ucp_am_handler_param_t handler_params;
      handler_params.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                                  UCP_AM_HANDLER_PARAM_FIELD_CB |
                                  UCP_AM_HANDLER_PARAM_FIELD_ARG;
      handler_params.id = kUcpAmHandlerId;
      handler_params.cb = HandleIncomingActiveMessage;
      handler_params.arg = this;
      ucp_worker_set_am_recv_handler(ucp_worker_, &handler_params);
    }

    {
      // Create endpoint for remote worker
      sockaddr listen_addr;
      UriToSockaddr(uri, &listen_addr);

      ucp_ep_params_t params;
      // TODO: error handling callback disables shared memory transport
      // params.field_mask = UCP_EP_PARAM_FIELD_ERR_HANDLER |
      // UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
      //                     UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR;
      params.field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR |
                          UCP_EP_PARAM_FIELD_NAME;
      params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
      params.err_handler.cb = HandlePeerError;
      params.err_handler.arg = this;
      params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
      params.name = "UcxClientImpl";
      params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&listen_addr);
      params.sockaddr.addrlen = sizeof(listen_addr);

      auto status = ucp_ep_create(ucp_worker_, &params, &remote_endpoint_);
      RETURN_NOT_OK(FromUcsStatus("ucp_ep_create", status));
    }

    driver_.reset(new UcpCallDriver(ucp_worker_, remote_endpoint_));

    return Status::OK();
  }

  Status Close() override {
    auto status = Status::OK();

    static uint8_t zeroes[1] = {0};
    UcpCallDriver driver(ucp_worker_, remote_endpoint_);
    RETURN_NOT_OK(driver.SendFrame(FrameType::kDisconnect, zeroes, 1));

    void* request = ucp_ep_close_nb(remote_endpoint_, UCP_EP_CLOSE_MODE_FLUSH);
    if (UCS_PTR_IS_ERR(request)) {
      status = FromUcsStatus("ucp_ep_close_nb", UCS_PTR_STATUS(request));
    } else if (UCS_PTR_IS_PTR(request)) {
      // Synchronously close endpoint
      while (true) {
        auto ucp_status = ucp_request_check_status(request);
        if (ucp_status == UCS_OK) {
          break;
        } else if (ucp_status != UCS_INPROGRESS) {
          // Ignore UCS_ERR_NOT_CONNECTED
          if (ucp_status == UCS_ERR_NOT_CONNECTED) break;
          status = FromUcsStatus("ucp_request_check_status", ucp_status);
          break;
        }
        ucp_worker_progress(ucp_worker_);
      }
      ucp_request_release(request);
    } else {
      // Closure happened immediately
      DCHECK(!request);
    }

    ucp_worker_destroy(ucp_worker_);
    ucp_cleanup(ucp_context_);

    remote_endpoint_ = nullptr;
    ucp_worker_ = nullptr;
    ucp_context_ = nullptr;
    return status;
  }

  Status GetFlightInfo(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) override {
    // TODO: respect options
    // TODO: can we find a way to share code with the gRPC backend?
    // TODO: constant
    RETURN_NOT_OK(
        driver_->StartCall("arrow.flight.protocol.FlightService/GetFlightInfo"));

    std::string payload;
    descriptor.SerializeToString(&payload);

    RETURN_NOT_OK(driver_->SendFrame(FrameType::kBuffer,
                                     reinterpret_cast<const uint8_t*>(payload.data()),
                                     static_cast<int64_t>(payload.size())));

    ARROW_ASSIGN_OR_RAISE(auto incoming_message, driver_->ReadNextFrame());
    if (incoming_message->type == FrameType::kBuffer) {
      // TODO: avoid allocating string
      RETURN_NOT_OK(FlightInfo::Deserialize(incoming_message->buffer->ToString(), info));
      ARROW_ASSIGN_OR_RAISE(incoming_message, driver_->ReadNextFrame());
    }
    RETURN_NOT_OK(driver_->ExpectFrameType(*incoming_message, FrameType::kHeaders));
    ARROW_ASSIGN_OR_RAISE(auto headers,
                          HeadersFrame::Parse(std::move(incoming_message->buffer)));
    // TODO: annotate error messages
    ARROW_ASSIGN_OR_RAISE(auto code_str, headers.Get("flight-status-code"));
    ARROW_ASSIGN_OR_RAISE(auto message_str, headers.Get("flight-status-message"));
    auto code = std::strtol(code_str.data(), nullptr, /*base=*/10);
    // TODO: validate
    auto status_code = static_cast<StatusCode>(code);
    if (status_code == StatusCode::OK) return Status::OK();
    return Status(status_code, std::string(message_str), nullptr);
  }

  Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<internal::ClientDataStream>* stream) override {
    driver_->set_memory_manager(options.memory_manager);
    RETURN_NOT_OK(driver_->StartCall("arrow.flight.protocol.FlightService/DoGet"));

    {
      std::string payload;
      ticket.SerializeToString(&payload);
      RETURN_NOT_OK(driver_->SendFrame(FrameType::kBuffer,
                                       reinterpret_cast<const uint8_t*>(payload.data()),
                                       static_cast<int64_t>(payload.size())));
    }

    *stream = arrow::internal::make_unique<UcxClientDataStream>(driver_.get());
    return Status::OK();
  }

  Status DoAction(const FlightCallOptions& options, const Action& action,
                  std::unique_ptr<ResultStream>* results) override {
    // Fake this for now to get the perf test to work
    return Status::OK();
  }

 private:
  static void HandlePeerError(void* arg, ucp_ep_h ep, ucs_status_t status) {
    // auto* self = reinterpret_cast<UcxClientImpl*>(arg);
    if (status != UCS_OK) {
      // TODO: UCS_ERR_CONNECTION_RESET should just re-create the client
      ARROW_LOG(WARNING) << FromUcsStatus("HandlePeerError", status);
    }
  }

  static ucs_status_t HandleIncomingActiveMessage(void* self, const void* header,
                                                  size_t header_length, void* data,
                                                  size_t data_length,
                                                  const ucp_am_recv_param_t* param) {
    auto* impl = reinterpret_cast<UcxClientImpl*>(self);
    return impl->DoHandleIncomingActiveMessage(header, header_length, data, data_length,
                                               param);
  }

  ucs_status_t DoHandleIncomingActiveMessage(const void* header, size_t header_length,
                                             void* data, size_t data_length,
                                             const ucp_am_recv_param_t* param) {
    // Got message with no active call, ignore
    if (!driver_) {
      ARROW_LOG(WARNING) << "Got message with no active call";
      return UCS_OK;
    }
    return driver_->RecvActiveMessage(header, header_length, data, data_length, param);
  }

  ucp_context_h ucp_context_;
  ucp_worker_h ucp_worker_;
  ucp_ep_h remote_endpoint_;
  std::unique_ptr<UcpCallDriver> driver_;
};

std::unique_ptr<arrow::flight::internal::ClientTransportImpl> MakeUcxClientImpl() {
  return arrow::internal::make_unique<UcxClientImpl>();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
