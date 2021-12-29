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

class UcxIpcMessageReader : public ipc::MessageReader {
 public:
  explicit UcxIpcMessageReader(UcpCallDriver driver)
      : driver_(std::move(driver)), stream_finished_(false) {}

  arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    if (stream_finished_) return nullptr;

    ARROW_ASSIGN_OR_RAISE(auto incoming_message, driver_.ReadNextFrame());
    if (incoming_message.type == FrameType::kHeaders) {
      // Trailers, stream is over
      stream_finished_ = true;
      ARROW_ASSIGN_OR_RAISE(auto headers,
                            HeadersFrame::Parse(std::move(incoming_message.buffer)));
      ARROW_ASSIGN_OR_RAISE(auto code_str, headers.Get("flight-status-code"));
      ARROW_ASSIGN_OR_RAISE(auto message_str, headers.Get("flight-status-message"));
      auto code = std::strtol(code_str.data(), nullptr, /*base=*/10);
      auto status_code = static_cast<StatusCode>(code);
      if (status_code == StatusCode::OK) {
        stream_finished_ = true;
        return nullptr;
      }
      return Status(status_code, std::string(message_str), nullptr);
    }
    RETURN_NOT_OK(driver_.ExpectFrameType(incoming_message, FrameType::kPayload));

    std::shared_ptr<Buffer> buffer = std::move(incoming_message.buffer);
    const uint8_t* payload = buffer->data();
    const int32_t metadata_len = BeBytesToInt32(payload);
    auto metadata = SliceBuffer(buffer, 4, metadata_len);
    std::shared_ptr<Buffer> body;
    if (metadata_len < buffer->size()) {
      const int32_t body_len = BeBytesToInt32(payload + 4 + metadata_len);
      body = SliceBuffer(buffer, 4 + metadata_len + 4, body_len);
    } else {
      body = std::make_shared<Buffer>(nullptr, 0);
    }

    // TODO: errors here also need to end stream, drain the stream
    // Validate IPC message
    ARROW_ASSIGN_OR_RAISE(auto message, ipc::Message::Open(metadata, body));
    return message;
  }

 private:
  UcpCallDriver driver_;
  bool stream_finished_;
};

class ARROW_FLIGHT_EXPORT UcxFlightStreamReader : public FlightStreamReader {
 public:
  explicit UcxFlightStreamReader(std::unique_ptr<ipc::MessageReader> reader)
      : message_reader_(std::move(reader)) {}
  arrow::Result<std::shared_ptr<Schema>> GetSchema() override {
    RETURN_NOT_OK(EnsureStarted());
    return reader_->schema();
  }
  Status Next(FlightStreamChunk* next) override {
    RETURN_NOT_OK(EnsureStarted());
    next->app_metadata = nullptr;
    RETURN_NOT_OK(reader_->ReadNext(&next->data));
    return Status::OK();
  }
  void Cancel() override {}

  Status ReadAll(std::vector<std::shared_ptr<RecordBatch>>* batches,
                 const StopToken& stop_token) {
    // TODO: this should be moved to a default method
    FlightStreamChunk chunk;

    while (true) {
      if (stop_token.IsStopRequested()) {
        Cancel();
        return stop_token.Poll();
      }
      RETURN_NOT_OK(Next(&chunk));
      if (!chunk.data) break;
      batches->emplace_back(std::move(chunk.data));
    }
    return Status::OK();
  }

 private:
  Status EnsureStarted() {
    if (!message_reader_) return Status::OK();
    ARROW_ASSIGN_OR_RAISE(reader_,
                          ipc::RecordBatchStreamReader::Open(std::move(message_reader_)));
    return Status::OK();
  }

  std::unique_ptr<ipc::MessageReader> message_reader_;
  std::shared_ptr<ipc::RecordBatchReader> reader_;
};

class ARROW_FLIGHT_EXPORT UcxClientImpl
    : public arrow::flight::internal::ClientTransportImpl {
 public:
  UcxClientImpl()
      : ucp_context_(nullptr), ucp_worker_(nullptr), remote_endpoint_(nullptr) {}

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
      ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM | UCP_FEATURE_WAKEUP;

      status = ucp_init(&ucp_params, ucp_config, &ucp_context_);
      ucp_config_release(ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_init", status));

      ucp_worker_params_t worker_params;
      std::memset(&worker_params, 0, sizeof(worker_params));
      worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
      worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

      status = ucp_worker_create(ucp_context_, &worker_params, &ucp_worker_);
      RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));
    }

    {
      // Create endpoint for remote worker
      sockaddr listen_addr;
      UriToSockaddr(uri, &listen_addr);

      ucp_ep_params_t params;
      params.field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR;
      params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
      params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&listen_addr);
      params.sockaddr.addrlen = sizeof(listen_addr);

      auto status = ucp_ep_create(ucp_worker_, &params, &remote_endpoint_);
      RETURN_NOT_OK(FromUcsStatus("ucp_ep_create", status));
    }

    return Status::OK();
  }

  Status Close() override {
    auto status = Status::OK();

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
          status = FromUcsStatus("ucp_request_check_status", ucp_status);
          break;
        }
        ucp_worker_progress(ucp_worker_);
      }
      ucp_request_release(request);
    } else {
      // Closure happened immediately
      DCHECK_EQ(request, nullptr);
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
    UcpCallDriver driver(ucp_worker_, remote_endpoint_);
    // TODO: constant
    RETURN_NOT_OK(driver.StartCall("arrow.flight.protocol.FlightService/GetFlightInfo"));

    std::string payload;
    descriptor.SerializeToString(&payload);

    RETURN_NOT_OK(driver.SendPayload(reinterpret_cast<const uint8_t*>(payload.data()),
                                     static_cast<int64_t>(payload.size())));

    ARROW_ASSIGN_OR_RAISE(auto incoming_message, driver.ReadNextFrame());
    if (incoming_message.type == FrameType::kPayload) {
      // TODO: avoid allocating string
      RETURN_NOT_OK(FlightInfo::Deserialize(incoming_message.buffer->ToString(), info));
      ARROW_ASSIGN_OR_RAISE(incoming_message, driver.ReadNextFrame());
    }
    RETURN_NOT_OK(driver.ExpectFrameType(incoming_message, FrameType::kHeaders));
    ARROW_ASSIGN_OR_RAISE(auto headers,
                          HeadersFrame::Parse(std::move(incoming_message.buffer)));
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
               std::unique_ptr<FlightStreamReader>* stream) override {
    UcpCallDriver driver(ucp_worker_, remote_endpoint_);
    RETURN_NOT_OK(driver.StartCall("arrow.flight.protocol.FlightService/DoGet"));

    {
      std::string payload;
      ticket.SerializeToString(&payload);
      RETURN_NOT_OK(driver.SendPayload(reinterpret_cast<const uint8_t*>(payload.data()),
                                       static_cast<int64_t>(payload.size())));
    }

    auto reader = arrow::internal::make_unique<UcxIpcMessageReader>(std::move(driver));
    *stream = arrow::internal::make_unique<UcxFlightStreamReader>(std::move(reader));
    return Status::OK();
  }

  Status DoAction(const FlightCallOptions& options, const Action& action,
                  std::unique_ptr<ResultStream>* results) override {
    // Fake this for now to get the perf test to work
    return Status::OK();
  }

 private:
  ucp_context_h ucp_context_;
  ucp_worker_h ucp_worker_;
  ucp_ep_h remote_endpoint_;
};

std::unique_ptr<arrow::flight::internal::ClientTransportImpl> MakeUcxClientImpl() {
  return arrow::internal::make_unique<UcxClientImpl>();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
