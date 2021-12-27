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
    if (incoming_message.first == FrameType::kPayload) {
      // TODO: avoid allocating string
      RETURN_NOT_OK(FlightInfo::Deserialize(incoming_message.second->ToString(), info));
      ARROW_ASSIGN_OR_RAISE(incoming_message, driver.ReadNextFrame());
    }
    if (incoming_message.first != FrameType::kHeaders) {
      return Status::IOError("Expected trailers");
    }
    ARROW_ASSIGN_OR_RAISE(auto headers,
                          HeadersFrame::Parse(std::move(incoming_message.second)));
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

    while (true) {
      // TODO: need a general reader abstraction
      ARROW_ASSIGN_OR_RAISE(auto incoming_message, driver.ReadNextFrame());
      if (incoming_message.first == FrameType::kPayload) {
        ARROW_ASSIGN_OR_RAISE(incoming_message, driver.ReadNextFrame());
        // TODO: parse payload
        continue;
      } else if (incoming_message.first == FrameType::kHeaders) {
        // Trailers, end of stream
        ARROW_ASSIGN_OR_RAISE(auto headers,
                              HeadersFrame::Parse(std::move(incoming_message.second)));
        ARROW_ASSIGN_OR_RAISE(auto code_str, headers.Get("flight-status-code"));
        ARROW_ASSIGN_OR_RAISE(auto message_str, headers.Get("flight-status-message"));
        auto code = std::strtol(code_str.data(), nullptr, /*base=*/10);
        auto status_code = static_cast<StatusCode>(code);
        if (status_code == StatusCode::OK) break;
        return Status(status_code, std::string(message_str), nullptr);
      } else {
        return Status::IOError("Expected payload or trailers, not frame type ",
                               static_cast<int32_t>(incoming_message.first));
      }
    }
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
