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
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

// TODO: move all this into its own .cc file
constexpr char kUcxScheme[] = "ucx";
constexpr char kUcxHost[] = "worker_address";
constexpr char kUcxUriPrefix[] = "ucx://worker_address/";

arrow::Result<Location> UcpAddress::ToLocation() const {
  Location location;
  auto encoded =
      util::base64_encode(util::string_view(reinterpret_cast<char*>(address), length));
  RETURN_NOT_OK(Location::Parse(kUcxUriPrefix + encoded, &location));
  return location;
}

void UcpAddress::Close() {
  if (worker) {
    ucp_worker_release_address(worker, address);
  } else if (address) {
    std::free(address);
  }
  worker = nullptr;
  address = nullptr;
  length = 0;
}

Status UcpAddress::FromUri(const arrow::internal::Uri& uri, UcpAddress* address) {
  if (!address) {
    return Status::Invalid("UcpAddress::FromUri requires an out argument");
  } else if (address->worker || address->address) {
    return Status::Invalid("Cannot overwrite existing allocated address");
  } else if (uri.scheme() != kUcxScheme) {
    return Status::NotImplemented("Flight scheme ", uri.scheme(),
                                  " is not supported by the UCX transport");
  } else if (uri.host() != kUcxHost) {
    return Status::Invalid("Expected URI in the format ", kUcxUriPrefix,
                           "..., but host was: ", uri.host());
  }

  // Remove the leading slash
  auto decoded_worker_address =
      util::base64_decode(util::string_view(uri.path()).substr(1));
  address->address =
      reinterpret_cast<ucp_address_t*>(std::malloc(decoded_worker_address.size()));
  std::memcpy(address->address, decoded_worker_address.data(),
              decoded_worker_address.size());
  return Status::OK();
}

Status UcpState::Init(const ucp_params_t& ucp_params) {
  // Initialize UCX
  ucp_config_t* ucp_config;
  RETURN_NOT_OK(
      FromUcsStatus("ucp_config_read", ucp_config_read(nullptr, nullptr, &ucp_config)));

  auto status = ucp_init(&ucp_params, ucp_config, &context);
  ucp_config_release(ucp_config);
  RETURN_NOT_OK(FromUcsStatus("ucp_init", status));

  // Initialize the worker
  ucp_worker_params_t worker_params;
  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

  // TODO: RAII release worker/context
  status = ucp_worker_create(context, &worker_params, &worker);
  RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));
  address.worker = worker;
  status = ucp_worker_get_address(worker, &address.address, &address.length);
  RETURN_NOT_OK(FromUcsStatus("ucp_worker_get_address", status));

  ARROW_ASSIGN_OR_RAISE(location, address.ToLocation());
  return Status::OK();
}

void UcpState::Close() {
  address.Close();
  ucp_worker_destroy(worker);
  ucp_cleanup(context);
}

void ClientStreamRecvCallback(void* request, ucs_status_t status, size_t length,
                              void* user_data) {
  ARROW_LOG(WARNING) << "CLIENT got message of length " << length;
  *reinterpret_cast<size_t*>(user_data) = length;
}

class ARROW_FLIGHT_EXPORT UcxClientImpl
    : public arrow::flight::internal::ClientTransportImpl {
 public:
  Status Init(const FlightClientOptions& options, const Location& location,
              const arrow::internal::Uri& uri) override {
    {
      // Initialize client state
      ucp_params_t ucp_params;
      std::memset(&ucp_params, 0, sizeof(ucp_params));
      ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
      ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM | UCP_FEATURE_WAKEUP;

      RETURN_NOT_OK(ucp_state_.Init(ucp_params));
    }

    {
      // Create endpoint for remote worker

      // TODO: factor out URI-to-sockaddr (see server)
      std::string host = uri.host();
      sockaddr_in listen_addr;
      std::memset(&listen_addr, 0, sizeof(sockaddr_in));
      listen_addr.sin_family = AF_INET;
      inet_pton(AF_INET, host.c_str(), &listen_addr.sin_addr);
      listen_addr.sin_port = htons(uri.port());

      ucp_ep_params_t params;
      params.field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR;
      params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
      params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&listen_addr);
      params.sockaddr.addrlen = sizeof(listen_addr);

      auto status = ucp_ep_create(ucp_state_.worker, &params, &remote_endpoint_);
      RETURN_NOT_OK(FromUcsStatus("ucp_ep_create", status));
    }

    return Status::OK();
  }

  Status Close() override {
    ucp_state_.Close();
    return Status::OK();
  }

  Status GetFlightInfo(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) override {
    // TODO: respect options

    std::string payload;
    descriptor.SerializeToString(&payload);

    ARROW_LOG(WARNING) << "Sending message of length " << payload.size();

    ucp_request_param_t request_param;
    void* request = ucp_stream_send_nbx(remote_endpoint_, payload.data(), payload.size(),
                                        &request_param);
    if (UCS_PTR_IS_ERR(request)) {
      RETURN_NOT_OK(FromUcsStatus("ucp_stream_send_nbx", UCS_PTR_STATUS(request)));
    } else if (UCS_PTR_IS_PTR(request)) {
      // TODO: factor out
      // TODO: callback based mode
      while (true) {
        auto status = ucp_request_check_status(request);
        if (status == UCS_OK) {
          break;
        } else if (status != UCS_INPROGRESS) {
          ucp_request_release(request);
          return FromUcsStatus("ucp_request_check_status", status);
        }
        ucp_worker_progress(ucp_state_.worker);
      }
      ucp_request_release(request);
    } else {
      // Send was completed instantly
      DCHECK(!request);
    }

    ARROW_ASSIGN_OR_RAISE(auto incoming_message, AllocateBuffer(590));
    size_t actual_length = 0;
    request_param = ucp_request_param_t{};
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_CALLBACK |
                                 UCP_OP_ATTR_FIELD_USER_DATA;
    request_param.cb.recv_stream = ClientStreamRecvCallback;
    request_param.flags = UCP_STREAM_RECV_FLAG_WAITALL;
    request_param.user_data = &actual_length;
    ARROW_LOG(WARNING) << "Client getting response";
    request =
        ucp_stream_recv_nbx(remote_endpoint_, incoming_message->mutable_data(),
                            incoming_message->size(), &actual_length, &request_param);
    if (UCS_PTR_IS_ERR(request)) {
      RETURN_NOT_OK(FromUcsStatus("ucp_stream_recv_nbx", UCS_PTR_STATUS(request)));
    } else if (UCS_PTR_IS_PTR(request)) {
      // TODO: factor out
      // TODO: callback based mode
      while (true) {
        auto status = ucp_request_check_status(request);
        if (status == UCS_OK) {
          break;
        } else if (status != UCS_INPROGRESS) {
          ucp_request_release(request);
          return FromUcsStatus("ucp_request_check_status", status);
        }
        ucp_worker_progress(ucp_state_.worker);
      }
      ucp_request_release(request);
    } else {
      // Send was completed instantly
      DCHECK(!request);
    }

    return FlightInfo::Deserialize(incoming_message->ToString(), info);
  }

 private:
  UcpState ucp_state_;
  ucp_ep_h remote_endpoint_;
};

std::unique_ptr<arrow::flight::internal::ClientTransportImpl> MakeUcxClientImpl() {
  return arrow::internal::make_unique<UcxClientImpl>();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
