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

#include <ucp/api/ucp.h>

#include "arrow/flight/transport_impl.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/base64.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

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
  status = ucp_worker_get_address(worker, &address, &address_len);
  RETURN_NOT_OK(FromUcsStatus("ucp_worker_get_address", status));

  auto encoded = util::base64_encode(
      util::string_view(reinterpret_cast<char*>(address), address_len));
  RETURN_NOT_OK(Location::Parse("ucx://worker_address/" + encoded, &location));

  return Status::OK();
}

void UcpState::Close() {
  ucp_worker_release_address(worker, address);
  ucp_worker_destroy(worker);
  ucp_cleanup(context);
}

Status UcxClientImpl::Init(const FlightClientOptions& options, const Location& location,
                           const arrow::internal::Uri& uri) {
  // TODO: constexpr constants
  if (uri.scheme() != "ucx") {
    return Status::NotImplemented("Flight scheme ", location.scheme(),
                                  " is not supported by the UCX transport");
  } else if (uri.host() != "worker_address") {
    return Status::Invalid(
        "Expected URI in the format ucx://worker_address/..., but host was: ",
        uri.host());
  }

  auto encoded_worker_address = uri.path();
  // Remove the leading slash
  auto decoded_worker_address =
      util::base64_decode(util::string_view(encoded_worker_address).substr(1));
  remote_address_ =
      reinterpret_cast<ucp_address_t*>(std::malloc(decoded_worker_address.size()));
  std::memcpy(remote_address_, decoded_worker_address.data(),
              decoded_worker_address.size());

  ucp_params_t ucp_params;
  std::memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM;

  RETURN_NOT_OK(ucp_state_.Init(ucp_params));

  return Status::OK();
}

Status UcxClientImpl::Close() {
  ucp_state_.Close();
  return Status::OK();
}

Status UcxClientImpl::GetFlightInfo(const FlightCallOptions& options,
                                    const FlightDescriptor& descriptor,
                                    std::unique_ptr<FlightInfo>* info) {
  // TODO: respect options
  // Tag-matching. Send a message containing our address, a tag to use, and the serialized
  // descriptor. The server responds on the given address with the given tag.

  // TODO: factor this out?
  ucp_ep_params_t ep_params;
  ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  ep_params.address = remote_address_;

  // TODO: ep needs to be destroyed too
  ucp_ep_h server_ep;
  auto status = ucp_ep_create(ucp_state_.worker, &ep_params, &server_ep);
  RETURN_NOT_OK(FromUcsStatus("ucp_ep_create", status));

  constexpr ucp_tag_t kTag = 0xDEADBEEFu;
  void* request =
      ucp_tag_send_nb(server_ep, reinterpret_cast<const void*>(ucp_state_.address),
                      ucp_state_.address_len, ucp_dt_make_contig(1), kTag,
                      [](void* request, ucs_status_t status) {});
  if (UCS_PTR_IS_ERR(request)) {
    return Status::IOError("ucp_tag_send_nb: unknown error sending message");
  } else if (UCS_PTR_IS_PTR(request)) {
    // TODO: factor out?
    while (true) {
      auto status = ucp_request_check_status(request);
      if (status == UCS_OK) {
        break;
      } else if (status != UCS_INPROGRESS) {
        return Status::IOError(
            "ucp_request_check_status: unknown error sending message: ",
            static_cast<int32_t>(status));
      }
      ucp_worker_progress(ucp_state_.worker);
    }
    // TODO: hello world example does this, why?
    // "Reset request state before recycling it"
    // request->completed = 0;
    ucp_request_release(request);
  }

  return Status::NotImplemented("NYI: rest of flow");
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
