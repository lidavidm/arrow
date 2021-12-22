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

#include "arrow/buffer.h"
#include "arrow/flight/transport_impl.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"
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

Status UcxClientImpl::Init(const FlightClientOptions& options, const Location& location,
                           const arrow::internal::Uri& uri) {
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
    UcpAddress remote_address;
    RETURN_NOT_OK(UcpAddress::FromUri(uri, &remote_address));

    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = remote_address.address;

    auto status = ucp_ep_create(ucp_state_.worker, &ep_params, &remote_endpoint_);
    RETURN_NOT_OK(FromUcsStatus("ucp_ep_create", status));
  }

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
  // TODO: thread safety? need to set option for multithreaded worker on init?

  // Tag-matching. Send a message containing our address, a tag to use, and the serialized
  // descriptor. The server responds on the given address with the given tag.

  std::vector<uint8_t> payload(ucp_state_.address.length + 8 + 8);
  ARROW_LOG(WARNING) << "Addr length: " << ucp_state_.address.length;
  // Client UCP address length
  payload[0] = static_cast<uint8_t>(ucp_state_.address.length & 0xFF);
  payload[1] = static_cast<uint8_t>((ucp_state_.address.length >> 8) & 0xFF);
  payload[2] = static_cast<uint8_t>((ucp_state_.address.length >> 16) & 0xFF);
  payload[3] = static_cast<uint8_t>((ucp_state_.address.length >> 24) & 0xFF);
  payload[4] = static_cast<uint8_t>((ucp_state_.address.length >> 32) & 0xFF);
  payload[5] = static_cast<uint8_t>((ucp_state_.address.length >> 40) & 0xFF);
  payload[6] = static_cast<uint8_t>((ucp_state_.address.length >> 48) & 0xFF);
  payload[7] = static_cast<uint8_t>((ucp_state_.address.length >> 56) & 0xFF);
  // Client UCP address
  std::memcpy(payload.data() + 8,
              reinterpret_cast<const void*>(ucp_state_.address.address),
              ucp_state_.address.length);
  // Tag
  uint8_t* buf = payload.data() + 8 + ucp_state_.address.length;
  ucp_tag_t response_tag = 12345678;
  buf[0] = static_cast<uint8_t>(response_tag & 0xFF);
  buf[1] = static_cast<uint8_t>((response_tag >> 8) & 0xFF);
  buf[2] = static_cast<uint8_t>((response_tag >> 16) & 0xFF);
  buf[3] = static_cast<uint8_t>((response_tag >> 24) & 0xFF);
  buf[4] = static_cast<uint8_t>((response_tag >> 32) & 0xFF);
  buf[5] = static_cast<uint8_t>((response_tag >> 40) & 0xFF);
  buf[6] = static_cast<uint8_t>((response_tag >> 48) & 0xFF);
  buf[7] = static_cast<uint8_t>((response_tag >> 56) & 0xFF);
  // FlightDescriptor
  std::string temp;
  descriptor.SerializeToString(&temp);
  payload.insert(payload.end(), temp.data(), temp.data() + temp.size());

  constexpr ucp_tag_t kTag = 0xDEADBEEFu;
  void* request = ucp_tag_send_nb(
      remote_endpoint_, reinterpret_cast<const void*>(payload.data()), payload.size(),
      ucp_dt_make_contig(1), kTag, [](void* request, ucs_status_t status) {});
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
  } else {
    // Send was completed immediately
    DCHECK_EQ(request, nullptr);
  }

  // Listen for a response from the server on the given tag
  const uint64_t tag_mask = std::numeric_limits<uint64_t>::max();
  ucp_tag_recv_info_t info_tag;
  ucp_tag_message_h msg_tag;
  while (true) {
    msg_tag = ucp_tag_probe_nb(ucp_state_.worker, response_tag, tag_mask, /*remove=*/1,
                               &info_tag);
    if (msg_tag) {
      // Message received
      break;
    } else if (ucp_worker_progress(ucp_state_.worker)) {
      continue;
    }
    // Go to sleep
    RETURN_NOT_OK(FromUcsStatus("ucp_worker_wait", ucp_worker_wait(ucp_state_.worker)));
  }

  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(info_tag.length));

  request = ucp_tag_msg_recv_nb(
      ucp_state_.worker, buffer->mutable_data(), info_tag.length, ucp_dt_make_contig(1),
      msg_tag, [](void* request, ucs_status_t status, ucp_tag_recv_info_t* info) {});
  if (UCS_PTR_IS_ERR(request)) {
    return FromUcsStatus("ucp_tag_msg_recv_nb", UCS_PTR_STATUS(request));
  } else {
    DCHECK(UCS_PTR_IS_PTR(request));
    while (true) {
      auto status = ucp_request_check_status(request);
      if (status == UCS_OK) {
        break;
      } else if (status != UCS_INPROGRESS) {
        // TODO: RAII handler for request?
        ucp_request_release(request);
        return FromUcsStatus("ucp_request_check_status", status);
      }
      ucp_worker_progress(ucp_state_.worker);
    }
    ucp_request_release(request);
  }

  return Status::UnknownError(buffer->ToString());
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
