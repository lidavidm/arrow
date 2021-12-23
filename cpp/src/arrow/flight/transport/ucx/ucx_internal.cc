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

#include "arrow/util/base64.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

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

Status UriToSockaddr(const arrow::internal::Uri& uri, sockaddr* addr) {
  std::string host = uri.host();
  // TODO: resolve hosts like "localhost"?
  if (host.empty()) {
    return Status::Invalid("TODO");
  } else if (uri.port() < 0) {
    return Status::Invalid("TODO");
  }

  sockaddr_in* listen_addr = reinterpret_cast<sockaddr_in*>(addr);
  std::memset(listen_addr, 0, sizeof(sockaddr_in));
  // TODO: IPv6 support
  listen_addr->sin_family = AF_INET;
  inet_pton(AF_INET, host.c_str(), &listen_addr->sin_addr);
  listen_addr->sin_port = htons(uri.port());
  return Status::OK();
}

Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status) {
  switch (ucs_status) {
    case UCS_OK:
      return Status::OK();
    case UCS_INPROGRESS:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_INPROGRESS ", ucs_status_string(ucs_status));
    case UCS_ERR_NO_MESSAGE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_MESSAGE ", ucs_status_string(ucs_status));
    case UCS_ERR_NO_RESOURCE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_RESOURCE ", ucs_status_string(ucs_status));
    case UCS_ERR_IO_ERROR:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_IO_ERROR ", ucs_status_string(ucs_status));
    case UCS_ERR_NO_MEMORY:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_MEMORY ", ucs_status_string(ucs_status));
    case UCS_ERR_INVALID_PARAM:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_INVALID_PARAM ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_UNREACHABLE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_UNREACHABLE ", ucs_status_string(ucs_status));
    case UCS_ERR_INVALID_ADDR:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_INVALID_ADDR ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_NOT_IMPLEMENTED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NOT_IMPLEMENTED ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_MESSAGE_TRUNCATED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_MESSAGE_TRUNCATED ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_NO_PROGRESS:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_PROGRESS ", ucs_status_string(ucs_status));
    case UCS_ERR_BUFFER_TOO_SMALL:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_BUFFER_TOO_SMALL ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_NO_ELEM:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_ELEM ", ucs_status_string(ucs_status));
    case UCS_ERR_SOME_CONNECTS_FAILED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_SOME_CONNECTS_FAILED ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_NO_DEVICE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_DEVICE ", ucs_status_string(ucs_status));
    case UCS_ERR_BUSY:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_BUSY ", ucs_status_string(ucs_status));
    case UCS_ERR_CANCELED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_CANCELED ", ucs_status_string(ucs_status));
    case UCS_ERR_SHMEM_SEGMENT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_SHMEM_SEGMENT ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_ALREADY_EXISTS:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_ALREADY_EXISTS ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_OUT_OF_RANGE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_OUT_OF_RANGE ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_TIMED_OUT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_TIMED_OUT ", ucs_status_string(ucs_status));
    case UCS_ERR_EXCEEDS_LIMIT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_EXCEEDS_LIMIT ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_UNSUPPORTED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_UNSUPPORTED ", ucs_status_string(ucs_status));
    case UCS_ERR_REJECTED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_REJECTED ", ucs_status_string(ucs_status));
    case UCS_ERR_NOT_CONNECTED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NOT_CONNECTED ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_CONNECTION_RESET:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_CONNECTION_RESET ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_FIRST_LINK_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_FIRST_LINK_FAILURE ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_LAST_LINK_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_LAST_LINK_FAILURE ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_FIRST_ENDPOINT_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_FIRST_ENDPOINT_FAILURE ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_LAST_ENDPOINT_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_LAST_ENDPOINT_FAILURE ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_ENDPOINT_TIMEOUT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_ENDPOINT_TIMEOUT ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_LAST:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_LAST ", ucs_status_string(ucs_status));
    default:
      return Status::UnknownError(
          context, ": Unknown UCX error: ", static_cast<int32_t>(ucs_status), " ",
          ucs_status_string(ucs_status));
  }
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
