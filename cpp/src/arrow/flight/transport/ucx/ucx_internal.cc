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

#include "arrow/buffer.h"
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

constexpr char kUcxScheme[] = "ucx";
constexpr char kUcxHost[] = "worker_address";
constexpr char kUcxUriPrefix[] = "ucx://worker_address/";

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

UcpCallDriver::UcpCallDriver(ucp_worker_h worker, ucp_ep_h endpoint)
    : worker_(worker), endpoint_(endpoint) {}

// Frame format

// TODO: should we just implement http/http2 over ucx..?
// should we make sure to 8-byte align everything?

// TODO: do we need to multiplex here? do an experiment: set up two
// endpoints in the client, do two parallel calls, and use netstat to
// see how many sockets UCX opens. insert artificial delay between
// headers/payload.

// 4 bytes: padding? version tag?
// 4 bytes: payload type (types follow)

// type 00: headers
// type 01: trailers
// 4 bytes: number of headers
// 4 byte total length?
// header: 4-byte length, 4-byte length, header, value

// type 02: payload
// 8 bytes: length
// payload

// TODO: we may invert the implementation here. mimic the IPC reader:
// feed byte buffers into a state machine, get back either (1) not
// enough data or (2) directions on what to do next

Status UcpCallDriver::StartCall(const std::string& method) {
  // TODO: does UCX do message coalescing? If we send this initial
  // message in two buffers, will it necessarily be worse?
  ARROW_ASSIGN_OR_RAISE(auto start_call, AllocateBuffer(8 + method.size()));
  uint8_t* payload = start_call->mutable_data();
  Int64ToBytesBe(static_cast<int64_t>(method.size()), payload);
  std::memcpy(payload + 8, method.data(), method.size());

  ucp_request_param_t request_param;
  request_param.op_attr_mask = 0;
  void* request = ucp_stream_send_nbx(endpoint_, start_call->data(), start_call->size(),
                                      &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));
  return Status::OK();
}

Status UcpCallDriver::SendPayload(const uint8_t* data, const int64_t size) {
  void* request = nullptr;
  ucp_request_param_t request_param;
  request_param.op_attr_mask = 0;

  // Send payload length
  uint8_t payload[8] = {0};
  Int64ToBytesBe(size, payload);
  request = ucp_stream_send_nbx(endpoint_, payload, 8, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  // Send payload
  request = ucp_stream_send_nbx(endpoint_, data, size, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  // TODO: need to frame payload with message type as well (headers, message, trailers)
  // TODO: need methods to send headers/trailers

  return Status::OK();
}

arrow::Result<std::unique_ptr<Buffer>> UcpCallDriver::ReadNextPayload() {
  void* request = nullptr;
  size_t actual_length = 0;

  ucp_request_param_t request_param;
  request_param.op_attr_mask =
      UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  request_param.cb.recv_stream = UcpCallDriver::StreamRecvCallback;
  request_param.flags = UCP_STREAM_RECV_FLAG_WAITALL;
  request_param.user_data = &actual_length;

  // Read payload length
  uint8_t payload_length[8] = {0};
  request =
      ucp_stream_recv_nbx(endpoint_, payload_length, 8, &actual_length, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_recv_nbx", request));
  DCHECK_EQ(actual_length, 8);

  // Read payload itself
  // TODO: try ucp_stream_recv_data_nb which has UCX allocate memory instead
  ARROW_ASSIGN_OR_RAISE(auto incoming_message,
                        AllocateBuffer(BeBytesToInt64(payload_length)));
  request = ucp_stream_recv_nbx(endpoint_, incoming_message->mutable_data(),
                                incoming_message->size(), &actual_length, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_recv_nbx", request));
  return incoming_message;
}

void UcpCallDriver::StreamRecvCallback(void* request, ucs_status_t status, size_t length,
                                       void* user_data) {
  *reinterpret_cast<size_t*>(user_data) = length;
}

Status UcpCallDriver::CompleteRequestBlocking(const std::string& context, void* request) {
  if (UCS_PTR_IS_ERR(request)) {
    return FromUcsStatus(context, UCS_PTR_STATUS(request));
  } else if (UCS_PTR_IS_PTR(request)) {
    // TODO: callback based mode
    while (true) {
      auto status = ucp_request_check_status(request);
      if (status == UCS_OK) {
        break;
      } else if (status != UCS_INPROGRESS) {
        ucp_request_release(request);
        return FromUcsStatus("ucp_request_check_status", status);
      }
      ucp_worker_progress(worker_);
    }
    ucp_request_release(request);
  } else {
    // Send was completed instantly
    DCHECK(!request);
  }
  return Status::OK();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
