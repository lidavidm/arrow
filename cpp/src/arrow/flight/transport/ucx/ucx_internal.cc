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
#include "arrow/util/bit_util.h"
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

// do we need to 8-byte align everything?

// 1 byte: version tag
// 1 byte: payload type
// 4 bytes: frame length

// type 00: headers
// type 01: trailers
// # of headers, followed by headers
// header: header length, value length, header, value

// type 02: payload
// payload

// TODO: we may invert the implementation here. mimic the IPC reader:
// feed byte buffers into a state machine, get back either (1) not
// enough data or (2) directions on what to do next
// This would make it easier to use UCX-allocated buffers

constexpr uint8_t kFrameVersion = 0x42;
constexpr char kHeaderMethod[] = ":method:";

arrow::Result<HeadersFrame> HeadersFrame::Parse(std::unique_ptr<Buffer> buffer) {
  HeadersFrame result;

  const uint8_t* payload = buffer->data();
  const int32_t num_headers = BeBytesToInt32(payload);
  payload += 4;
  for (int32_t i = 0; i < num_headers; i++) {
    // TODO: bounds checking
    const int32_t key_length = BeBytesToInt32(payload);
    payload += 4;
    const int32_t value_length = BeBytesToInt32(payload);
    payload += 4;
    const util::string_view key(reinterpret_cast<const char*>(payload), key_length);
    payload += key_length;
    const util::string_view value(reinterpret_cast<const char*>(payload), value_length);
    payload += value_length;
    result.headers_.emplace_back(key, value);
  }

  result.buffer_ = std::move(buffer);
  return result;
}

arrow::Result<util::string_view> HeadersFrame::Get(const std::string& key) {
  for (const auto& pair : headers_) {
    if (pair.first == key) return pair.second;
  }
  return Status::KeyError(key);
}

Status UcpCallDriver::SendFrame(FrameType frame_type, const uint8_t* data,
                                const int64_t size) {
  void* request = nullptr;
  ucp_request_param_t request_param;
  request_param.op_attr_mask = 0;

  // TODO: does UCX coalesce small writes? Is there a penalty for two
  // separate sends when both are small?

  DCHECK_GT(size, 0);

  // Send frame header
  uint8_t header[8] = {0};
  header[0] = kFrameVersion;
  header[1] = static_cast<uint8_t>(frame_type);
  Int32ToBytesBe(size, header + 4);
  request = ucp_stream_send_nbx(endpoint_, header, 8, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  // Send payload
  request = ucp_stream_send_nbx(endpoint_, data, size, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  return Status::OK();
}

arrow::Result<std::pair<FrameType, std::unique_ptr<Buffer>>>
UcpCallDriver::ReadNextFrame() {
  void* request = nullptr;
  size_t actual_length = 0;

  ucp_request_param_t request_param;
  request_param.op_attr_mask =
      UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  request_param.cb.recv_stream = UcpCallDriver::StreamRecvCallback;
  request_param.flags = UCP_STREAM_RECV_FLAG_WAITALL;
  request_param.user_data = &actual_length;

  // Read frame header
  uint8_t frame_header[8] = {0};
  request =
      ucp_stream_recv_nbx(endpoint_, frame_header, 8, &actual_length, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_recv_nbx", request));
  DCHECK_EQ(actual_length, 8);

  if (frame_header[0] != kFrameVersion) {
    return Status::IOError("Expected frame version ", kFrameVersion, " but got ",
                           frame_header[0]);
  } else if (frame_header[1] > static_cast<uint8_t>(FrameType::kMaxFrameType)) {
    return Status::IOError("Unknown frame type ", frame_header[1]);
  }

  // Read payload itself
  // TODO: try ucp_stream_recv_data_nb which has UCX allocate memory instead
  const int32_t payload_length = BeBytesToInt32(frame_header + 4);
  DCHECK_GT(payload_length, 0);
  ARROW_ASSIGN_OR_RAISE(auto incoming_message, AllocateBuffer(payload_length));
  request = ucp_stream_recv_nbx(endpoint_, incoming_message->mutable_data(),
                                incoming_message->size(), &actual_length, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_recv_nbx", request));
  return std::make_pair(static_cast<FrameType>(frame_header[1]),
                        std::move(incoming_message));
}

Status UcpCallDriver::StartCall(const std::string& method) {
  // TODO: un-hard-code header serialization here
  std::vector<std::pair<std::string, std::string>> headers;
  headers.emplace_back(kHeaderMethod, method);
  RETURN_NOT_OK(SendHeaders(headers));
  return Status::OK();
}

Status UcpCallDriver::SendHeaders(
    const std::vector<std::pair<std::string, std::string>>& headers) {
  int32_t total_length = 4 /* # of headers */;
  for (const auto& header : headers) {
    total_length += 4 /* key length */ + 4 /* value length */ +
                    header.first.size() /* key */ + header.second.size();
  }

  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(total_length));
  uint8_t* payload = buffer->mutable_data();

  Int32ToBytesBe(headers.size(), payload);
  payload += 4;
  for (const auto& header : headers) {
    Int32ToBytesBe(header.first.size(), payload);
    payload += 4;
    Int32ToBytesBe(header.second.size(), payload);
    payload += 4;
    std::memcpy(payload, header.first.data(), header.first.size());
    payload += header.first.size();
    std::memcpy(payload, header.second.data(), header.second.size());
    payload += header.second.size();
  }

  RETURN_NOT_OK(SendFrame(FrameType::kHeaders, buffer->data(), buffer->size()));
  return Status::OK();
}

Status UcpCallDriver::SendPayload(const uint8_t* data, const int64_t size) {
  RETURN_NOT_OK(SendFrame(FrameType::kPayload, data, size));
  return Status::OK();
}

static const uint8_t kPaddingBytes[8] = {0, 0, 0, 0, 0, 0, 0, 0};

Status UcpCallDriver::SendFlightPayload(const FlightPayload& payload) {
  const bool has_body = ipc::Message::HasBody(payload.ipc_message.type);
  int32_t total_length = 0;
  total_length += 4;
  total_length += payload.ipc_message.metadata->size();
  if (has_body) {
    total_length += 4;
    total_length += payload.ipc_message.body_length;
  }

  void* request = nullptr;
  ucp_request_param_t request_param;
  request_param.op_attr_mask = 0;

  // TODO: does UCX coalesce small writes? Is there a penalty for two
  // separate sends when both are small?

  // Send frame header
  uint8_t header[8] = {0};
  header[0] = kFrameVersion;
  header[1] = static_cast<uint8_t>(FrameType::kPayload);
  DCHECK_GT(total_length, 0);
  Int32ToBytesBe(total_length, header + 4);
  request = ucp_stream_send_nbx(endpoint_, header, 8, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  // Send IPC header length
  Int32ToBytesBe(payload.ipc_message.metadata->size(), header);
  request = ucp_stream_send_nbx(endpoint_, header, 4, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  // Send IPC header
  DCHECK_GT(payload.ipc_message.metadata->size(), 0);
  request = ucp_stream_send_nbx(endpoint_, payload.ipc_message.metadata->data(),
                                payload.ipc_message.metadata->size(), &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  if (!has_body) return Status::OK();

  // Send IPC body length
  Int32ToBytesBe(payload.ipc_message.body_length, header);
  request = ucp_stream_send_nbx(endpoint_, header, 4, &request_param);
  RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

  // Send IPC body buffers
  int32_t actual_length = 0;
  for (const auto& buffer : payload.ipc_message.body_buffers) {
    if (!buffer || buffer->size() == 0) continue;

    actual_length += buffer->size();
    DCHECK_GT(buffer->size(), 0);
    request =
        ucp_stream_send_nbx(endpoint_, buffer->data(), buffer->size(), &request_param);
    RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));

    // Write padding if not multiple of 8
    const auto remainder =
        static_cast<int>(bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
    if (remainder) {
      request = ucp_stream_send_nbx(endpoint_, kPaddingBytes, remainder, &request_param);
      DCHECK_GT(remainder, 0);
      RETURN_NOT_OK(CompleteRequestBlocking("ucp_stream_send_nbx", request));
      actual_length += remainder;
    }
  }

  ARROW_CHECK_EQ(actual_length, payload.ipc_message.body_length);
  return Status::OK();
}

arrow::Result<HeadersFrame> UcpCallDriver::ReadHeaders() {
  ARROW_ASSIGN_OR_RAISE(auto frame, ReadNextFrame());
  if (frame.first != FrameType::kHeaders) {
    return Status::IOError("Expected headers frame, got ",
                           static_cast<int32_t>(frame.first));
  }
  ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Parse(std::move(frame.second)));
  return headers;
}

arrow::Result<std::unique_ptr<Buffer>> UcpCallDriver::ReadNextPayload() {
  ARROW_ASSIGN_OR_RAISE(auto frame, ReadNextFrame());
  if (frame.first != FrameType::kPayload) {
    return Status::IOError("Expected payload frame, got ",
                           static_cast<int32_t>(frame.first));
  }
  return std::move(frame.second);
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
