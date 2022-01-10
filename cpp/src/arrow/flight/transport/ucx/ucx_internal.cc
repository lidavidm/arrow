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

#include <deque>
#include <mutex>

#include "arrow/buffer.h"
#include "arrow/util/base64.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
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
      return Status::OutOfMemory(context, ": UCX error ",
                                 static_cast<int32_t>(ucs_status), ": ",
                                 "UCS_ERR_NO_MEMORY ", ucs_status_string(ucs_status));
    case UCS_ERR_INVALID_PARAM:
      return Status::Invalid(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_INVALID_PARAM ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_UNREACHABLE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_UNREACHABLE ", ucs_status_string(ucs_status));
    case UCS_ERR_INVALID_ADDR:
      return Status::Invalid(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_INVALID_ADDR ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_NOT_IMPLEMENTED:
      return Status::NotImplemented(
          context, ": UCX error ", static_cast<int32_t>(ucs_status), ": ",
          "UCS_ERR_NOT_IMPLEMENTED ", ucs_status_string(ucs_status));
    case UCS_ERR_MESSAGE_TRUNCATED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_MESSAGE_TRUNCATED ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_NO_PROGRESS:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_PROGRESS ", ucs_status_string(ucs_status));
    case UCS_ERR_BUFFER_TOO_SMALL:
      return Status::Invalid(context, ": UCX error ", static_cast<int32_t>(ucs_status),
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
      return Status::Cancelled(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                               ": ", "UCS_ERR_CANCELED ", ucs_status_string(ucs_status));
    case UCS_ERR_SHMEM_SEGMENT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_SHMEM_SEGMENT ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_ALREADY_EXISTS:
      return Status::AlreadyExists(
          context, ": UCX error ", static_cast<int32_t>(ucs_status), ": ",
          "UCS_ERR_ALREADY_EXISTS ", ucs_status_string(ucs_status));
    case UCS_ERR_OUT_OF_RANGE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_OUT_OF_RANGE ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_TIMED_OUT:
      return Status::Cancelled(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                               ": ", "UCS_ERR_TIMED_OUT ", ucs_status_string(ucs_status));
    case UCS_ERR_EXCEEDS_LIMIT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_EXCEEDS_LIMIT ",
                             ucs_status_string(ucs_status));
    case UCS_ERR_UNSUPPORTED:
      return Status::Invalid(context, ": UCX error ", static_cast<int32_t>(ucs_status),
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

// TODO: we may invert the implementation here. mimic the IPC reader:
// feed byte buffers into a state machine, get back either (1) not
// enough data or (2) directions on what to do next
// This would make it easier to use UCX-allocated buffers

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

struct IncompleteAmRecv {
  Future<std::shared_ptr<Frame>> future;
  std::shared_ptr<Frame> frame;
};

void AmRecvCallback(void* request, ucs_status_t status, size_t length, void* user_data) {
  IncompleteAmRecv* recv_state = reinterpret_cast<IncompleteAmRecv*>(user_data);
  ucp_request_free(request);
  if (status != UCS_OK) {
    recv_state->future.MarkFinished(
        FromUcsStatus("ucp_am_recv_data_nbx (callback)", status));
  } else {
    recv_state->future.MarkFinished(std::move(recv_state->frame));
  }
  delete recv_state;
}

// pImpl the driver since async methods require a stable address
class UcpCallDriver::Impl {
 public:
  Impl() : worker_(nullptr), endpoint_(nullptr) {}
  Impl(ucp_worker_h worker, ucp_ep_h endpoint,
       std::shared_ptr<MemoryManager> memory_manager)
      : worker_(worker),
        endpoint_(endpoint),
        memory_manager_(memory_manager
                            ? std::move(memory_manager)
                            : CPUDevice::Instance()->default_memory_manager()) {}

  arrow::Result<std::shared_ptr<Frame>> ReadNextFrame() {
    // TODO: reimplement the client/server async, get rid of sync methods here
    auto fut = ReadFrameAsync();
    while (!fut.is_finished()) MakeProgress();
    RETURN_NOT_OK(fut.status());
    return fut.MoveResult();
  }

  Future<std::shared_ptr<Frame>> ReadFrameAsync() {
    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (!frames_.empty() && frames_.front().is_finished()) {
      auto fut = frames_.front();
      frames_.pop_front();
      return fut;
    }
    frames_.push_back(Future<std::shared_ptr<Frame>>::Make());
    return frames_.back();
  }

  Status SendFrame(FrameType frame_type, const uint8_t* data, const int64_t size) {
    void* request = nullptr;
    ucp_request_param_t request_param;
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    request_param.flags = UCP_AM_SEND_FLAG_REPLY;

    // UCX appears to crash on zero-byte payloads
    DCHECK_GT(size, 0);

    // Send frame header
    uint8_t header[8] = {0};
    header[0] = kFrameVersion;
    header[1] = static_cast<uint8_t>(frame_type);
    Int32ToBytesBe(size, header + 4);

    // TODO: is the active message API ordered?
    request = ucp_am_send_nbx(endpoint_, kUcpAmHandlerId, header, 8, data, size,
                              &request_param);
    RETURN_NOT_OK(CompleteRequestBlocking("ucp_am_send_nbx", request));

    return Status::OK();
  }

  Status SendFlightPayload(const FlightPayload& payload) {
    static const uint8_t kPaddingBytes[8] = {0, 0, 0, 0, 0, 0, 0, 0};

    void* request = nullptr;
    ucp_request_param_t request_param;
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_DATATYPE;
    request_param.flags = UCP_AM_SEND_FLAG_REPLY;
    request_param.datatype = UCP_DATATYPE_IOV;

    int32_t total_messages = 1;
    for (const auto& buffer : payload.ipc_message.body_buffers) {
      if (!buffer || buffer->size() == 0) continue;
      total_messages++;

      const auto remainder = static_cast<int>(
          bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
      if (remainder) total_messages++;
    }

    // Do an active message send with IOV to send all buffers in one go
    uint8_t header[8] = {0};
    header[0] = kFrameVersion;
    header[1] = static_cast<uint8_t>(FrameType::kPayload);
    Int32ToBytesBe(payload.ipc_message.metadata->size(), header + 4);
    std::vector<ucp_dt_iov_t> iovs(total_messages);

    iovs[0].buffer = const_cast<void*>(
        reinterpret_cast<const void*>(payload.ipc_message.metadata->data()));
    iovs[0].length = payload.ipc_message.metadata->size();
    if (ipc::Message::HasBody(payload.ipc_message.type)) {
      ucp_dt_iov_t* iov = iovs.data() + 1;
      for (const auto& buffer : payload.ipc_message.body_buffers) {
        if (!buffer || buffer->size() == 0) continue;

        iov->buffer = const_cast<void*>(reinterpret_cast<const void*>(buffer->data()));
        iov->length = buffer->size();
        ++iov;

        const auto remainder = static_cast<int>(
            bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
        if (remainder) {
          iov->buffer = const_cast<void*>(reinterpret_cast<const void*>(kPaddingBytes));
          iov->length = remainder;
          ++iov;
        }
      }
    }

    request = ucp_am_send_nbx(endpoint_, kUcpAmHandlerId, header, 8, iovs.data(),
                              iovs.size(), &request_param);
    RETURN_NOT_OK(CompleteRequestBlocking("ucp_am_send_nbx", request));
    return Status::OK();
  }

  Status Close() {
    void* request = ucp_ep_close_nb(endpoint_, UCP_EP_CLOSE_MODE_FLUSH);
    if (UCS_PTR_IS_ERR(request)) {
      return FromUcsStatus("ucp_ep_close_nb", UCS_PTR_STATUS(request));
    } else if (UCS_PTR_IS_PTR(request)) {
      ucs_status_t status;
      do {
        ucp_worker_progress(worker_);
        status = ucp_request_check_status(request);
      } while (status == UCS_INPROGRESS);
      ucp_request_free(request);
      if (status != UCS_OK) {
        return FromUcsStatus("ucp_request_check_status", status);
      }
    } else {
      DCHECK(!request);
    }
    return Status::OK();
  }

  void MakeProgress() { ucp_worker_progress(worker_); }

  void Push(std::shared_ptr<Frame> frame) {
    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (!frames_.empty() && !frames_.front().is_finished()) {
      frames_.front().MarkFinished(std::move(frame));
      frames_.pop_front();
    } else {
      frames_.push_back(Future<std::shared_ptr<Frame>>::MakeFinished(std::move(frame)));
    }
  }

  void Push(Status status) {
    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (!frames_.empty() && !frames_.front().is_finished()) {
      frames_.front().MarkFinished(std::move(status));
      frames_.pop_front();
    } else {
      frames_.push_back(Future<std::shared_ptr<Frame>>::MakeFinished(std::move(status)));
    }
  }

  Future<std::shared_ptr<Frame>> RecvActiveMessage(const void* header,
                                                   size_t header_length, void* data,
                                                   const size_t data_length,
                                                   const ucp_am_recv_param_t* param) {
    DCHECK(param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP);

    if (header_length < 8) {
      return Status::IOError("Header is too short, must be at least 8 bytes, got ",
                             header_length);
    }

    const uint8_t* frame_header = reinterpret_cast<const uint8_t*>(header);
    if (frame_header[0] != kFrameVersion) {
      return Status::IOError("Expected frame version ", kFrameVersion, " but got ",
                             frame_header[0]);
    } else if (frame_header[1] > static_cast<uint8_t>(FrameType::kMaxFrameType)) {
      return Status::IOError("Unknown frame type ", frame_header[1]);
    }

    if (data_length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
      return Status::Invalid(
          "Cannot allocate buffer greater than int64_t max, requested: ", data_length);
    }

    const FrameType frame_type = static_cast<FrameType>(frame_header[1]);
    std::unique_ptr<Buffer> buffer;

    if (frame_type == FrameType::kPayload) {
      ARROW_ASSIGN_OR_RAISE(buffer, memory_manager_->AllocateBuffer(data_length));
    } else {
      // TODO: allow custom pool
      ARROW_ASSIGN_OR_RAISE(buffer, AllocateBuffer(data_length));
    }
    auto frame = std::make_shared<Frame>(frame_type, BeBytesToInt32(frame_header + 4),
                                         std::move(buffer));

    // TODO: for DATA, recv_data_nbx seems to memcpy, but docs state
    // recv is sometimes needed (e.g. "unpack data to device
    // memory"). Can we predict this ahead of time and save a copy?
    // look at ucp_dt_unpack_only, seems contiguous datatype with
    // cpu-accessible buffer means we can skip the recv
    if ((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) ||
        (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_DATA)) {
      // Asynchronous receive, or unpack to destination.
      // It would be nice if we could reuse the future's allocation...
      IncompleteAmRecv* recv_state = new IncompleteAmRecv;
      recv_state->future = Future<std::shared_ptr<Frame>>::Make();
      recv_state->frame = std::move(frame);
      // Must save the future since the callback may run (and free state) before we even
      // exit the function
      auto future = recv_state->future;

      ucp_request_param_t recv_param;
      recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
      recv_param.cb.recv_am = AmRecvCallback;
      recv_param.user_data = reinterpret_cast<void*>(recv_state);
      // TODO: need to be able to differentiate between CUDA and ROCm
      if (!recv_state->frame->buffer->is_cpu()) {
        recv_param.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMORY_TYPE;
        recv_param.memory_type = UCS_MEMORY_TYPE_CUDA;
      }

      void* dest = reinterpret_cast<void*>(recv_state->frame->buffer->mutable_address());
      void* request = ucp_am_recv_data_nbx(worker_, data, dest, data_length, &recv_param);
      if (UCS_PTR_IS_ERR(request)) {
        delete recv_state;
        return FromUcsStatus("ucp_am_recv_data_nbx", UCS_PTR_STATUS(request));
      } else if (!request) {
        // Request completed instantly
        auto fut = std::move(recv_state->future);
        fut.MarkFinished(std::move(recv_state->frame));
        delete recv_state;
        return fut;
      }
      return future;
    }

    // Data will be freed after callback returns - copy to buffer
    std::memcpy(frame->buffer->mutable_data(), data, data_length);
    return frame;
  }

  const std::shared_ptr<MemoryManager>& memory_manager() const { return memory_manager_; }

 private:
  Status CompleteRequestBlocking(const std::string& context, void* request) {
    if (UCS_PTR_IS_ERR(request)) {
      return FromUcsStatus(context, UCS_PTR_STATUS(request));
    } else if (UCS_PTR_IS_PTR(request)) {
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

  ucp_worker_h worker_;
  ucp_ep_h endpoint_;
  std::shared_ptr<MemoryManager> memory_manager_;

  std::mutex frame_mutex_;
  std::deque<Future<std::shared_ptr<Frame>>> frames_;
};

UcpCallDriver::UcpCallDriver() : impl_(nullptr) {}
UcpCallDriver::UcpCallDriver(ucp_worker_h worker, ucp_ep_h endpoint,
                             std::shared_ptr<MemoryManager> memory_manager)
    : impl_(new Impl(worker, endpoint, std::move(memory_manager))) {}
UcpCallDriver::UcpCallDriver(UcpCallDriver&&) = default;
UcpCallDriver& UcpCallDriver::operator=(UcpCallDriver&&) = default;
UcpCallDriver::~UcpCallDriver() = default;

arrow::Result<std::shared_ptr<Frame>> UcpCallDriver::ReadNextFrame() {
  return impl_->ReadNextFrame();
}

Future<std::shared_ptr<Frame>> UcpCallDriver::ReadFrameAsync() {
  return impl_->ReadFrameAsync();
}

Status UcpCallDriver::ExpectFrameType(const Frame& frame, FrameType type) {
  if (frame.type != type) {
    return Status::IOError("Expected frame type ", static_cast<int32_t>(type),
                           ", but got frame type ", static_cast<int32_t>(frame.type));
  }
  return Status::OK();
}

Status UcpCallDriver::StartCall(const std::string& method) {
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

  RETURN_NOT_OK(impl_->SendFrame(FrameType::kHeaders, buffer->data(), buffer->size()));
  return Status::OK();
}

Status UcpCallDriver::SendStatus(
    const Status& status,
    const std::vector<std::pair<std::string, std::string>>& headers) {
  // TODO: need to translate status codes
  auto all_headers = headers;
  all_headers.emplace_back("flight-status-code",
                           std::to_string(static_cast<int32_t>(status.code())));
  all_headers.emplace_back("flight-status-message", status.ToString());
  return SendHeaders(all_headers);
}

Status UcpCallDriver::SendPayload(const uint8_t* data, const int64_t size) {
  RETURN_NOT_OK(impl_->SendFrame(FrameType::kPayload, data, size));
  return Status::OK();
}

Status UcpCallDriver::SendFlightPayload(const FlightPayload& payload) {
  return impl_->SendFlightPayload(payload);
}

Status UcpCallDriver::Close() { return impl_->Close(); }

void UcpCallDriver::MakeProgress() { impl_->MakeProgress(); }

void UcpCallDriver::Push(std::shared_ptr<Frame> frame) {
  return impl_->Push(std::move(frame));
}
void UcpCallDriver::Push(Status status) { return impl_->Push(std::move(status)); }

namespace {
/// A buffer backed by an incoming UCP active message buffer with
/// UCP_AM_RECV_ATTR_FLAG_DATA set.
class UcpAmDataBuffer : public Buffer {
 public:
  explicit UcpAmDataBuffer(ucp_worker_h worker, const uint8_t* data, const int64_t size)
      : Buffer(data, size), worker_(worker) {}

  ~UcpAmDataBuffer() {
    ucp_am_data_release(worker_,
                        const_cast<void*>(reinterpret_cast<const void*>(data())));
  }

 private:
  ucp_worker_h worker_;
};
}  // namespace

arrow::Future<std::shared_ptr<Frame>> UcpCallDriver::RecvActiveMessage(
    const void* header, size_t header_length, void* data, const size_t data_length,
    const ucp_am_recv_param_t* param) {
  return impl_->RecvActiveMessage(header, header_length, data, data_length, param);
}

const std::shared_ptr<MemoryManager>& UcpCallDriver::memory_manager() const {
  return impl_->memory_manager();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
