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
#include <unordered_map>

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
      return Status::NotImplemented(
          context, ": UCX error ", static_cast<int32_t>(ucs_status), ": ",
          "UCS_ERR_UNSUPPORTED ", ucs_status_string(ucs_status));
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

constexpr char kHeaderMethod[] = ":method:";

arrow::Result<HeadersFrame> HeadersFrame::Parse(std::unique_ptr<Buffer> buffer) {
  HeadersFrame result;

  const uint8_t* payload = buffer->data();
  const uint32_t num_headers = BytesToUInt32Be(payload);
  payload += 4;
  for (uint32_t i = 0; i < num_headers; i++) {
    // TODO: bounds checking
    const uint32_t key_length = BytesToUInt32Be(payload);
    payload += 4;
    const uint32_t value_length = BytesToUInt32Be(payload);
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

namespace {
static inline Status LengthToUInt32BytesBe(const int64_t in, uint8_t* out) {
  if (ARROW_PREDICT_FALSE(in < 0)) {
    return Status::Invalid("Length cannot be negative");
  } else if (ARROW_PREDICT_FALSE(
                 in > static_cast<int64_t>(std::numeric_limits<uint32_t>::max()))) {
    return Status::Invalid("Length cannot exceed uint32_t");
  }
  UInt32ToBytesBe(static_cast<uint32_t>(in), out);
  return Status::OK();
}

class UcxAmBuffer : public Buffer {
 public:
  explicit UcxAmBuffer(ucp_worker_h worker, void* data, size_t length)
      : Buffer(const_cast<const uint8_t*>(reinterpret_cast<uint8_t*>(data)), length),
        worker_(worker) {}

  ~UcxAmBuffer() {
    ucp_am_data_release(worker_,
                        const_cast<void*>(reinterpret_cast<const void*>(data())));
  }

 private:
  ucp_worker_h worker_;
};

arrow::Result<std::shared_ptr<Frame>> ParseFrameHeader(const void* header,
                                                       size_t header_length) {
  if (header_length < kFrameHeaderBytes) {
    return Status::IOError("Header is too short, must be at least ", kFrameHeaderBytes,
                           " bytes, got ", header_length);
  }

  const uint8_t* frame_header = reinterpret_cast<const uint8_t*>(header);
  if (frame_header[0] != kFrameVersion) {
    return Status::IOError("Expected frame version ", static_cast<int>(kFrameVersion),
                           " but got ", static_cast<int>(frame_header[0]));
  } else if (frame_header[1] > static_cast<uint8_t>(FrameType::kMaxFrameType)) {
    return Status::IOError("Unknown frame type ", static_cast<int>(frame_header[1]));
  }

  const FrameType frame_type = static_cast<FrameType>(frame_header[1]);
  const uint32_t frame_counter = BytesToUInt32Be(frame_header + 4);
  const uint32_t frame_size = BytesToUInt32Be(frame_header + 8);

  if (frame_type == FrameType::kDisconnect) {
    return Status::Cancelled("Client initiated disconnect");
  }

  return std::make_shared<Frame>(frame_type, frame_size, frame_counter, nullptr);
}
};  // namespace

// pImpl the driver since async methods require a stable address
class UcpCallDriver::Impl {
 public:
  Impl() : worker_(nullptr), endpoint_(nullptr) {}
  Impl(ucp_worker_h worker, ucp_ep_h endpoint,
       std::shared_ptr<MemoryManager> memory_manager)
      : worker_(worker),
        endpoint_(endpoint),
        memory_manager_(memory_manager ? std::move(memory_manager)
                                       : CPUDevice::Instance()->default_memory_manager()),
        counter_(0) {
    ucp_ep_attr_t attrs;
    std::memset(&attrs, 0, sizeof(attrs));
    attrs.field_mask = UCP_EP_ATTR_FIELD_NAME;
    if (ucp_ep_query(endpoint_, &attrs) == UCS_OK) {
      name_ = attrs.name;
    } else {
      name_ = "(unknown remote)";
    }
  }

  arrow::Result<std::shared_ptr<Frame>> ReadNextFrame() {
    auto fut = ReadFrameAsync();
    while (!fut.is_finished()) MakeProgress();
    RETURN_NOT_OK(fut.status());
    return fut.MoveResult();
  }

  Future<std::shared_ptr<Frame>> ReadFrameAsync() {
    // TODO: consolidate into ReadNextFrame, use condition variable/mutex
    RETURN_NOT_OK(CheckClosed());

    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (ARROW_PREDICT_FALSE(!status_.ok())) return status_;

    const uint32_t counter_value = next_counter_++;
    auto it = frames_.find(counter_value);
    if (it != frames_.end()) {
      Future<std::shared_ptr<Frame>> fut = it->second;
      frames_.erase(it);
      return fut;
    }
    auto pair = frames_.insert({counter_value, Future<std::shared_ptr<Frame>>::Make()});
    DCHECK(pair.second);
    return pair.first->second;
  }

  Status SendFrame(FrameType frame_type, const uint8_t* data, const int64_t size) {
    static uint8_t kZeroes[1] = {0};

    RETURN_NOT_OK(CheckClosed());

    void* request = nullptr;
    ucp_request_param_t request_param;
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    request_param.flags = UCP_AM_SEND_FLAG_REPLY;

    // Send frame header
    uint8_t header[kFrameHeaderBytes] = {0};
    header[0] = kFrameVersion;
    header[1] = static_cast<uint8_t>(frame_type);
    UInt32ToBytesBe(counter_++, header + 4);
    RETURN_NOT_OK(LengthToUInt32BytesBe(size, header + 8));

    if (size == 0) {
      // UCX appears to crash on zero-byte payloads
      request =
          ucp_am_send_nbx(endpoint_, kUcpAmHandlerId, header, kFrameHeaderBytes, kZeroes,
                          /*size=*/1, &request_param);
    } else {
      request = ucp_am_send_nbx(endpoint_, kUcpAmHandlerId, header, kFrameHeaderBytes,
                                data, size, &request_param);
    }
    RETURN_NOT_OK(CompleteRequestBlocking("ucp_am_send_nbx", request));

    return Status::OK();
  }

  Future<> SendFlightPayload(const FlightPayload& payload) {
    static const uint8_t kPaddingBytes[8] = {0, 0, 0, 0, 0, 0, 0, 0};

    RETURN_NOT_OK(CheckClosed());

    RETURN_NOT_OK(SendFrame(FrameType::kPayloadHeader,
                            payload.ipc_message.metadata->data(),
                            payload.ipc_message.metadata->size()));

    if (!ipc::Message::HasBody(payload.ipc_message.type)) {
      return Status::OK();
    }

    int32_t total_buffers = 0;
    for (const auto& buffer : payload.ipc_message.body_buffers) {
      if (!buffer || buffer->size() == 0) continue;
      total_buffers++;

      const auto remainder = static_cast<int>(
          bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
      if (remainder) total_buffers++;
    }

    // Do an active message send with IOV to send all buffers in one go
    // TODO: no need for unique_ptr so long as no early returns below
    std::unique_ptr<PendingAmSend> pending_send =
        arrow::internal::make_unique<PendingAmSend>();
    pending_send->payload = payload;
    pending_send->header[0] = kFrameVersion;
    pending_send->header[1] = static_cast<uint8_t>(FrameType::kPayloadBody);
    UInt32ToBytesBe(counter_++, pending_send->header + 4);
    RETURN_NOT_OK(
        LengthToUInt32BytesBe(payload.ipc_message.body_length, pending_send->header + 8));
    pending_send->iovs.resize(total_buffers);
    pending_send->completed = Future<>::Make();

    ucp_dt_iov_t* iov = pending_send->iovs.data();
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

    ucp_request_param_t request_param;
    std::memset(&request_param, 0, sizeof(request_param));
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE |
                                 UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_USER_DATA;
    request_param.cb.send = AmSendCallback;
    request_param.datatype = UCP_DATATYPE_IOV;
    request_param.flags = UCP_AM_SEND_FLAG_REPLY;
    request_param.user_data = pending_send.release();

    {
      auto* pending_send = reinterpret_cast<PendingAmSend*>(request_param.user_data);

      void* request = ucp_am_send_nbx(endpoint_, kUcpAmHandlerId, pending_send->header,
                                      kFrameHeaderBytes, pending_send->iovs.data(),
                                      pending_send->iovs.size(), &request_param);
      if (!request) {
        // Request completed immediately
        delete pending_send;
        return Status::OK();
      } else if (UCS_PTR_IS_ERR(request)) {
        delete pending_send;
        return FromUcsStatus("ucp_am_send_nbx", UCS_PTR_STATUS(request));
      }
      return pending_send->completed;
    }
  }

  Status Close() {
    if (!endpoint_) return Status::OK();

    for (auto& item : frames_) {
      item.second.MarkFinished(Status::Cancelled("UcpCallDriver is being closed"));
    }
    frames_.clear();

    void* request = ucp_ep_close_nb(endpoint_, UCP_EP_CLOSE_MODE_FLUSH);
    if (UCS_PTR_IS_ERR(request)) {
      return FromUcsStatus("ucp_ep_close_nb", UCS_PTR_STATUS(request));
    } else if (UCS_PTR_IS_PTR(request)) {
      ucs_status_t status;
      while ((status = ucp_request_check_status(request)) == UCS_INPROGRESS) {
        MakeProgress();
      }
      ucp_request_free(request);
      if (status != UCS_OK) {
        return FromUcsStatus("ucp_request_check_status", status);
      }
    } else {
      DCHECK(!request);
    }

    endpoint_ = nullptr;
    return Status::OK();
  }

  void MakeProgress() { ucp_worker_progress(worker_); }

  void Push(std::shared_ptr<Frame> frame) {
    // TODO: ensure Push doesn't synchronously run a callback on this
    // thread since that'll block UCX from making progress
    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (ARROW_PREDICT_FALSE(!status_.ok())) return;
    auto pair = frames_.insert({frame->counter, frame});
    if (!pair.second) {
      pair.first->second.MarkFinished(std::move(frame));
      frames_.erase(pair.first);
    }
  }

  void Push(Status status) {
    std::unique_lock<std::mutex> guard(frame_mutex_);
    status_ = std::move(status);
    for (auto& item : frames_) {
      item.second.MarkFinished(status_);
    }
    frames_.clear();
  }

  ucs_status_t RecvActiveMessage(const void* header, size_t header_length, void* data,
                                 const size_t data_length,
                                 const ucp_am_recv_param_t* param) {
    auto maybe_status =
        RecvActiveMessageImpl(header, header_length, data, data_length, param);
    if (!maybe_status.ok()) {
      Push(maybe_status.status());
      // Can't really report errors here
      // TODO: forcefully disconnect?
      return UCS_OK;
    }
    return maybe_status.MoveValueUnsafe();
  }

  const std::shared_ptr<MemoryManager>& memory_manager() const { return memory_manager_; }
  void set_memory_manager(std::shared_ptr<MemoryManager> memory_manager) {
    if (memory_manager) {
      memory_manager_ = std::move(memory_manager);
    } else {
      memory_manager_ = CPUDevice::Instance()->default_memory_manager();
    }
  }

 private:
  struct PendingAmSend {
    FlightPayload payload;
    uint8_t header[kFrameHeaderBytes];
    std::vector<ucp_dt_iov_t> iovs;
    Future<> completed;
  };

  struct PendingAmRecv {
    UcpCallDriver::Impl* driver;
    std::shared_ptr<Frame> frame;
  };

  static void AmSendCallback(void* request, ucs_status_t status, void* user_data) {
    auto* pending_send = reinterpret_cast<PendingAmSend*>(user_data);
    if (status == UCS_OK) {
      pending_send->completed.MarkFinished();
    } else {
      pending_send->completed.MarkFinished(FromUcsStatus("ucp_am_send_nbx", status));
    }
    delete pending_send;
    ucp_request_free(request);
  }

  static void AmRecvCallback(void* request, ucs_status_t status, size_t length,
                             void* user_data) {
    auto* pending_recv = reinterpret_cast<PendingAmRecv*>(user_data);
    ucp_request_free(request);
    if (status != UCS_OK) {
      pending_recv->driver->Push(
          FromUcsStatus("ucp_am_recv_data_nbx (callback)", status));
    } else {
      pending_recv->driver->Push(std::move(pending_recv->frame));
    }
    delete pending_recv;
  }

  arrow::Result<ucs_status_t> RecvActiveMessageImpl(const void* header,
                                                    size_t header_length, void* data,
                                                    const size_t data_length,
                                                    const ucp_am_recv_param_t* param) {
    DCHECK(param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP);

    if (data_length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
      return Status::Invalid(
          "Cannot allocate buffer greater than int64_t max, requested: ", data_length);
    }

    ARROW_ASSIGN_OR_RAISE(auto frame, ParseFrameHeader(header, header_length));
    DCHECK_EQ(static_cast<size_t>(frame->length), data_length);

    if ((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_DATA) &&
        (frame->type != FrameType::kPayloadBody || memory_manager_->is_cpu())) {
      // Zero-copy path. UCX-allocated buffer must be freed later.
      frame->buffer =
          arrow::internal::make_unique<UcxAmBuffer>(worker_, data, data_length);
      Push(std::move(frame));
      return UCS_INPROGRESS;
    }

    if ((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_DATA) ||
        (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)) {
      // Asynchronous receive (RNDV), or unpack to destination (DATA).
      if (frame->type == FrameType::kPayloadBody) {
        ARROW_ASSIGN_OR_RAISE(frame->buffer,
                              memory_manager_->AllocateBuffer(data_length));
      } else {
        // TODO: allow custom pool
        ARROW_ASSIGN_OR_RAISE(frame->buffer, AllocateBuffer(data_length));
      }

      PendingAmRecv* pending_recv = new PendingAmRecv;
      pending_recv->driver = this;
      pending_recv->frame = std::move(frame);

      ucp_request_param_t recv_param;
      recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
      recv_param.cb.recv_am = AmRecvCallback;
      recv_param.user_data = pending_recv;
      // TODO: need to be able to differentiate between CUDA and ROCm
      if (!pending_recv->frame->buffer->is_cpu()) {
        recv_param.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMORY_TYPE;
        recv_param.memory_type = UCS_MEMORY_TYPE_CUDA;
      }

      void* dest =
          reinterpret_cast<void*>(pending_recv->frame->buffer->mutable_address());
      void* request = ucp_am_recv_data_nbx(worker_, data, dest, data_length, &recv_param);
      if (UCS_PTR_IS_ERR(request)) {
        delete pending_recv;
        return FromUcsStatus("ucp_am_recv_data_nbx", UCS_PTR_STATUS(request));
      } else if (!request) {
        // Request completed instantly
        Push(std::move(pending_recv->frame));
        delete pending_recv;
      }
      return UCS_OK;
    } else {
      // Data will be freed after callback returns - copy to buffer
      if (frame->type != FrameType::kPayloadBody || memory_manager_->is_cpu()) {
        // TODO: allow custom pool
        ARROW_ASSIGN_OR_RAISE(frame->buffer, AllocateBuffer(data_length));
        std::memcpy(frame->buffer->mutable_data(), data, data_length);
      } else {
        ARROW_ASSIGN_OR_RAISE(
            frame->buffer,
            MemoryManager::CopyBuffer(Buffer(reinterpret_cast<uint8_t*>(data),
                                             static_cast<int64_t>(data_length)),
                                      memory_manager_));
      }
      Push(std::move(frame));
      return UCS_OK;
    }
  }

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
        MakeProgress();
      }
      ucp_request_free(request);
    } else {
      // Send was completed instantly
      DCHECK(!request);
    }
    return Status::OK();
  }

  Status CheckClosed() {
    if (!endpoint_) {
      return Status::Invalid("UcpCallDriver is closed");
    }
    return Status::OK();
  }

  ucp_worker_h worker_;
  ucp_ep_h endpoint_;
  std::shared_ptr<MemoryManager> memory_manager_;

  // Internal name for logging/tracing
  std::string name_;
  // Counter used to reorder messages
  uint32_t counter_ = 0;

  std::mutex frame_mutex_;
  Status status_;
  std::unordered_map<uint32_t, Future<std::shared_ptr<Frame>>> frames_;
  uint32_t next_counter_ = 0;
};

UcpCallDriver::UcpCallDriver(ucp_worker_h worker, ucp_ep_h endpoint,
                             std::shared_ptr<MemoryManager> memory_manager)
    : impl_(new Impl(worker, endpoint, std::move(memory_manager))) {}
UcpCallDriver::UcpCallDriver(UcpCallDriver&&) = default;
UcpCallDriver& UcpCallDriver::operator=(UcpCallDriver&&) = default;
UcpCallDriver::~UcpCallDriver() = default;

arrow::Result<std::shared_ptr<Frame>> UcpCallDriver::ReadNextFrame() {
  return impl_->ReadNextFrame();
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

  RETURN_NOT_OK(LengthToUInt32BytesBe(headers.size(), payload));
  payload += 4;
  for (const auto& header : headers) {
    RETURN_NOT_OK(LengthToUInt32BytesBe(header.first.size(), payload));
    payload += 4;
    RETURN_NOT_OK(LengthToUInt32BytesBe(header.second.size(), payload));
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

Future<> UcpCallDriver::SendFlightPayload(const FlightPayload& payload) {
  return impl_->SendFlightPayload(payload);
}

Status UcpCallDriver::SendFrame(FrameType frame_type, const uint8_t* data,
                                const int64_t size) {
  return impl_->SendFrame(frame_type, data, size);
}

Status UcpCallDriver::Close() { return impl_->Close(); }

void UcpCallDriver::MakeProgress() { impl_->MakeProgress(); }

ucs_status_t UcpCallDriver::RecvActiveMessage(const void* header, size_t header_length,
                                              void* data, const size_t data_length,
                                              const ucp_am_recv_param_t* param) {
  return impl_->RecvActiveMessage(header, header_length, data, data_length, param);
}

const std::shared_ptr<MemoryManager>& UcpCallDriver::memory_manager() const {
  return impl_->memory_manager();
}

void UcpCallDriver::set_memory_manager(std::shared_ptr<MemoryManager> memory_manager) {
  impl_->set_memory_manager(std::move(memory_manager));
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
