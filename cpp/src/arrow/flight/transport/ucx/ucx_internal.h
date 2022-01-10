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

#pragma once

#include <arpa/inet.h>
#include <ucp/api/ucp.h>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/flight/transport_impl.h"
#include "arrow/flight/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

// TODO: use unsigned integers?
static inline void Int64ToBytesBe(const int64_t in, uint8_t* out) {
  const uint64_t val = static_cast<uint64_t>(in);
  out[0] = static_cast<uint8_t>((val >> 56) & 0xFF);
  out[1] = static_cast<uint8_t>((val >> 48) & 0xFF);
  out[2] = static_cast<uint8_t>((val >> 40) & 0xFF);
  out[3] = static_cast<uint8_t>((val >> 32) & 0xFF);
  out[4] = static_cast<uint8_t>((val >> 24) & 0xFF);
  out[5] = static_cast<uint8_t>((val >> 16) & 0xFF);
  out[6] = static_cast<uint8_t>((val >> 8) & 0xFF);
  out[7] = static_cast<uint8_t>(val & 0xFF);
}

static inline void Int32ToBytesBe(const int32_t in, uint8_t* out) {
  const uint32_t val = static_cast<uint32_t>(in);
  out[0] = static_cast<uint8_t>((val >> 24) & 0xFF);
  out[1] = static_cast<uint8_t>((val >> 16) & 0xFF);
  out[2] = static_cast<uint8_t>((val >> 8) & 0xFF);
  out[3] = static_cast<uint8_t>(val & 0xFF);
}

// TODO: inconsistent naming (BytesBe)
static inline int64_t BeBytesToInt64(const uint8_t* in) {
  uint64_t val =
      static_cast<uint64_t>(in[7]) | (static_cast<uint64_t>(in[6]) << 8) |
      (static_cast<uint64_t>(in[5]) << 16) | (static_cast<uint64_t>(in[4]) << 24) |
      (static_cast<uint64_t>(in[3]) << 32) | (static_cast<uint64_t>(in[2]) << 40) |
      (static_cast<uint64_t>(in[1]) << 48) | (static_cast<uint64_t>(in[0]) << 56);
  // TODO: this isn't technically right until C++20? P1236R1
  return static_cast<int64_t>(val);
}

static inline int32_t BeBytesToInt32(const uint8_t* in) {
  uint32_t val = static_cast<uint32_t>(in[3]) | (static_cast<uint32_t>(in[2]) << 8) |
                 (static_cast<uint32_t>(in[1]) << 16) |
                 (static_cast<uint32_t>(in[0]) << 24);
  // TODO: this isn't technically right until C++20? P1236R1
  return static_cast<int32_t>(val);
}

ARROW_FLIGHT_EXPORT
Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status);

enum class FrameType : uint8_t {
  // Key-value headers.
  kHeaders = 0,
  // Binary blob.
  kPayload,
  // Keep at end.
  kMaxFrameType = kPayload,
};

class HeadersFrame {
 public:
  arrow::Result<util::string_view> Get(const std::string& key);

  static arrow::Result<HeadersFrame> Parse(std::unique_ptr<Buffer> buffer);

 private:
  std::unique_ptr<Buffer> buffer_;
  std::vector<std::pair<util::string_view, util::string_view>> headers_;
};

struct Frame {
  FrameType type;
  std::unique_ptr<Buffer> buffer;

  Frame() = default;
  Frame(FrameType type_, std::unique_ptr<Buffer> buffer_)
      : type(type_), buffer(std::move(buffer_)) {}
};

constexpr uint8_t kFrameVersion = 0x42;
constexpr uint32_t kUcpAmHandlerId = 0x1024;

class UcpCallDriver {
 public:
  UcpCallDriver();
  UcpCallDriver(ucp_worker_h worker, ucp_ep_h endpoint);

  UcpCallDriver(const UcpCallDriver&) = delete;
  UcpCallDriver(UcpCallDriver&&);
  void operator=(const UcpCallDriver&) = delete;
  UcpCallDriver& operator=(UcpCallDriver&&);

  ~UcpCallDriver();

  // Client side only.
  Status StartCall(const std::string& method);

  Status SendHeaders(const std::vector<std::pair<std::string, std::string>>& headers);
  Status SendStatus(const Status& status,
                    const std::vector<std::pair<std::string, std::string>>& headers = {});
  Status SendPayload(const uint8_t* data, const int64_t size);
  Status SendFlightPayload(const FlightPayload& payload);

  arrow::Result<std::shared_ptr<Frame>> ReadNextFrame();

  /// Read the next frame asynchronously.
  Future<std::shared_ptr<Frame>> ReadFrameAsync();

  Status ExpectFrameType(const Frame& frame, FrameType type);

  Status Close();

  // Synchronously make progress (to adapt async to sync APIs)
  void MakeProgress();
  void Push(std::shared_ptr<Frame> frame);
  void Push(Status status);

  Future<std::shared_ptr<Frame>> RecvActiveMessage(const void* header,
                                                   size_t header_length, void* data,
                                                   const size_t data_length,
                                                   const ucp_am_recv_param_t* param);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// Helper to convert a Uri to a struct sockaddr (used in ucp_listener_params_t)
ARROW_FLIGHT_EXPORT
Status UriToSockaddr(const arrow::internal::Uri& uri, sockaddr* addr);

ARROW_FLIGHT_EXPORT
std::unique_ptr<arrow::flight::internal::ClientTransportImpl> MakeUcxClientImpl();

ARROW_FLIGHT_EXPORT
std::unique_ptr<arrow::flight::internal::ServerTransportImpl> MakeUcxServerImpl();

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
