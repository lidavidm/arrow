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

static constexpr char kMethodDoGet[] = "arrow.flight.protocol.FlightService/DoGet";
static constexpr char kMethodGetFlightInfo[] =
    "arrow.flight.protocol.FlightService/GetFlightInfo";

static constexpr char kHeaderStatusCode[] = "flight-status-code";
static constexpr char kHeaderStatusMessage[] = "flight-status-message";

static inline void UInt32ToBytesBe(const uint32_t in, uint8_t* out) {
  out[0] = static_cast<uint8_t>((in >> 24) & 0xFF);
  out[1] = static_cast<uint8_t>((in >> 16) & 0xFF);
  out[2] = static_cast<uint8_t>((in >> 8) & 0xFF);
  out[3] = static_cast<uint8_t>(in & 0xFF);
}

static inline uint32_t BytesToUInt32Be(const uint8_t* in) {
  return static_cast<uint32_t>(in[3]) | (static_cast<uint32_t>(in[2]) << 8) |
         (static_cast<uint32_t>(in[1]) << 16) | (static_cast<uint32_t>(in[0]) << 24);
}

ARROW_FLIGHT_EXPORT
Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status);

enum class FrameType : uint8_t {
  // Key-value headers.
  kHeaders = 0,
  // Binary blob, does not contain Arrow data.
  kBuffer,
  // Binary blob. Contains IPC metadata, app metadata
  kPayloadHeader,
  // Binary blob. Contains IPC body.
  kPayloadBody,
  // Ask server to disconnect (to avoid client/server waiting on each other)
  kDisconnect,
  // Keep at end.
  kMaxFrameType = kDisconnect,
};

/// \brief A collection of key-value headers.
class HeadersFrame {
 public:
  /// \brief Get a header value (or an error if it was not found)
  arrow::Result<util::string_view> Get(const std::string& key);

  /// \brief Parse the headers from the buffer.
  static arrow::Result<HeadersFrame> Parse(std::unique_ptr<Buffer> buffer);

 private:
  std::unique_ptr<Buffer> buffer_;
  std::vector<std::pair<util::string_view, util::string_view>> headers_;
};

/// \brief The size of a frame header.
constexpr static size_t kFrameHeaderBytes = 12;
/// \brief The version tag in a frame.
constexpr uint8_t kFrameVersion = 0x42;
/// \brief The active message handler callback ID.
constexpr uint32_t kUcpAmHandlerId = 0x1024;

/// \brief A single message sent over UCX.
///
/// A frame is expected to be sent over UCP Active Messages and
/// consists of a header (of kFrameHeaderBytes bytes) and a body.
///
/// The header is as follows:
/// +-------+---------------------------------+
/// | Bytes | Function                        |
/// +=======+=================================+
/// | 0     | Version tag (see kFrameVersion) |
/// | 1     | Frame type (see FrameType)      |
/// | 2-3   | Unused, reserved                |
/// | 4-7   | Frame counter                   |
/// | 8-11  | Body size                       |
/// +-------+---------------------------------+
struct Frame {
  /// \brief The message type.
  FrameType type;
  /// \brief The message length.
  uint32_t length;
  /// \brief An incrementing message counter (may wrap over).
  uint32_t counter;
  /// \brief The message contents.
  std::unique_ptr<Buffer> buffer;

  Frame() = default;
  Frame(FrameType type_, uint32_t length_, uint32_t counter_,
        std::unique_ptr<Buffer> buffer_)
      : type(type_), length(length_), counter(counter_), buffer(std::move(buffer_)) {}
};

/// \brief Manage the state of a UCX connection.
class UcpCallDriver {
 public:
  UcpCallDriver(ucp_context_h context, ucp_worker_h worker, ucp_ep_h endpoint,
                std::shared_ptr<MemoryManager> memory_manager = NULLPTR);

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
  Future<> SendFlightPayload(const FlightPayload& payload);
  Status SendFrame(FrameType frame_type, const uint8_t* data, const int64_t size);

  arrow::Result<std::shared_ptr<Frame>> ReadNextFrame();
  Future<std::shared_ptr<Frame>> ReadFrameAsync();

  Status ExpectFrameType(const Frame& frame, FrameType type);

  /// Disconnect the other side of the connection. Note, this can cause deadlock.
  Status Close();

  /// Synchronously make progress (to adapt async to sync APIs)
  void MakeProgress();

  const std::shared_ptr<MemoryManager>& memory_manager() const;
  void set_memory_manager(std::shared_ptr<MemoryManager> memory_manager);

  ucs_status_t RecvActiveMessage(const void* header, size_t header_length, void* data,
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
