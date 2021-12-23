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
#include <atomic>

#include "arrow/flight/transport_impl.h"
#include "arrow/flight/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

/// A UCP address (opaque handle and length).
class UcpAddress {
 public:
  // If not null, then this is a UCX-managed address and Close() needs
  // to release the address. Else, this is an Arrow-allocated address
  // and Close() just needs to free memory.
  ucp_worker_h worker;
  ucp_address_t* address;
  uint64_t length;

  UcpAddress() : worker(nullptr), address(nullptr), length(0) {}

  arrow::Result<Location> ToLocation() const;
  void Close();

  static Status FromUri(const arrow::internal::Uri& uri, UcpAddress* address);

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(UcpAddress);
};

/// General UCP state that both server and client require.
struct UcpState {
  ucp_context_h context;
  ucp_worker_h worker;
  UcpAddress address;
  Location location;

  Status Init(const ucp_params_t& ucp_params);
  void Close();
};

// struct UcpPayloadFrame {
//   int64_t length;
//   // TODO: owned vs unowned payload
//   // TODO: manage UCX-allocated memory;
//   std::unique_ptr<Buffer> payload;
// };

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

static inline int64_t BeBytesToInt64(const uint8_t* in) {
  uint64_t val =
      static_cast<uint64_t>(in[7]) | (static_cast<uint64_t>(in[6]) << 8) |
      (static_cast<uint64_t>(in[5]) << 16) | (static_cast<uint64_t>(in[4]) << 24) |
      (static_cast<uint64_t>(in[3]) << 32) | (static_cast<uint64_t>(in[2]) << 40) |
      (static_cast<uint64_t>(in[1]) << 48) | (static_cast<uint64_t>(in[0]) << 56);
  // TODO: this isn't technically right until C++20? P1236R1
  return static_cast<int64_t>(val);
}

ARROW_FLIGHT_EXPORT
Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status);

class UcpCallDriver {
 public:
  UcpCallDriver(ucp_worker_h worker, ucp_ep_h endpoint);

  // Client side only.
  Status StartCall(const std::string& method);

  Status SendPayload(const uint8_t* data, const int64_t size);

  arrow::Result<std::unique_ptr<Buffer>> ReadNextPayload();

 private:
  static void StreamRecvCallback(void* request, ucs_status_t status, size_t length,
                                 void* user_data);

  Status CompleteRequestBlocking(const std::string& context, void* request);

  ucp_worker_h worker_;
  ucp_ep_h endpoint_;
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
