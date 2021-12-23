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

struct UcpStartCallFrame {
  // TODO: version # or something?
  int64_t length;
  std::string method;

  arrow::Result<std::unique_ptr<Buffer>> Serialize() const;
  static UcpStartCallFrame MakeFromMethod(const std::string& method);
};

// struct UcpPayloadFrame {
//   int64_t length;
//   // TODO: owned vs unowned payload
//   // TODO: manage UCX-allocated memory;
//   std::unique_ptr<Buffer> payload;
// };

/// Helper to convert a Uri to a struct sockaddr (used in ucp_listener_params_t)
ARROW_FLIGHT_EXPORT
Status UriToSockaddr(const arrow::internal::Uri& uri, sockaddr* addr);

ARROW_FLIGHT_EXPORT
std::unique_ptr<arrow::flight::internal::ClientTransportImpl> MakeUcxClientImpl();

ARROW_FLIGHT_EXPORT
std::unique_ptr<arrow::flight::internal::ServerTransportImpl> MakeUcxServerImpl();

ARROW_FLIGHT_EXPORT
Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status);

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
