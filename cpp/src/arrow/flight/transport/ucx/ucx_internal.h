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

#include <ucp/api/ucp.h>

#include "arrow/flight/transport_impl.h"
#include "arrow/flight/visibility.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

class ARROW_FLIGHT_EXPORT UcxServerImpl
    : public arrow::flight::internal::ServerTransportImpl {
 public:
  UcxServerImpl();

  Status Init(const FlightServerOptions& options, const arrow::internal::Uri& location,
              FlightServerBase* server);
  Status Shutdown() override;
  Status Wait() override;
  Location location() const override;

 private:
  ucp_context_h ucp_context_;
  ucp_worker_h ucp_worker_;
  ucp_address_t* ucp_address_;
  uint64_t ucp_address_len_;
  Location location_;
};

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
