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

#include <ucp/api/ucp.h>

#include "arrow/flight/transport_impl.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/base64.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

Status UcxClientImpl::Init(const FlightClientOptions& options, const Location& location,
                           const arrow::internal::Uri& uri) {
  return Status::NotImplemented("NYI");
}
Status UcxClientImpl::Close() { return Status::OK(); }
}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
