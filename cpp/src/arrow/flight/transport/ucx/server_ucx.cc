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

UcxServerImpl::UcxServerImpl() : ucp_address_(nullptr), ucp_address_len_(0) {}

Status UcxServerImpl::Init(const FlightServerOptions& options,
                           const arrow::internal::Uri& location,
                           FlightServerBase* server) {
  // Initialize UCX
  ucp_params_t ucp_params;
  ucp_config_t* ucp_config;

  std::memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM;

  RETURN_NOT_OK(
      FromUcsStatus("ucp_config_read", ucp_config_read(nullptr, nullptr, &ucp_config)));
  auto init_status = ucp_init(&ucp_params, ucp_config, &ucp_context_);
  ucp_config_release(ucp_config);
  RETURN_NOT_OK(FromUcsStatus("ucp_init", init_status));

  // Initialize the worker
  ucp_worker_params_t worker_params;
  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

  // TODO: RAII release worker/context
  RETURN_NOT_OK(
      FromUcsStatus("ucp_worker_create",
                    ucp_worker_create(ucp_context_, &worker_params, &ucp_worker_)));
  RETURN_NOT_OK(FromUcsStatus(
      "ucp_worker_get_address",
      ucp_worker_get_address(ucp_worker_, &ucp_address_, &ucp_address_len_)));

  auto encoded = util::base64_encode(
      util::string_view(reinterpret_cast<char*>(ucp_address_), ucp_address_len_));
  RETURN_NOT_OK(Location::Parse("ucx://worker_address/" + encoded, &location_));
  return Status::OK();
}
Status UcxServerImpl::Shutdown() {
  ucp_worker_release_address(ucp_worker_, ucp_address_);
  ucp_worker_destroy(ucp_worker_);
  ucp_cleanup(ucp_context_);
  return Status::OK();
}
Status UcxServerImpl::Wait() { return Status::OK(); }
Location UcxServerImpl::location() const { return location_; }
}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
