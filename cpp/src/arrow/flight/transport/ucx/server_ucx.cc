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

#include "arrow/flight/transport/ucx/server_ucx.h"

#include <ucp/api/ucp.h>

#include "arrow/flight/server_impl.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

inline Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status) {
  switch (ucs_status) {
    case UCS_OK:
      return Status::OK();
    default:
      // TODO: other cases
      return Status::UnknownError(
          context, ": Unknown UCX error: ", static_cast<int32_t>(ucs_status));
  }
}

#define UCS_STATUS(expr) FromUcsStatus(#expr, (expr))

class ARROW_FLIGHT_EXPORT UcxTransportImpl
    : public arrow::flight::internal::ServerTransportImpl {
 public:
  Status Init(const FlightServerOptions& options, const arrow::internal::Uri& location,
              FlightServerBase* server) override {
    // Initialize UCX
    ucp_params_t ucp_params;
    ucp_config_t* ucp_config;

    std::memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
    ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM;

    RETURN_NOT_OK(UCS_STATUS(ucp_config_read(nullptr, nullptr, &ucp_config)));
    RETURN_NOT_OK(UCS_STATUS(ucp_init(&ucp_params, ucp_config, &ucp_context_)));
    return Status::NotImplemented("NYI");
  }
  Status Shutdown() override {
    ucp_cleanup(ucp_context_);
    return Status::NotImplemented("NYI");
  }
  Status Wait() override { return Status::NotImplemented("NYI"); }
  int port() const override { return -1; }

 private:
  ucp_context_h ucp_context_;
};

void RegisterTransportImpl(
    arrow::flight::internal::ServerTransportImplRegistry* registry) {
  // TODO: is there a recognized URI scheme?
  DCHECK_OK(registry->RegisterImpl(
      "ucx",
      []() -> arrow::Result<
               std::unique_ptr<arrow::flight::internal::ServerTransportImpl>> {
        return arrow::internal::make_unique<UcxTransportImpl>();
      }));
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
