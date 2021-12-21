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
#include "arrow/util/logging.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

UcxServerImpl::UcxServerImpl() {}

Status UcxServerImpl::Init(const FlightServerOptions& options,
                           const arrow::internal::Uri& location,
                           FlightServerBase* server) {
  ucp_params_t ucp_params;
  std::memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM;

  RETURN_NOT_OK(ucp_state_.Init(ucp_params));

  running_.test_and_set();
  std::thread t(&UcxServerImpl::RunServer, this);
  server_thread_.swap(t);

  return Status::OK();
}
Status UcxServerImpl::Shutdown() {
  running_.clear();
  RETURN_NOT_OK(Wait());
  ucp_state_.Close();
  return Status::OK();
}
Status UcxServerImpl::Wait() {
  try {
    server_thread_.join();
  } catch (const std::system_error& e) {
    if (e.code() == std::errc::invalid_argument) {
      return Status::Invalid("Cannot Wait() on server that is not running: ", e.what());
    }
    return Status::UnknownError("Could not Wait(): ", e.what());
  }
  return Status::OK();
}
Location UcxServerImpl::location() const { return ucp_state_.location; }

void UcxServerImpl::RunServer() {
  constexpr uint64_t tag = 0xDEADBEEFu;
  const uint64_t tag_mask = std::numeric_limits<uint64_t>::max();
  while (running_.test_and_set()) {
    ucp_worker_progress(ucp_state_.worker);

    ucp_tag_recv_info_t info_tag;
    ucp_tag_message_h msg_tag =
        ucp_tag_probe_nb(ucp_state_.worker, tag, tag_mask, /*remove=*/1, &info_tag);
    if (!msg_tag) continue;

    ARROW_LOG(WARNING) << "Incoming message";
    // TODO: use Arrow allocator
    void* incoming_msg = std::malloc(info_tag.length);

    void* request = ucp_tag_msg_recv_nb(
        ucp_state_.worker, incoming_msg, info_tag.length, ucp_dt_make_contig(1), msg_tag,
        [](void* request, ucs_status_t status, ucp_tag_recv_info_t* info) {});
    if (UCS_PTR_IS_ERR(request)) {
    } else {
      DCHECK(UCS_PTR_IS_PTR(request));
      while (true) {
        auto status = ucp_request_check_status(request);
        if (status == UCS_OK) {
          break;
        } else if (status != UCS_INPROGRESS) {
          // return Status::IOError(
          //     "ucp_request_check_status: unknown error receiving message: ",
          //     static_cast<int32_t>(status));
        }
        ucp_worker_progress(ucp_state_.worker);
      }
      ucp_request_release(request);
    }

    ARROW_LOG(WARNING) << "Received message";
    std::free(incoming_msg);
  }
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
