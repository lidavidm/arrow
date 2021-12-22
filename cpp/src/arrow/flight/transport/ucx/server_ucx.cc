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

#include "arrow/buffer.h"
#include "arrow/flight/server.h"
#include "arrow/flight/transport_impl.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

namespace {
class UcxServerCallContext : public flight::ServerCallContext {
 public:
  const std::string& peer_identity() const override { return peer_; }
  const std::string& peer() const override { return peer_; }
  ServerMiddleware* GetMiddleware(const std::string& key) const override {
    return nullptr;
  }
  bool is_cancelled() const override { return false; }

 private:
  std::string peer_;
};
}  // namespace

UcxServerImpl::UcxServerImpl() : service_(nullptr) {}

Status UcxServerImpl::Init(const FlightServerOptions& options,
                           const arrow::internal::Uri& location,
                           FlightServerBase* server) {
  ucp_params_t ucp_params;
  std::memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM;

  RETURN_NOT_OK(ucp_state_.Init(ucp_params));

  service_ = server;

  // TODO: dispatch onto an Arrow thread pool?
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

Status HandleOneRequest(const UcpState& ucp_state, ucp_tag_recv_info_t info_tag,
                        ucp_tag_message_h msg_tag, FlightServerBase* service) {
  // TODO: use memory pool
  // TODO: guard against very large mallocs?

  ARROW_ASSIGN_OR_RAISE(auto payload, AllocateBuffer(info_tag.length));

  void* request = ucp_tag_msg_recv_nb(
      ucp_state.worker, reinterpret_cast<void*>(payload->mutable_data()), info_tag.length,
      ucp_dt_make_contig(1), msg_tag,
      [](void* request, ucs_status_t status, ucp_tag_recv_info_t* info) {});
  if (UCS_PTR_IS_ERR(request)) {
    return FromUcsStatus("ucp_tag_msg_recv_nb", UCS_PTR_STATUS(request));
  } else {
    DCHECK(UCS_PTR_IS_PTR(request));
    while (true) {
      auto status = ucp_request_check_status(request);
      if (status == UCS_OK) {
        break;
      } else if (status != UCS_INPROGRESS) {
        // TODO: RAII handler for request?
        ucp_request_release(request);
        return FromUcsStatus("ucp_request_check_status", status);
      }
      ucp_worker_progress(ucp_state.worker);
    }
    ucp_request_release(request);
  }

  // Parse the payload
  UcpAddress client_addr;
  client_addr.length = static_cast<uint64_t>(payload->data()[0]) |
                       (static_cast<uint64_t>(payload->data()[1]) << 8) |
                       (static_cast<uint64_t>(payload->data()[2]) << 16) |
                       (static_cast<uint64_t>(payload->data()[3]) << 24) |
                       (static_cast<uint64_t>(payload->data()[4]) << 32) |
                       (static_cast<uint64_t>(payload->data()[5]) << 40) |
                       (static_cast<uint64_t>(payload->data()[6]) << 48) |
                       (static_cast<uint64_t>(payload->data()[7]) << 56);
  ARROW_LOG(WARNING) << "Addr length: " << client_addr.length;
  client_addr.address = reinterpret_cast<ucp_address_t*>(std::malloc(client_addr.length));
  // TODO: validate length
  std::memcpy(client_addr.address, payload->data() + 8, client_addr.length);

  const uint8_t* buf = payload->data() + 8 + client_addr.length;
  ucp_tag_t response_tag =
      static_cast<uint64_t>(buf[0]) | (static_cast<uint64_t>(buf[1]) << 8) |
      (static_cast<uint64_t>(buf[2]) << 16) | (static_cast<uint64_t>(buf[3]) << 24) |
      (static_cast<uint64_t>(buf[4]) << 32) | (static_cast<uint64_t>(buf[5]) << 40) |
      (static_cast<uint64_t>(buf[6]) << 48) | (static_cast<uint64_t>(buf[7]) << 56);
  ARROW_LOG(WARNING) << "Response tag: " << response_tag;

  std::string serialized_descriptor(reinterpret_cast<const char*>(buf + 8),
                                    info_tag.length - (8 + 8 + client_addr.length));
  FlightDescriptor descriptor;
  RETURN_NOT_OK(FlightDescriptor::Deserialize(serialized_descriptor, &descriptor));
  ARROW_LOG(WARNING) << descriptor.ToString();

  // Call service handler and return response via tagged send
  // context
  UcxServerCallContext context;
  std::unique_ptr<FlightInfo> info;
  auto status = service->GetFlightInfo(context, descriptor, &info);

  ucp_ep_params_t ep_params;
  ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  ep_params.address = client_addr.address;

  ucp_ep_h remote_endpoint;
  RETURN_NOT_OK(FromUcsStatus(
      "ucp_ep_create", ucp_ep_create(ucp_state.worker, &ep_params, &remote_endpoint)));

  // Oh god, how do I return errors?  Probably eventually switch to
  // streams?  Are tags ordered? Can I queue up two tagged messages
  // and ensure they get sent in order? (Seems not. Tag matching comes
  // from…Infiniband? UCX just replicates the underlying semantics?)

  // What is the right mental model for a stream? A socket? A logical
  // channel (= a gRPC call)?
  std::shared_ptr<Buffer> response_payload;
  if (!status.ok()) {
    response_payload = Buffer::FromString(status.ToString());
  } else {
    return Status::NotImplemented("NYI");
  }

  request = ucp_tag_send_nb(remote_endpoint,
                            reinterpret_cast<const void*>(response_payload->data()),
                            response_payload->size(), ucp_dt_make_contig(1), response_tag,
                            [](void* request, ucs_status_t status) {});
  if (UCS_PTR_IS_ERR(request)) {
    return Status::IOError("ucp_tag_send_nb: unknown error sending message");
  } else if (UCS_PTR_IS_PTR(request)) {
    // TODO: factor out?
    while (true) {
      auto status = ucp_request_check_status(request);
      if (status == UCS_OK) {
        break;
      } else if (status != UCS_INPROGRESS) {
        return Status::IOError(
            "ucp_request_check_status: unknown error sending message: ",
            static_cast<int32_t>(status));
      }
      ucp_worker_progress(ucp_state.worker);
    }
    // TODO: hello world example does this, why?
    // "Reset request state before recycling it"
    // request->completed = 0;
    ucp_request_release(request);
  } else {
    // Send was completed immediately
    DCHECK_EQ(request, nullptr);
  }

  return Status::OK();
}

void UcxServerImpl::RunServer() {
  // TODO: threading model? Multiple threads, one worker? One worker per thread?
  constexpr uint64_t tag = 0xDEADBEEFu;
  const uint64_t tag_mask = std::numeric_limits<uint64_t>::max();
  while (running_.test_and_set()) {
    ucp_worker_progress(ucp_state_.worker);

    ucp_tag_recv_info_t info_tag;
    ucp_tag_message_h msg_tag =
        ucp_tag_probe_nb(ucp_state_.worker, tag, tag_mask, /*remove=*/1, &info_tag);
    if (!msg_tag) continue;

    auto status = HandleOneRequest(ucp_state_, info_tag, msg_tag, service_);
    if (!status.ok()) {
      ReportError(std::move(status));
    }
  }
  // TODO: flush_ep? see hello world example
}

void UcxServerImpl::ReportError(Status st) {
  ARROW_LOG(WARNING) << "Error in Flight UCX server loop: " << st.ToString();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
