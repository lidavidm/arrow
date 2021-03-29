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

#include "arrow/flight/client_middleware.h"

#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
// XXX: namespace/header will change in v0.6.0
#include <opentelemetry/trace/propagation/text_map_propagator.h>

#include "arrow/util/make_unique.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace flight {

namespace {

class OutgoingTextMapCarrier : public opentelemetry::trace::propagation::TextMapCarrier {
 public:
  explicit OutgoingTextMapCarrier(AddCallHeaders* outgoing_headers)
      : outgoing_headers_(outgoing_headers) {}

  opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view) const
      noexcept override {
    return opentelemetry::nostd::string_view();
  }
  void Set(opentelemetry::nostd::string_view key,
           opentelemetry::nostd::string_view value) noexcept override {
    outgoing_headers_->AddHeader(std::string(key), std::string(value));
  }

 private:
  AddCallHeaders* outgoing_headers_;
};

class ARROW_FLIGHT_EXPORT TracingClientMiddleware : public ClientMiddleware {
 public:
  void SendingHeaders(AddCallHeaders* outgoing_headers) override {
    auto context = opentelemetry::context::GetDefaultStorage()->GetCurrent();
    auto carrier = OutgoingTextMapCarrier(outgoing_headers);
    opentelemetry::trace::propagation::HttpTraceContext().Inject(carrier, context);
  }
  void ReceivedHeaders(const CallHeaders&) override {}
  void CallCompleted(const Status&) override {}
};

class ARROW_FLIGHT_EXPORT TracingClientMiddlewareFactory
    : public ClientMiddlewareFactory {
 public:
  TracingClientMiddlewareFactory() {}

  void StartCall(const CallInfo& info,
                 std::unique_ptr<ClientMiddleware>* middleware) override {
    *middleware = arrow::internal::make_unique<TracingClientMiddleware>();
  }
};
}  // namespace

std::shared_ptr<ClientMiddlewareFactory> MakeTracingClientMiddlewareFactory() {
  return std::make_shared<TracingClientMiddlewareFactory>();
}

}  // namespace flight
}  // namespace arrow
