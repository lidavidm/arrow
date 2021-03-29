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

#include "arrow/flight/server_middleware.h"

#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
// XXX: namespace/header will change in v0.6.0
#include <opentelemetry/trace/propagation/text_map_propagator.h>

#include "arrow/util/make_unique.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace flight {

namespace {

class IncomingTextMapCarrier : public opentelemetry::trace::propagation::TextMapCarrier {
 public:
  explicit IncomingTextMapCarrier(const CallHeaders& incoming_headers)
      : incoming_headers_(incoming_headers) {}

  opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const
      noexcept override {
    auto it = incoming_headers_.find(std::string(key));
    if (it == incoming_headers_.end()) return opentelemetry::nostd::string_view();
    return opentelemetry::nostd::string_view(it->second.data(), it->second.size());
  }
  void Set(opentelemetry::nostd::string_view,
           opentelemetry::nostd::string_view) noexcept override {}

 private:
  const CallHeaders& incoming_headers_;
};
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

}  // namespace

class ARROW_FLIGHT_EXPORT TracingServerMiddleware::Impl {
 public:
  Impl(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span,
       opentelemetry::nostd::unique_ptr<opentelemetry::trace::Scope> scope,
       opentelemetry::nostd::unique_ptr<opentelemetry::context::Token> token)
      : span_(std::move(span)), scope_(std::move(scope)), token_(std::move(token)) {}

  void SendingHeaders(AddCallHeaders* outgoing_headers) {
    auto context = opentelemetry::context::GetDefaultStorage()->GetCurrent();
    auto carrier = OutgoingTextMapCarrier(outgoing_headers);
    opentelemetry::trace::propagation::HttpTraceContext().Inject(carrier, context);
  }
  void CallCompleted(const Status& status) {
    arrow::internal::tracing::MarkSpan(status, span_.get());
    span_->End();
    opentelemetry::context::GetDefaultStorage()->Detach(*token_);
  }
  std::string GetTraceId() const {
    constexpr size_t kBufferSize = 2 * opentelemetry::trace::TraceId::kSize;
    char buffer[kBufferSize];
    span_->GetContext().trace_id().ToLowerBase16(
        opentelemetry::nostd::span<char, kBufferSize>(buffer, kBufferSize));
    return std::string(buffer, kBufferSize);
  }
  std::string GetSpanId() const {
    constexpr size_t kBufferSize = 2 * opentelemetry::trace::SpanId::kSize;
    char buffer[kBufferSize];
    span_->GetContext().span_id().ToLowerBase16(
        opentelemetry::nostd::span<char, kBufferSize>(buffer, kBufferSize));
    return std::string(buffer, kBufferSize);
  }

 private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::nostd::unique_ptr<opentelemetry::trace::Scope> scope_;
  opentelemetry::nostd::unique_ptr<opentelemetry::context::Token> token_;
};

namespace {
constexpr char kSpanNameInvalid[] = "arrow::flight::Invalid";
constexpr char kSpanNameHandshake[] = "arrow::flight::Handshake";
constexpr char kSpanNameListFlights[] = "arrow::flight::ListFlights";
constexpr char kSpanNameGetFlightInfo[] = "arrow::flight::GetFlightInfo";
constexpr char kSpanNameGetSchema[] = "arrow::flight::GetSchema";
constexpr char kSpanNameDoGet[] = "arrow::flight::DoGet";
constexpr char kSpanNameDoPut[] = "arrow::flight::DoPut";
constexpr char kSpanNameDoAction[] = "arrow::flight::DoAction";
constexpr char kSpanNameListActions[] = "arrow::flight::ListActions";
constexpr char kSpanNameDoExchange[] = "arrow::flight::DoExchange";

opentelemetry::nostd::string_view GetSpanName(FlightMethod method) {
  switch (method) {
    case FlightMethod::Invalid:
      return kSpanNameInvalid;
    case FlightMethod::Handshake:
      return kSpanNameHandshake;
    case FlightMethod::ListFlights:
      return kSpanNameListFlights;
    case FlightMethod::GetFlightInfo:
      return kSpanNameGetFlightInfo;
    case FlightMethod::GetSchema:
      return kSpanNameGetSchema;
    case FlightMethod::DoGet:
      return kSpanNameDoGet;
    case FlightMethod::DoPut:
      return kSpanNameDoPut;
    case FlightMethod::DoAction:
      return kSpanNameDoAction;
    case FlightMethod::ListActions:
      return kSpanNameListActions;
    case FlightMethod::DoExchange:
      return kSpanNameDoExchange;
  }
  return kSpanNameInvalid;
}

class ARROW_FLIGHT_EXPORT TracingServerMiddlewareFactory
    : public ServerMiddlewareFactory {
 public:
  TracingServerMiddlewareFactory() : tracer_(arrow::internal::tracing::GetTracer()) {}

  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    // Extract incoming span (if any) from headers
    auto context = opentelemetry::context::GetDefaultStorage()->GetCurrent();
    auto carrier = IncomingTextMapCarrier(incoming_headers);
    auto new_context =
        opentelemetry::trace::propagation::HttpTraceContext().Extract(carrier, context);
    auto token = opentelemetry::context::GetDefaultStorage()->Attach(new_context);

    auto span = tracer_->StartSpan(GetSpanName(info.method));
    auto scope = tracer_->WithActiveSpan(span);
    *middleware = std::make_shared<TracingServerMiddleware>(
        arrow::internal::make_unique<TracingServerMiddleware::Impl>(
            std::move(span), std::move(scope), std::move(token)));
    return Status::OK();
  }

 private:
  opentelemetry::trace::Tracer* tracer_;
};
}  // namespace

TracingServerMiddleware::TracingServerMiddleware(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
void TracingServerMiddleware::SendingHeaders(AddCallHeaders* outgoing_headers) {
  impl_->SendingHeaders(outgoing_headers);
}
void TracingServerMiddleware::CallCompleted(const Status& status) {
  impl_->CallCompleted(status);
}
std::string TracingServerMiddleware::GetTraceId() const { return impl_->GetTraceId(); }
std::string TracingServerMiddleware::GetSpanId() const { return impl_->GetSpanId(); }
constexpr char TracingServerMiddleware::kMiddlewareKey[];

std::shared_ptr<ServerMiddlewareFactory> MakeTracingServerMiddlewareFactory() {
  return std::make_shared<TracingServerMiddlewareFactory>();
}

}  // namespace flight
}  // namespace arrow
