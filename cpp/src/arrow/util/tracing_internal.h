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

#include <memory>

#include <opentelemetry/trace/provider.h>

#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {
namespace tracing {

ARROW_EXPORT
opentelemetry::trace::Tracer* GetTracer();

// TODO: maybe these should be macros?
inline void MarkSpan(const Status& s, opentelemetry::trace::Span* span) {
  if (!s.ok()) {
    span->SetStatus(opentelemetry::trace::StatusCode::kError, s.ToString());
  } else {
    span->SetStatus(opentelemetry::trace::StatusCode::kOk);
  }
}

template <typename T>
inline Result<T> MarkSpan(Result<T> result, opentelemetry::trace::Span* span) {
  MarkSpan(result.status(), span);
  return result;
}

template <typename T>
Iterator<T> WrapIterator(
    Iterator<T> wrapped,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> parent_span,
    const std::string& span_name) {
  struct {
    Result<T> operator()() {
      opentelemetry::trace::StartSpanOptions options;
      options.parent = parent_span->GetContext();
      auto span = GetTracer()->StartSpan(span_name, options);
      auto scope = GetTracer()->WithActiveSpan(span);
      return wrapped.Next();
    }

    Iterator<T> wrapped;
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> parent_span;
    std::string span_name;
  } Wrapper;
  Wrapper.wrapped = std::move(wrapped);
  Wrapper.parent_span = std::move(parent_span);
  Wrapper.span_name = span_name;
  return MakeFunctionIterator(std::move(Wrapper));
}

template <typename T>
AsyncGenerator<T> WrapAsyncGenerator(
    AsyncGenerator<T> wrapped,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> parent_span,
    const std::string& span_name) {
  // TODO: what Future/Executor should do is automatically propagate the span context
  return [=]() mutable -> Future<T> {
    auto scope = GetTracer()->WithActiveSpan(parent_span);
    auto span = GetTracer()->StartSpan(span_name);
    auto fut = wrapped();
    fut.AddCallback([=](const Result<T>& result) {
      if (!result.ok()) {
        span->SetStatus(opentelemetry::trace::StatusCode::kError,
                        result.status().ToString());
      } else {
        span->SetStatus(opentelemetry::trace::StatusCode::kOk);
      }
      span->End();
      if (!result.ok() || IsIterationEnd<T>(*result)) parent_span->End();
    });
    return fut;
  };
}

}  // namespace tracing
}  // namespace internal
}  // namespace arrow
