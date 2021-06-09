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

#include "arrow/compute/api_aggregate.h"

#include "arrow/compute/exec.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// Function options

using ::arrow::internal::checked_cast;

namespace internal {
template <>
struct EnumTraits<QuantileOptions::Interpolation>
    : BasicEnumTraits<QuantileOptions::Interpolation> {
  static std::string name() { return "QuantileOptions::Interpolation"; }
  static std::array<QuantileOptions::Interpolation, 5> values() {
    return {
        QuantileOptions::LINEAR,  QuantileOptions::LOWER,    QuantileOptions::HIGHER,
        QuantileOptions::NEAREST, QuantileOptions::MIDPOINT,
    };
  }
};
namespace {
using ::arrow::internal::DataMember;
static auto kScalarAggregateOptionsType = GetFunctionOptionsType<ScalarAggregateOptions>(
    DataMember("skip_nulls", &ScalarAggregateOptions::skip_nulls),
    DataMember("min_count", &ScalarAggregateOptions::min_count));
static auto kModeOptionsType =
    GetFunctionOptionsType<ModeOptions>(DataMember("n", &ModeOptions::n));
static auto kVarianceOptionsType =
    GetFunctionOptionsType<VarianceOptions>(DataMember("ddof", &VarianceOptions::ddof));
static auto kQuantileOptionsType = GetFunctionOptionsType<QuantileOptions>(
    DataMember("q", &QuantileOptions::q),
    DataMember("interpolation", &QuantileOptions::interpolation));
static auto kTDigestOptionsType = GetFunctionOptionsType<TDigestOptions>(
    DataMember("q", &TDigestOptions::q), DataMember("delta", &TDigestOptions::delta),
    DataMember("buffer_size", &TDigestOptions::buffer_size));
static auto kIndexOptionsType =
    GetFunctionOptionsType<IndexOptions>(DataMember("value", &IndexOptions::value));
}  // namespace
}  // namespace internal

ScalarAggregateOptions::ScalarAggregateOptions(bool skip_nulls, uint32_t min_count)
    : FunctionOptions(internal::kScalarAggregateOptionsType),
      skip_nulls(skip_nulls),
      min_count(min_count) {}
constexpr char ScalarAggregateOptions::kTypeName[];

ModeOptions::ModeOptions(int64_t n) : FunctionOptions(internal::kModeOptionsType), n(n) {}
constexpr char ModeOptions::kTypeName[];

VarianceOptions::VarianceOptions(int ddof)
    : FunctionOptions(internal::kVarianceOptionsType), ddof(ddof) {}
constexpr char VarianceOptions::kTypeName[];

QuantileOptions::QuantileOptions(double q, enum Interpolation interpolation)
    : FunctionOptions(internal::kQuantileOptionsType),
      q{q},
      interpolation{interpolation} {}
QuantileOptions::QuantileOptions(std::vector<double> q, enum Interpolation interpolation)
    : FunctionOptions(internal::kQuantileOptionsType),
      q{std::move(q)},
      interpolation{interpolation} {}
constexpr char QuantileOptions::kTypeName[];

TDigestOptions::TDigestOptions(double q, uint32_t delta, uint32_t buffer_size)
    : FunctionOptions(internal::kTDigestOptionsType),
      q{q},
      delta{delta},
      buffer_size{buffer_size} {}
TDigestOptions::TDigestOptions(std::vector<double> q, uint32_t delta,
                               uint32_t buffer_size)
    : FunctionOptions(internal::kTDigestOptionsType),
      q{std::move(q)},
      delta{delta},
      buffer_size{buffer_size} {}
constexpr char TDigestOptions::kTypeName[];

IndexOptions::IndexOptions(std::shared_ptr<Scalar> value)
    : FunctionOptions(internal::kIndexOptionsType), value{std::move(value)} {}
IndexOptions::IndexOptions() : IndexOptions(std::make_shared<NullScalar>()) {}
constexpr char IndexOptions::kTypeName[];

namespace internal {
void RegisterAggregateOptions(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunctionOptionsType(kScalarAggregateOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kModeOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kVarianceOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kQuantileOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kTDigestOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kIndexOptionsType));
}
}  // namespace internal

// ----------------------------------------------------------------------
// Scalar aggregates

Result<Datum> Count(const Datum& value, const ScalarAggregateOptions& options,
                    ExecContext* ctx) {
  return CallFunction("count", {value}, &options, ctx);
}

Result<Datum> Mean(const Datum& value, const ScalarAggregateOptions& options,
                   ExecContext* ctx) {
  return CallFunction("mean", {value}, &options, ctx);
}

Result<Datum> Sum(const Datum& value, const ScalarAggregateOptions& options,
                  ExecContext* ctx) {
  return CallFunction("sum", {value}, &options, ctx);
}

Result<Datum> MinMax(const Datum& value, const ScalarAggregateOptions& options,
                     ExecContext* ctx) {
  return CallFunction("min_max", {value}, &options, ctx);
}

Result<Datum> Any(const Datum& value, ExecContext* ctx) {
  return CallFunction("any", {value}, ctx);
}

Result<Datum> All(const Datum& value, ExecContext* ctx) {
  return CallFunction("all", {value}, ctx);
}

Result<Datum> Mode(const Datum& value, const ModeOptions& options, ExecContext* ctx) {
  return CallFunction("mode", {value}, &options, ctx);
}

Result<Datum> Stddev(const Datum& value, const VarianceOptions& options,
                     ExecContext* ctx) {
  return CallFunction("stddev", {value}, &options, ctx);
}

Result<Datum> Variance(const Datum& value, const VarianceOptions& options,
                       ExecContext* ctx) {
  return CallFunction("variance", {value}, &options, ctx);
}

Result<Datum> Quantile(const Datum& value, const QuantileOptions& options,
                       ExecContext* ctx) {
  return CallFunction("quantile", {value}, &options, ctx);
}

Result<Datum> TDigest(const Datum& value, const TDigestOptions& options,
                      ExecContext* ctx) {
  return CallFunction("tdigest", {value}, &options, ctx);
}

Result<Datum> Index(const Datum& value, const IndexOptions& options, ExecContext* ctx) {
  return CallFunction("index", {value}, &options, ctx);
}

}  // namespace compute
}  // namespace arrow
