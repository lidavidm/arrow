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

#include "arrow/flight/types.h"

#include <memory>
#include <sstream>
#include <string_view>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/reader.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/string.h"
#include "arrow/util/string_builder.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {

namespace {
template <typename FlightType, typename ProtoType>
arrow::Result<std::string> SerializeTypeToString(const std::string& type_name,
                                                 const FlightType& value) {
  ProtoType proto;
  RETURN_NOT_OK(internal::ToProto(value, &proto));

  std::string out;
  if (!proto.SerializeToString(&out)) {
    return Status::IOError("Serialized ", type_name, " exceeded 2 GiB limit");
  }
  return out;
}
template <typename FlightType, typename ProtoType>
arrow::Result<FlightType> DeserializeTypeFromString(const std::string& type_name,
                                                    std::string_view serialized) {
  ProtoType proto;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized ", type_name, " size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!proto.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid ", type_name);
  }
  FlightType value;
  RETURN_NOT_OK(internal::FromProto(proto, &value));
  return value;
}
}  // namespace

const char* kSchemeGrpc = "grpc";
const char* kSchemeGrpcTcp = "grpc+tcp";
const char* kSchemeGrpcUnix = "grpc+unix";
const char* kSchemeGrpcTls = "grpc+tls";

const char* kErrorDetailTypeId = "flight::FlightStatusDetail";

const char* FlightStatusDetail::type_id() const { return kErrorDetailTypeId; }

std::string FlightStatusDetail::ToString() const { return CodeAsString(); }

FlightStatusCode FlightStatusDetail::code() const { return code_; }

std::string FlightStatusDetail::extra_info() const { return extra_info_; }

void FlightStatusDetail::set_extra_info(std::string extra_info) {
  extra_info_ = std::move(extra_info);
}

std::string FlightStatusDetail::CodeAsString() const {
  switch (code()) {
    case FlightStatusCode::Internal:
      return "Internal";
    case FlightStatusCode::TimedOut:
      return "TimedOut";
    case FlightStatusCode::Cancelled:
      return "Cancelled";
    case FlightStatusCode::Unauthenticated:
      return "Unauthenticated";
    case FlightStatusCode::Unauthorized:
      return "Unauthorized";
    case FlightStatusCode::Unavailable:
      return "Unavailable";
    case FlightStatusCode::Failed:
      return "Failed";
    default:
      return "Unknown";
  }
}

std::shared_ptr<FlightStatusDetail> FlightStatusDetail::UnwrapStatus(
    const arrow::Status& status) {
  if (!status.detail() || status.detail()->type_id() != kErrorDetailTypeId) {
    return nullptr;
  }
  return std::dynamic_pointer_cast<FlightStatusDetail>(status.detail());
}

Status MakeFlightError(FlightStatusCode code, std::string message,
                       std::string extra_info) {
  StatusCode arrow_code = arrow::StatusCode::IOError;
  return arrow::Status(arrow_code, std::move(message),
                       std::make_shared<FlightStatusDetail>(code, std::move(extra_info)));
}

bool FlightDescriptor::Equals(const FlightDescriptor& other) const {
  if (type != other.type) {
    return false;
  }
  switch (type) {
    case PATH:
      return path == other.path;
    case CMD:
      return cmd == other.cmd;
    default:
      return false;
  }
}

std::string FlightDescriptor::ToString() const {
  std::stringstream ss;
  ss << "<FlightDescriptor ";
  switch (type) {
    case PATH: {
      ss << "path='";
      bool first = true;
      for (const auto& p : path) {
        if (!first) {
          ss << "/";
        }
        first = false;
        ss << p;
      }
      ss << "'";
      break;
    }
    case CMD:
      ss << "cmd='" << cmd << "'";
      break;
    default:
      break;
  }
  ss << ">";
  return ss.str();
}

Status FlightPayload::Validate() const {
  static constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();
  if (descriptor && descriptor->size() > kInt32Max) {
    return Status::CapacityError("Descriptor size overflow (>= 2**31)");
  }
  if (app_metadata && app_metadata->size() > kInt32Max) {
    return Status::CapacityError("app_metadata size overflow (>= 2**31)");
  }
  if (ipc_message.body_length > kInt32Max) {
    return Status::Invalid("Cannot send record batches exceeding 2GiB yet");
  }
  return Status::OK();
}

arrow::Result<std::shared_ptr<Schema>> SchemaResult::GetSchema(
    ipc::DictionaryMemo* dictionary_memo) const {
  io::BufferReader schema_reader(raw_schema_);
  return ipc::ReadSchema(&schema_reader, dictionary_memo);
}

arrow::Result<std::unique_ptr<SchemaResult>> SchemaResult::Make(const Schema& schema) {
  std::string schema_in;
  RETURN_NOT_OK(internal::SchemaToString(schema, &schema_in));
  return std::make_unique<SchemaResult>(std::move(schema_in));
}

Status SchemaResult::GetSchema(ipc::DictionaryMemo* dictionary_memo,
                               std::shared_ptr<Schema>* out) const {
  return GetSchema(dictionary_memo).Value(out);
}

std::string SchemaResult::ToString() const {
  return "<SchemaResult raw_schema=(serialized)>";
}

bool SchemaResult::Equals(const SchemaResult& other) const {
  return raw_schema_ == other.raw_schema_;
}

arrow::Result<std::string> SchemaResult::SerializeToString() const {
  return SerializeTypeToString<SchemaResult, pb::SchemaResult>("SchemaResult", *this);
}

arrow::Result<SchemaResult> SchemaResult::Deserialize(std::string_view serialized) {
  ARROW_ASSIGN_OR_RAISE(std::string schema,
                        (DeserializeTypeFromString<std::string, pb::SchemaResult>(
                            "SchemaResult", serialized)));
  return SchemaResult(std::move(schema));
}

arrow::Result<std::string> FlightDescriptor::SerializeToString() const {
  return SerializeTypeToString<FlightDescriptor, pb::FlightDescriptor>("FlightDescriptor",
                                                                       *this);
}

Status FlightDescriptor::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<FlightDescriptor> FlightDescriptor::Deserialize(
    std::string_view serialized) {
  return DeserializeTypeFromString<FlightDescriptor, pb::FlightDescriptor>(
      "FlightDescriptor", serialized);
}

Status FlightDescriptor::Deserialize(const std::string& serialized,
                                     FlightDescriptor* out) {
  return Deserialize(serialized).Value(out);
}

std::string Ticket::ToString() const {
  std::stringstream ss;
  ss << "<Ticket ticket='" << ticket << "'>";
  return ss.str();
}

bool Ticket::Equals(const Ticket& other) const { return ticket == other.ticket; }

arrow::Result<std::string> Ticket::SerializeToString() const {
  return SerializeTypeToString<Ticket, pb::Ticket>("Ticket", *this);
}

Status Ticket::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<Ticket> Ticket::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<Ticket, pb::Ticket>("Ticket", serialized);
}

Status Ticket::Deserialize(const std::string& serialized, Ticket* out) {
  return Deserialize(serialized).Value(out);
}

FlightInfo::FlightInfo()
    : data_{"", {FlightDescriptor::DescriptorType::UNKNOWN, "", {}}, {}, -1, -1},
      schema_(nullptr),
      reconstructed_schema_(false) {}

arrow::Result<FlightInfo> FlightInfo::Make(const Schema& schema,
                                           const FlightDescriptor& descriptor,
                                           const std::vector<FlightEndpoint>& endpoints,
                                           int64_t total_records, int64_t total_bytes) {
  FlightInfo::Data data;
  data.descriptor = descriptor;
  data.endpoints = endpoints;
  data.total_records = total_records;
  data.total_bytes = total_bytes;
  RETURN_NOT_OK(internal::SchemaToString(schema, &data.schema));
  return FlightInfo(data);
}

arrow::Result<std::shared_ptr<Schema>> FlightInfo::GetSchema(
    ipc::DictionaryMemo* dictionary_memo) const {
  if (reconstructed_schema_) {
    return schema_;
  }
  io::BufferReader schema_reader(data_.schema);
  RETURN_NOT_OK(ipc::ReadSchema(&schema_reader, dictionary_memo).Value(&schema_));
  reconstructed_schema_ = true;
  return schema_;
}

Status FlightInfo::GetSchema(ipc::DictionaryMemo* dictionary_memo,
                             std::shared_ptr<Schema>* out) const {
  return GetSchema(dictionary_memo).Value(out);
}

arrow::Result<std::string> FlightInfo::SerializeToString() const {
  return SerializeTypeToString<FlightInfo, pb::FlightInfo>("FlightInfo", *this);
}

Status FlightInfo::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightInfo::Deserialize(
    std::string_view serialized) {
  ARROW_ASSIGN_OR_RAISE(auto data,
                        (DeserializeTypeFromString<FlightInfo::Data, pb::FlightInfo>(
                            "FlightInfo", serialized)));
  return std::make_unique<FlightInfo>(std::move(data));
}

Status FlightInfo::Deserialize(const std::string& serialized,
                               std::unique_ptr<FlightInfo>* out) {
  return Deserialize(serialized).Value(out);
}

std::string FlightInfo::ToString() const {
  std::stringstream ss;
  ss << "<FlightInfo schema=";
  if (schema_) {
    ss << schema_->ToString();
  } else {
    ss << "(serialized)";
  }
  ss << " descriptor=" << data_.descriptor.ToString();
  ss << " endpoints=[";
  bool first = true;
  for (const auto& endpoint : data_.endpoints) {
    if (!first) ss << ", ";
    ss << endpoint.ToString();
    first = false;
  }
  ss << "] total_records=" << data_.total_records;
  ss << " total_bytes=" << data_.total_bytes;
  ss << '>';
  return ss.str();
}

bool FlightInfo::Equals(const FlightInfo& other) const {
  return data_.schema == other.data_.schema &&
         data_.descriptor == other.data_.descriptor &&
         data_.endpoints == other.data_.endpoints &&
         data_.total_records == other.data_.total_records &&
         data_.total_bytes == other.data_.total_bytes;
}

RetryInfo::RetryInfo() = default;
RetryInfo::RetryInfo(FlightInfo info_param, FlightDescriptor retry_descriptor_param,
                     std::optional<double> progress_param,
                     std::chrono::system_clock::time_point expiration_time_param)
    : info(std::move(info_param)),
      retry_descriptor(std::move(retry_descriptor_param)),
      progress(progress_param),
      expiration_time(expiration_time_param) {}
arrow::Result<std::string> RetryInfo::SerializeToString() const {
  return SerializeTypeToString<RetryInfo, pb::RetryInfo>("RetryInfo", *this);
}
arrow::Result<RetryInfo> RetryInfo::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<RetryInfo, pb::RetryInfo>("RetryInfo", serialized);
}
bool RetryInfo::Equals(const RetryInfo& other) const {
  return info == other.info && retry_descriptor == other.retry_descriptor &&
         progress == other.progress && expiration_time == other.expiration_time;
}
std::string RetryInfo::ToString() const {
  std::stringstream ss;
  ss << "<RetryInfo info=" << info.ToString()
     << " retry_descriptor=" << retry_descriptor.ToString() << " progress=";
  if (progress.has_value()) {
    ss << arrow::internal::ToChars(*progress);
  } else {
    ss << "(nullopt)";
  }
  ss << " expiration_time=" << expiration_time.time_since_epoch().count() << '>';
  return ss.str();
}

Location::Location() { uri_ = std::make_shared<arrow::internal::Uri>(); }

Status FlightListing::Next(std::unique_ptr<FlightInfo>* info) {
  return Next().Value(info);
}

arrow::Result<Location> Location::Parse(const std::string& uri_string) {
  Location location;
  RETURN_NOT_OK(location.uri_->Parse(uri_string));
  return location;
}

Status Location::Parse(const std::string& uri_string, Location* location) {
  return Parse(uri_string).Value(location);
}

arrow::Result<Location> Location::ForGrpcTcp(const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << "grpc+tcp://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

Status Location::ForGrpcTcp(const std::string& host, const int port, Location* location) {
  return ForGrpcTcp(host, port).Value(location);
}

arrow::Result<Location> Location::ForGrpcTls(const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << "grpc+tls://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

Status Location::ForGrpcTls(const std::string& host, const int port, Location* location) {
  return ForGrpcTls(host, port).Value(location);
}

arrow::Result<Location> Location::ForGrpcUnix(const std::string& path) {
  std::stringstream uri_string;
  uri_string << "grpc+unix://" << path;
  return Location::Parse(uri_string.str());
}

Status Location::ForGrpcUnix(const std::string& path, Location* location) {
  return ForGrpcUnix(path).Value(location);
}

arrow::Result<Location> Location::ForScheme(const std::string& scheme,
                                            const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << scheme << "://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

std::string Location::ToString() const { return uri_->ToString(); }
std::string Location::scheme() const {
  std::string scheme = uri_->scheme();
  if (scheme.empty()) {
    // Default to grpc+tcp
    return "grpc+tcp";
  }
  return scheme;
}

bool Location::Equals(const Location& other) const {
  return ToString() == other.ToString();
}

std::string FlightEndpoint::ToString() const {
  std::stringstream ss;
  ss << "<FlightEndpoint ticket=" << ticket.ToString();
  ss << " locations=[";
  bool first = true;
  for (const auto& location : locations) {
    if (!first) ss << ", ";
    ss << location.ToString();
    first = false;
  }
  ss << "]>";
  return ss.str();
}

bool FlightEndpoint::Equals(const FlightEndpoint& other) const {
  return ticket == other.ticket && locations == other.locations;
}

arrow::Result<std::string> FlightEndpoint::SerializeToString() const {
  return SerializeTypeToString<FlightEndpoint, pb::FlightEndpoint>("FlightEndpoint",
                                                                   *this);
}

arrow::Result<FlightEndpoint> FlightEndpoint::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<FlightEndpoint, pb::FlightEndpoint>("FlightEndpoint",
                                                                       serialized);
}

std::string ActionType::ToString() const {
  return arrow::util::StringBuilder("<ActionType type='", type, "' description='",
                                    description, "'>");
}

bool ActionType::Equals(const ActionType& other) const {
  return type == other.type && description == other.description;
}

arrow::Result<std::string> ActionType::SerializeToString() const {
  return SerializeTypeToString<ActionType, pb::ActionType>("ActionType", *this);
}

arrow::Result<ActionType> ActionType::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<ActionType, pb::ActionType>("ActionType", serialized);
}

std::string Criteria::ToString() const {
  return arrow::util::StringBuilder("<Criteria expression='", expression, "'>");
}

bool Criteria::Equals(const Criteria& other) const {
  return expression == other.expression;
}

arrow::Result<std::string> Criteria::SerializeToString() const {
  return SerializeTypeToString<Criteria, pb::Criteria>("Criteria", *this);
}

arrow::Result<Criteria> Criteria::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<Criteria, pb::Criteria>("Criteria", serialized);
}

std::string Action::ToString() const {
  std::stringstream ss;
  ss << "<Action type='" << type;
  ss << "' body=";
  if (body) {
    ss << "(" << body->size() << " bytes)";
  } else {
    ss << "(nullptr)";
  }
  ss << '>';
  return ss.str();
}

bool Action::Equals(const Action& other) const {
  return (type == other.type) &&
         ((body == other.body) || (body && other.body && body->Equals(*other.body)));
}

arrow::Result<std::string> Action::SerializeToString() const {
  return SerializeTypeToString<Action, pb::Action>("Action", *this);
}

arrow::Result<Action> Action::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<Action, pb::Action>("Action", serialized);
}

std::string Result::ToString() const {
  std::stringstream ss;
  ss << "<Result body=";
  if (body) {
    ss << "(" << body->size() << " bytes)>";
  } else {
    ss << "(nullptr)>";
  }
  return ss.str();
}

bool Result::Equals(const Result& other) const {
  return (body == other.body) || (body && other.body && body->Equals(*other.body));
}

arrow::Result<std::string> Result::SerializeToString() const {
  return SerializeTypeToString<Result, pb::Result>("Result", *this);
}

arrow::Result<Result> Result::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<Result, pb::Result>("Result", serialized);
}

// --- ActionCancelQueryRequest ------------------------------

ActionCancelQueryRequest::ActionCancelQueryRequest() = default;
ActionCancelQueryRequest::ActionCancelQueryRequest(FlightInfo info_param)
    : info(std::move(info_param)) {}

arrow::Result<std::string> ActionCancelQueryRequest::SerializeToString() const {
  return SerializeTypeToString<ActionCancelQueryRequest, pb::ActionCancelQueryRequest>(
      "ActionCancelQueryRequest", *this);
}
arrow::Result<ActionCancelQueryRequest> ActionCancelQueryRequest::Deserialize(
    std::string_view serialized) {
  return DeserializeTypeFromString<ActionCancelQueryRequest,
                                   pb::ActionCancelQueryRequest>(
      "ActionCancelQueryRequest", serialized);
}
arrow::Result<Action> ActionCancelQueryRequest::SerializeToAction() const {
  ARROW_ASSIGN_OR_RAISE(std::string body, SerializeToString());
  return Action{ActionCancelQueryRequest::kActionType.type,
                Buffer::FromString(std::move(body))};
}
arrow::Result<ActionCancelQueryRequest> ActionCancelQueryRequest::Deserialize(
    const Action& action) {
  if (action.type != ActionCancelQueryRequest::kActionType.type) {
    return Status::Invalid("Action type is invalid, expected '",
                           ActionCancelQueryRequest::kActionType.type, "' but got '",
                           action.type, "'");
  }
  return Deserialize(std::string_view(*action.body));
}
bool ActionCancelQueryRequest::Equals(const ActionCancelQueryRequest& other) const {
  return false;  // TODO:
}
std::string ActionCancelQueryRequest::ToString() const {
  return "";  // TODO:
}
const ActionType ActionCancelQueryRequest::kActionType{
    "arrow.flight.ActionCancelQueryRequest", "TODO:"};

// --- ActionCancelQueryResult ------------------------------

ActionCancelQueryResult::ActionCancelQueryResult() = default;
ActionCancelQueryResult::ActionCancelQueryResult(CancelResult result_param)
    : result(result_param) {}

arrow::Result<std::string> ActionCancelQueryResult::SerializeToString() const {
  return SerializeTypeToString<ActionCancelQueryResult, pb::ActionCancelQueryResult>(
      "ActionCancelQueryResult", *this);
}
arrow::Result<ActionCancelQueryResult> ActionCancelQueryResult::Deserialize(
    std::string_view serialized) {
  return DeserializeTypeFromString<ActionCancelQueryResult, pb::ActionCancelQueryResult>(
      "ActionCancelQueryResult", serialized);
}
arrow::Result<Result> ActionCancelQueryResult::SerializeToActionResult() const {
  ARROW_ASSIGN_OR_RAISE(std::string body, SerializeToString());
  return Result{Buffer::FromString(std::move(body))};
}
arrow::Result<ActionCancelQueryResult> ActionCancelQueryResult::Deserialize(
    const Result& result) {
  return Deserialize(std::string_view(*result.body));
}
bool ActionCancelQueryResult::Equals(const ActionCancelQueryResult& other) const {
  return false;  // TODO:
}
std::string ActionCancelQueryResult::ToString() const {
  return "";  // TODO:
}

// --- ActionCloseQueryRequest ------------------------------

ActionCloseQueryRequest::ActionCloseQueryRequest() = default;
ActionCloseQueryRequest::ActionCloseQueryRequest(FlightInfo info_param)
    : info(std::move(info_param)) {}

arrow::Result<std::string> ActionCloseQueryRequest::SerializeToString() const {
  return SerializeTypeToString<ActionCloseQueryRequest, pb::ActionCloseQueryRequest>(
      "ActionCloseQueryRequest", *this);
}
arrow::Result<ActionCloseQueryRequest> ActionCloseQueryRequest::Deserialize(
    std::string_view serialized) {
  return DeserializeTypeFromString<ActionCloseQueryRequest, pb::ActionCloseQueryRequest>(
      "ActionCloseQueryRequest", serialized);
}
arrow::Result<Action> ActionCloseQueryRequest::SerializeToAction() const {
  ARROW_ASSIGN_OR_RAISE(std::string body, SerializeToString());
  return Action{ActionCloseQueryRequest::kActionType.type,
                Buffer::FromString(std::move(body))};
}
arrow::Result<ActionCloseQueryRequest> ActionCloseQueryRequest::Deserialize(
    const Action& action) {
  if (action.type != ActionCloseQueryRequest::kActionType.type) {
    return Status::Invalid("Action type is invalid, expected '",
                           ActionCloseQueryRequest::kActionType.type, "' but got '",
                           action.type, "'");
  }
  return Deserialize(std::string_view(*action.body));
}
bool ActionCloseQueryRequest::Equals(const ActionCloseQueryRequest& other) const {
  return false;  // TODO:
}
std::string ActionCloseQueryRequest::ToString() const {
  return "";  // TODO:
}
const ActionType ActionCloseQueryRequest::kActionType{
    "arrow.flight.ActionCloseQueryRequest", "TODO:"};

// --- ActionCloseQueryResult ------------------------------

ActionCloseQueryResult::ActionCloseQueryResult() = default;
ActionCloseQueryResult::ActionCloseQueryResult(CloseResult result_param)
    : result(result_param) {}

arrow::Result<std::string> ActionCloseQueryResult::SerializeToString() const {
  return SerializeTypeToString<ActionCloseQueryResult, pb::ActionCloseQueryResult>(
      "ActionCloseQueryResult", *this);
}
arrow::Result<ActionCloseQueryResult> ActionCloseQueryResult::Deserialize(
    std::string_view serialized) {
  return DeserializeTypeFromString<ActionCloseQueryResult, pb::ActionCloseQueryResult>(
      "ActionCloseQueryResult", serialized);
}
arrow::Result<Result> ActionCloseQueryResult::SerializeToActionResult() const {
  ARROW_ASSIGN_OR_RAISE(std::string body, SerializeToString());
  return Result{Buffer::FromString(std::move(body))};
}
arrow::Result<ActionCloseQueryResult> ActionCloseQueryResult::Deserialize(
    const Result& result) {
  return Deserialize(std::string_view(*result.body));
}
bool ActionCloseQueryResult::Equals(const ActionCloseQueryResult& other) const {
  return false;  // TODO:
}
std::string ActionCloseQueryResult::ToString() const {
  return "";  // TODO:
}

// --- ActionRefreshQueryRequest ------------------------------

ActionRefreshQueryRequest::ActionRefreshQueryRequest() = default;
ActionRefreshQueryRequest::ActionRefreshQueryRequest(
    FlightInfo info_param,
    std::optional<std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>>
        desired_expiration_time_param)
    : info(std::move(info_param)),
      desired_expiration_time(std::move(desired_expiration_time_param)) {}

arrow::Result<std::string> ActionRefreshQueryRequest::SerializeToString() const {
  return SerializeTypeToString<ActionRefreshQueryRequest, pb::ActionRefreshQueryRequest>(
      "ActionRefreshQueryRequest", *this);
}
arrow::Result<ActionRefreshQueryRequest> ActionRefreshQueryRequest::Deserialize(
    std::string_view serialized) {
  return DeserializeTypeFromString<ActionRefreshQueryRequest,
                                   pb::ActionRefreshQueryRequest>(
      "ActionRefreshQueryRequest", serialized);
}
arrow::Result<Action> ActionRefreshQueryRequest::SerializeToAction() const {
  ARROW_ASSIGN_OR_RAISE(std::string body, SerializeToString());
  return Action{ActionRefreshQueryRequest::kActionType.type,
                Buffer::FromString(std::move(body))};
}
arrow::Result<ActionRefreshQueryRequest> ActionRefreshQueryRequest::Deserialize(
    const Action& action) {
  if (action.type != ActionRefreshQueryRequest::kActionType.type) {
    return Status::Invalid("Action type is invalid, expected '",
                           ActionRefreshQueryRequest::kActionType.type, "' but got '",
                           action.type, "'");
  }
  return Deserialize(std::string_view(*action.body));
}
bool ActionRefreshQueryRequest::Equals(const ActionRefreshQueryRequest& other) const {
  return false;  // TODO:
}
std::string ActionRefreshQueryRequest::ToString() const {
  return "";  // TODO:
}
const ActionType ActionRefreshQueryRequest::kActionType{
    "arrow.flight.ActionRefreshQueryRequest", "TODO:"};

// --- ActionRefreshQueryResult ------------------------------

ActionRefreshQueryResult::ActionRefreshQueryResult() = default;
ActionRefreshQueryResult::ActionRefreshQueryResult(
    std::optional<FlightInfo> new_info_param,
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>
        new_expiration_time_param)
    : new_info(std::move(new_info_param)),
      new_expiration_time(std::move(new_expiration_time_param)) {}

arrow::Result<std::string> ActionRefreshQueryResult::SerializeToString() const {
  return SerializeTypeToString<ActionRefreshQueryResult, pb::ActionRefreshQueryResult>(
      "ActionRefreshQueryResult", *this);
}
arrow::Result<ActionRefreshQueryResult> ActionRefreshQueryResult::Deserialize(
    std::string_view serialized) {
  return DeserializeTypeFromString<ActionRefreshQueryResult,
                                   pb::ActionRefreshQueryResult>(
      "ActionRefreshQueryResult", serialized);
}
arrow::Result<Result> ActionRefreshQueryResult::SerializeToActionResult() const {
  ARROW_ASSIGN_OR_RAISE(std::string body, SerializeToString());
  return Result{Buffer::FromString(std::move(body))};
}
arrow::Result<ActionRefreshQueryResult> ActionRefreshQueryResult::Deserialize(
    const Result& result) {
  return Deserialize(std::string_view(*result.body));
}
bool ActionRefreshQueryResult::Equals(const ActionRefreshQueryResult& other) const {
  return false;  // TODO:
}
std::string ActionRefreshQueryResult::ToString() const {
  return "";  // TODO:
}

Status ResultStream::Next(std::unique_ptr<Result>* info) { return Next().Value(info); }

Status MetadataRecordBatchReader::Next(FlightStreamChunk* next) {
  return Next().Value(next);
}

arrow::Result<std::vector<std::shared_ptr<RecordBatch>>>
MetadataRecordBatchReader::ToRecordBatches() {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, Next());
    if (!chunk.data) break;
    batches.emplace_back(std::move(chunk.data));
  }
  return batches;
}

Status MetadataRecordBatchReader::ReadAll(
    std::vector<std::shared_ptr<RecordBatch>>* batches) {
  return ToRecordBatches().Value(batches);
}

arrow::Result<std::shared_ptr<Table>> MetadataRecordBatchReader::ToTable() {
  ARROW_ASSIGN_OR_RAISE(auto batches, ToRecordBatches());
  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  return Table::FromRecordBatches(schema, std::move(batches));
}

Status MetadataRecordBatchReader::ReadAll(std::shared_ptr<Table>* table) {
  return ToTable().Value(table);
}

Status MetadataRecordBatchWriter::Begin(const std::shared_ptr<Schema>& schema) {
  return Begin(schema, ipc::IpcWriteOptions::Defaults());
}

namespace {
class MetadataRecordBatchReaderAdapter : public RecordBatchReader {
 public:
  explicit MetadataRecordBatchReaderAdapter(
      std::shared_ptr<Schema> schema, std::shared_ptr<MetadataRecordBatchReader> delegate)
      : schema_(std::move(schema)), delegate_(std::move(delegate)) {}
  std::shared_ptr<Schema> schema() const override { return schema_; }
  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    while (true) {
      ARROW_ASSIGN_OR_RAISE(FlightStreamChunk next, delegate_->Next());
      if (!next.data && !next.app_metadata) {
        // EOS
        *batch = nullptr;
        return Status::OK();
      } else if (next.data) {
        *batch = std::move(next.data);
        return Status::OK();
      }
      // Got metadata, but no data (which is valid) - read the next message
    }
  }

 private:
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<MetadataRecordBatchReader> delegate_;
};
};  // namespace

arrow::Result<std::shared_ptr<RecordBatchReader>> MakeRecordBatchReader(
    std::shared_ptr<MetadataRecordBatchReader> reader) {
  ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
  return std::make_shared<MetadataRecordBatchReaderAdapter>(std::move(schema),
                                                            std::move(reader));
}

SimpleFlightListing::SimpleFlightListing(const std::vector<FlightInfo>& flights)
    : position_(0), flights_(flights) {}

SimpleFlightListing::SimpleFlightListing(std::vector<FlightInfo>&& flights)
    : position_(0), flights_(std::move(flights)) {}

arrow::Result<std::unique_ptr<FlightInfo>> SimpleFlightListing::Next() {
  if (position_ >= static_cast<int>(flights_.size())) {
    return nullptr;
  }
  return std::make_unique<FlightInfo>(std::move(flights_[position_++]));
}

SimpleResultStream::SimpleResultStream(std::vector<Result>&& results)
    : results_(std::move(results)), position_(0) {}

arrow::Result<std::unique_ptr<Result>> SimpleResultStream::Next() {
  if (position_ >= results_.size()) {
    return nullptr;
  }
  return std::make_unique<Result>(std::move(results_[position_++]));
}

std::string BasicAuth::ToString() const {
  return arrow::util::StringBuilder("<BasicAuth username='", username,
                                    "' password=(redacted)>");
}

bool BasicAuth::Equals(const BasicAuth& other) const {
  return (username == other.username) && (password == other.password);
}

arrow::Result<BasicAuth> BasicAuth::Deserialize(std::string_view serialized) {
  return DeserializeTypeFromString<BasicAuth, pb::BasicAuth>("BasicAuth", serialized);
}

Status BasicAuth::Deserialize(const std::string& serialized, BasicAuth* out) {
  return Deserialize(serialized).Value(out);
}

arrow::Result<std::string> BasicAuth::SerializeToString() const {
  return SerializeTypeToString<BasicAuth, pb::BasicAuth>("BasicAuth", *this);
}

Status BasicAuth::Serialize(const BasicAuth& basic_auth, std::string* out) {
  return basic_auth.SerializeToString().Value(out);
}
}  // namespace flight
}  // namespace arrow
