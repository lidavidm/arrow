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

#include "arrow/flight/sql_types.h"

#include "arrow/buffer.h"
#include "arrow/config.h"

#include "arrow/flight/FlightSql.pb.h"

namespace arrow {
namespace flight {
namespace sql {

namespace pb = arrow::flight::protocol;

arrow::Result<std::shared_ptr<Buffer>> SqlInfo::Serialize(MemoryPool* pool) {
  pb::sql::ActionGetSqlInfoResult result;
  auto& server_info = *result.mutable_server_info();
  server_info.set_name(server_name);
  server_info.set_version(server_version);
  server_info.set_arrow_format_version("1.0.0");
  server_info.set_arrow_library_version(GetBuildInfo().version_string);
  auto length = result.ByteSizeLong();
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(length, pool));
  result.SerializeWithCachedSizesToArray(buffer->mutable_data());
  return buffer;
}

arrow::Result<SqlInfo> SqlInfo::Deserialize(const Buffer& buffer) {
  pb::sql::ActionGetSqlInfoResult result;
  // TODO: do we need catches?
  result.ParseFromArray(buffer.data(), buffer.size());
  SqlInfo info;
  info.server_name = result.server_info().name();
  info.server_version = result.server_info().version();
  info.arrow_format_version = result.server_info().arrow_format_version();
  info.arrow_library_version = result.server_info().arrow_library_version();
  return info;
}

FlightDescriptor QueryStatement::ToDescriptor() const {
  FlightDescriptor descriptor;
  descriptor.type = FlightDescriptor::DescriptorType::CMD;
  pb::sql::Command command;
  command.mutable_statement_query()->set_query(query);
  command.SerializeToString(&descriptor.cmd);
  return descriptor;
}

QueryStatement QueryStatement::FromQuery(const std::string& query) {
  return QueryStatement{query};
}

FlightDescriptor UpdateStatement::ToDescriptor() const {
  FlightDescriptor descriptor;
  descriptor.type = FlightDescriptor::DescriptorType::CMD;
  pb::sql::Command command;
  command.mutable_statement_update()->set_query(query);
  command.SerializeToString(&descriptor.cmd);
  return descriptor;
}

arrow::Result<std::shared_ptr<Buffer>> UpdateResult::Serialize(MemoryPool* pool) {
  pb::sql::DoPutUpdateResult result;
  result.set_record_count(record_count);
  auto length = result.ByteSizeLong();
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(length, pool));
  result.SerializeWithCachedSizesToArray(buffer->mutable_data());
  return buffer;
}

arrow::Result<UpdateResult> UpdateResult::Deserialize(const Buffer& buffer) {
  pb::sql::DoPutUpdateResult pb_result;
  // TODO: do we need catches?
  pb_result.ParseFromArray(buffer.data(), buffer.size());
  UpdateResult result;
  result.record_count = pb_result.record_count();
  return result;
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
