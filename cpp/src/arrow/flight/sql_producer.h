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

// Interfaces to use for defining FlightSQL servers. API should be considered
// experimental for now

#pragma once

#include "arrow/flight/sql_types.h"
#include "arrow/flight/types.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace flight {

class FlightDataStream;
class FlightServerBase;
class ServerCallContext;

namespace sql {

class ARROW_FLIGHT_EXPORT FlightSqlBase {
 public:
  virtual arrow::Result<SqlInfo> GetSqlInfo(const ServerCallContext& context) = 0;
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetQueryStatementInfo(
      const ServerCallContext& context, const QueryStatement& query) = 0;
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> ExecuteQueryStatement(
      const ServerCallContext& context, const Ticket& request) = 0;
  // TODO: should this instead be a stream of UpdateResult?
  virtual arrow::Result<UpdateResult> ExecuteUpdateStatement(
      const ServerCallContext& context, const UpdateStatement& update,
      std::unique_ptr<MetadataRecordBatchReader> reader) = 0;
};

std::unique_ptr<FlightServerBase> MakeServer(std::unique_ptr<FlightSqlBase> server);

}  // namespace sql
}  // namespace flight
}  // namespace arrow
