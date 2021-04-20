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

// Interfaces to use for writing FlightSQL clients. API should be considered
// experimental for now

#pragma once

#include <string>

#include "arrow/flight/client.h"
#include "arrow/flight/sql_types.h"
#include "arrow/flight/types.h"

namespace arrow {
namespace flight {
namespace sql {

arrow::Result<SqlInfo> GetSqlInfo(FlightClient* client, const FlightCallOptions& options);
arrow::Result<SqlInfo> GetSqlInfo(FlightClient* client) { return GetSqlInfo(client, {}); }

/// \brief Execute a non-bulk update query (e.g. a single CREATE TABLE or INSERT).
arrow::Result<UpdateResult> ExecuteSimpleUpdate(FlightClient* client,
                                                const FlightCallOptions& options,
                                                const UpdateStatement& query);
arrow::Result<UpdateResult> ExecuteSimpleUpdate(FlightClient* client,
                                                const UpdateStatement& query) {
  return ExecuteSimpleUpdate(client, {}, query);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
