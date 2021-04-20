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

#include <string>

#include "arrow/flight/types.h"

namespace arrow {
namespace flight {
namespace sql {

// Action types
constexpr char kGetSqlInfoAction[] = "GetSqlInfo";

constexpr char kSqlInfoServerName[] = "FLIGHT_SQL_SERVER_NAME";
constexpr char kSqlInfoServerVersion[] = "FLIGHT_SQL_SERVER_VERSION";
constexpr char kSqlInfoArrowFormatVersion[] = "FLIGHT_SQL_ARROW_FORMAT_VERSION";
constexpr char kSqlInfoArrowLibraryVersion[] = "FLIGHT_SQL_ARROW_LIBRARY_VERSION";

struct SqlInfo {
  std::string server_name;
  std::string server_version;
  std::string arrow_format_version;
  std::string arrow_library_version;

  // TODO: custom info, SQL info

  arrow::Result<std::shared_ptr<Buffer>> Serialize(MemoryPool* pool = NULLPTR);
  static arrow::Result<SqlInfo> Deserialize(const Buffer& buffer);
};

struct QueryStatement {
  std::string query;

  FlightDescriptor ToDescriptor() const;
  static QueryStatement FromQuery(const std::string& query);
};

struct UpdateStatement {
  std::string query;

  FlightDescriptor ToDescriptor() const;
};
// Not very "Arrow". Should be one query with a table of values. What
// sorts of APIs do columnar DBs provide?

struct UpdateResult {
  int64_t record_count = 0;

  arrow::Result<std::shared_ptr<Buffer>> Serialize(MemoryPool* pool = NULLPTR);
  static arrow::Result<UpdateResult> Deserialize(const Buffer& buffer);
};

}  // namespace sql
}  // namespace flight
}  // namespace arrow
