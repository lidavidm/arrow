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

#include "arrow/flight/sql_client.h"

#include "arrow/flight/FlightSql.pb.h"

namespace arrow {
namespace flight {
namespace sql {

arrow::Result<SqlInfo> GetSqlInfo(FlightClient* client,
                                  const FlightCallOptions& options) {
  Action action{kGetSqlInfoAction, /*body=*/nullptr};
  std::unique_ptr<ResultStream> results;
  RETURN_NOT_OK(client->DoAction(options, action, &results));
  std::unique_ptr<Result> raw_info;
  RETURN_NOT_OK(results->Next(&raw_info));
  if (!raw_info) return Status::Invalid("Server did not return a response");
  if (!raw_info->body) return Status::Invalid("Server returned an empty response");
  // Drain extra results (though they should not be sent)
  std::unique_ptr<Result> ignored_info;
  do {
    RETURN_NOT_OK(results->Next(&ignored_info));
  } while (ignored_info);
  return SqlInfo::Deserialize(*raw_info->body);
}

/// Execute a non-bulk update query.
arrow::Result<UpdateResult> ExecuteSimpleUpdate(flight::FlightClient* client,
                                                const FlightCallOptions& options,
                                                const UpdateStatement& query) {
  auto schema = arrow::schema({});
  std::unique_ptr<FlightStreamWriter> stream;
  std::unique_ptr<FlightMetadataReader> reader;
  RETURN_NOT_OK(client->DoPut(options, query.ToDescriptor(), schema, &stream, &reader));
  RETURN_NOT_OK(stream->DoneWriting());
  std::shared_ptr<Buffer> result;
  while (true) {
    std::shared_ptr<Buffer> buf;
    RETURN_NOT_OK(reader->ReadMetadata(&buf));
    if (!buf) break;
    result = buf;
  }
  RETURN_NOT_OK(stream->Close());
  if (!result) return Status::Invalid("Server did not send a response");
  return UpdateResult::Deserialize(*result);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
