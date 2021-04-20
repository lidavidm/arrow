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

#include "arrow/flight/sql_producer.h"

#include "arrow/flight/FlightSql.pb.h"
#include "arrow/flight/server.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace flight {
namespace sql {

namespace pb = arrow::flight::protocol;

class SqlProducerFlightServer : public FlightServerBase {
 public:
  explicit SqlProducerFlightServer(std::unique_ptr<FlightSqlBase> impl)
      : impl_(std::move(impl)) {}

  arrow::Result<pb::sql::Command> UnwrapCommand(const FlightDescriptor& request) {
    if (!request.is_command()) {
      return Status::Invalid("Must provide a CMD FlightDescriptor");
    }
    pb::sql::Command command;
    command.ParseFromString(request.cmd);
    return command;
  }

  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    ARROW_ASSIGN_OR_RAISE(auto command, UnwrapCommand(request));
    switch (command.command_case()) {
      case pb::sql::Command::CommandCase::kStatementQuery: {
        QueryStatement query;
        query.query = command.statement_query().query();
        ARROW_ASSIGN_OR_RAISE(*info, impl_->GetQueryStatementInfo(context, query));
        return Status::OK();
      }
      case pb::sql::Command::CommandCase::kStatementUpdate: {
        // TODO: do we want to enforce control/data plane distinction for DoPut?
        return Status::Invalid("Cannot execute update statement over GetFlightInfo");
      }
      default:
        return Status::NotImplemented("Not implemented: ", command.command_case());
    }
  }

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* stream) override {
    ARROW_ASSIGN_OR_RAISE(*stream, impl_->ExecuteQueryStatement(context, request));
    return Status::OK();
  }

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    // TODO: this class needs to take in a memory_pool for Serialize calls
    ARROW_ASSIGN_OR_RAISE(auto command, UnwrapCommand(reader->descriptor()));
    switch (command.command_case()) {
      case pb::sql::Command::CommandCase::kStatementQuery: {
        return Status::Invalid("Cannot execute query statement over DoPut");
      }
      case pb::sql::Command::CommandCase::kStatementUpdate: {
        UpdateStatement update;
        update.query = command.statement_update().query();
        ARROW_ASSIGN_OR_RAISE(auto result, impl_->ExecuteUpdateStatement(
                                               context, update, std::move(reader)));
        ARROW_ASSIGN_OR_RAISE(auto serialized_result, result.Serialize());
        RETURN_NOT_OK(writer->WriteMetadata(*serialized_result));
        return Status::OK();
      }
      default:
        return Status::NotImplemented("Not implemented: ", command.command_case());
    }
  }

  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    // TODO: move constants into protobuf
    if (action.type == "GetSqlInfo") {
      ARROW_ASSIGN_OR_RAISE(auto info, impl_->GetSqlInfo(context));
      std::vector<Result> results(1);
      ARROW_ASSIGN_OR_RAISE(results[0].body, info.Serialize());
      *result = std::unique_ptr<ResultStream>(new SimpleResultStream(std::move(results)));
      return Status::OK();
    }
    return Status::NotImplemented("Unknown action type: ", action.type);
  }

 private:
  std::unique_ptr<FlightSqlBase> impl_;
};

std::unique_ptr<FlightServerBase> MakeServer(std::unique_ptr<FlightSqlBase> server) {
  return internal::make_unique<SqlProducerFlightServer>(std::move(server));
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
