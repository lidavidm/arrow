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

#include "arrow/flight/transport_impl.h"

#include <unordered_map>

#include "arrow/flight/server.h"
#include "arrow/flight/types.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {
namespace internal {

Status FlightServiceImpl::DoGet(const ServerCallContext& context, const Ticket& ticket,
                                TransportDataStream* stream) {
  std::unique_ptr<FlightDataStream> data_stream;
  RETURN_NOT_OK(service_->DoGet(context, ticket, &data_stream));

  if (!data_stream) return Status::KeyError("No data in this flight");

  // Write the schema as the first message in the stream
  FlightPayload schema_payload;
  RETURN_NOT_OK(data_stream->GetSchemaPayload(&schema_payload));
  auto status = stream->Write(schema_payload);
  // Connection terminated
  if (status.IsIOError()) return Status::OK();
  RETURN_NOT_OK(status);

  // Consume data stream and write out payloads
  while (true) {
    FlightPayload payload;
    RETURN_NOT_OK(data_stream->Next(&payload));
    // End of stream
    if (payload.ipc_message.metadata == nullptr) break;
    auto status = stream->Write(payload);
    // TODO: document why IOError is ignored here (how is it reported?)
    if (status.IsIOError()) return Status::OK();
    RETURN_NOT_OK(status);
  }
  RETURN_NOT_OK(stream->WritesDone());
  return Status::OK();
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
