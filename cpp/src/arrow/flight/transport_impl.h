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

// Experimental, internal interface for implementing alternate transports in Flight.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/flight/types.h"
#include "arrow/flight/visibility.h"
#include "arrow/type_fwd.h"

namespace arrow {

namespace internal {
class Uri;
}

namespace flight {

// TODO: type_fwd.h
class Action;
class ActionType;
class ClientAuthHandler;
class FlightCallOptions;
class FlightClientOptions;
class FlightInfo;
class FlightListing;
class FlightMetadataReader;
class FlightMetadataWriter;
class FlightServerBase;
class FlightServerOptions;
class FlightStreamReader;
class FlightStreamWriter;
class Location;
class ResultStream;
class SchemaResult;
class ServerCallContext;

namespace internal {

/// An implementation of a Flight client for a particular transport.
class ARROW_FLIGHT_EXPORT ClientTransportImpl {
 public:
  virtual ~ClientTransportImpl() = default;

  /// Initialize the client.
  virtual Status Init(const FlightClientOptions& options, const Location& location,
                      const arrow::internal::Uri& uri) = 0;
  /// Close the client. Once this returns, the client is no longer usable.
  virtual Status Close() = 0;

  virtual Status Authenticate(const FlightCallOptions& options,
                              std::unique_ptr<ClientAuthHandler> auth_handler) {
    return Status::NotImplemented("NYI");
  }
  virtual arrow::Result<std::pair<std::string, std::string>> AuthenticateBasicToken(
      const FlightCallOptions& options, const std::string& username,
      const std::string& password) {
    return Status::NotImplemented("NYI");
  }
  virtual Status DoAction(const FlightCallOptions& options, const Action& action,
                          std::unique_ptr<ResultStream>* results) {
    return Status::NotImplemented("NYI");
  }
  virtual Status ListActions(const FlightCallOptions& options,
                             std::vector<ActionType>* actions) {
    return Status::NotImplemented("NYI");
  }
  virtual Status GetFlightInfo(const FlightCallOptions& options,
                               const FlightDescriptor& descriptor,
                               std::unique_ptr<FlightInfo>* info) {
    return Status::NotImplemented("NYI");
  }
  virtual Status GetSchema(const FlightCallOptions& options,
                           const FlightDescriptor& descriptor,
                           std::unique_ptr<SchemaResult>* schema_result) {
    return Status::NotImplemented("NYI");
  }
  virtual Status ListFlights(const FlightCallOptions& options, const Criteria& criteria,
                             std::unique_ptr<FlightListing>* listing) {
    return Status::NotImplemented("NYI");
  }
  virtual Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
                       std::unique_ptr<FlightStreamReader>* stream) {
    return Status::NotImplemented("NYI");
  }
  virtual Status DoPut(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       const std::shared_ptr<Schema>& schema,
                       std::unique_ptr<FlightStreamWriter>* stream,
                       std::unique_ptr<FlightMetadataReader>* reader) {
    return Status::NotImplemented("NYI");
  }
  virtual Status DoExchange(const FlightCallOptions& options,
                            const FlightDescriptor& descriptor,
                            std::unique_ptr<FlightStreamWriter>* writer,
                            std::unique_ptr<FlightStreamReader>* reader) {
    return Status::NotImplemented("NYI");
  }
};

class ARROW_FLIGHT_EXPORT ServerDataStream {
 public:
  virtual ~ServerDataStream() = default;
  // virtual Status Read(FlightData* data) = 0;
  virtual Status Write(const FlightPayload& payload) = 0;
  virtual Status WritesDone() = 0;
};

/// The implementation of the Flight service. Transport
/// implementations should implement the necessary interfaces and call
/// methods of this service.
class ARROW_FLIGHT_EXPORT FlightServiceImpl {
 public:
  explicit FlightServiceImpl(FlightServerBase* base) : service_(base) {}
  Status DoGet(const ServerCallContext& context, const Ticket& request,
               ServerDataStream* stream);
  FlightServerBase* base() const { return service_; }

 private:
  FlightServerBase* service_;
};

/// An implementation of a Flight server for a particular transport.
class ARROW_FLIGHT_EXPORT ServerTransportImpl {
 public:
  virtual ~ServerTransportImpl() = default;

  /// Initialize the server.
  virtual Status Init(const FlightServerOptions& options, const arrow::internal::Uri& uri,
                      FlightServiceImpl* service) = 0;
  /// Shutdown the server. Once this returns, the server is no longer listening.
  virtual Status Shutdown() = 0;
  /// Wait for the server to shutdown. Once this returns, the server is no longer
  /// listening.
  virtual Status Wait() = 0;
  /// Get the address the server is listening on, else an empty Location.
  virtual Location location() const = 0;
};

/// A registry of transport implementations.
class ARROW_FLIGHT_EXPORT TransportImplRegistry {
 public:
  using ClientFactory =
      std::function<arrow::Result<std::unique_ptr<ClientTransportImpl>>()>;
  using ServerFactory =
      std::function<arrow::Result<std::unique_ptr<ServerTransportImpl>>()>;

  TransportImplRegistry();

  arrow::Result<std::unique_ptr<ClientTransportImpl>> MakeClientImpl(
      const std::string& scheme);
  arrow::Result<std::unique_ptr<ServerTransportImpl>> MakeServerImpl(
      const std::string& scheme);

  Status RegisterClient(const std::string& scheme, ClientFactory factory);
  Status RegisterServer(const std::string& scheme, ServerFactory factory);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

ARROW_FLIGHT_EXPORT
TransportImplRegistry* GetDefaultTransportImplRegistry();

}  // namespace internal
}  // namespace flight
}  // namespace arrow
