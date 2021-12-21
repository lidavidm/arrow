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

#include "arrow/flight/types.h"
#include "arrow/flight/visibility.h"
#include "arrow/type_fwd.h"

namespace arrow {

namespace internal {
class Uri;
}

namespace flight {

class FlightClientOptions;
class FlightServerBase;
class FlightServerOptions;

namespace internal {

/// An implementation of a Flight client for a particular transport.
class ARROW_FLIGHT_EXPORT ClientTransportImpl {
 public:
  virtual ~ClientTransportImpl() = default;

  /// Initialize the client.
  virtual Status Init(const FlightClientOptions& options,
                      const arrow::internal::Uri& location) = 0;
  /// Close the client. Once this returns, the client is no longer usable.
  virtual Status Close() = 0;

  // TODO:
};

/// An implementation of a Flight server for a particular transport.
class ARROW_FLIGHT_EXPORT ServerTransportImpl {
 public:
  virtual ~ServerTransportImpl() = default;

  /// Initialize the server.
  virtual Status Init(const FlightServerOptions& options,
                      const arrow::internal::Uri& location, FlightServerBase* server) = 0;
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
