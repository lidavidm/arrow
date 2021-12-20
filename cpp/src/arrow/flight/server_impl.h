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

namespace arrow {

class Status;

namespace internal {
class Uri;
}

namespace flight {

class FlightServerBase;
class FlightServerOptions;

namespace internal {

class ServerImpl {
 public:
  /// Initialize the server.
  virtual Status Init(const FlightServerOptions& options,
                      const arrow::internal::Uri& location, FlightServerBase* server) = 0;
  /// Shutdown the server. Once this returns, the server is no longer listening.
  virtual Status Shutdown() = 0;
  /// Wait for the server to shutdown. Once this returns, the server is no longer
  /// listening.
  virtual Status Wait() = 0;

  /// Get the port the server is listening on, or -1 if not listening or not applicable.
  virtual int port() const = 0;
  // TODO: should probably be an optional Location or something
};

}  // namespace internal
}  // namespace flight
}  // namespace arrow
