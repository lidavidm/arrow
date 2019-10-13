/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.example.integration;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Test that all error codes are supported and distinguishable.
 */
class ErrorCodesScenario implements Scenario {

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) {
    return new NoOpFlightProducer() {
      @Override
      public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        switch (action.getType()) {
          case "UNKNOWN":
            listener.onError(CallStatus.UNKNOWN.withDescription("Integration test").toRuntimeException());
            break;
          case "INTERNAL":
            listener.onError(CallStatus.INTERNAL.withDescription("Integration test").toRuntimeException());
            break;
          case "INVALID_ARGUMENT":
            listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("Integration test").toRuntimeException());
            break;
          case "TIMED_OUT":
            listener.onError(CallStatus.TIMED_OUT.withDescription("Integration test").toRuntimeException());
            break;
          case "NOT_FOUND":
            listener.onError(CallStatus.NOT_FOUND.withDescription("Integration test").toRuntimeException());
            break;
          case "ALREADY_EXISTS":
            listener.onError(CallStatus.ALREADY_EXISTS.withDescription("Integration test").toRuntimeException());
            break;
          case "CANCELLED":
            listener.onError(CallStatus.CANCELLED.withDescription("Integration test").toRuntimeException());
            break;
          case "UNAUTHENTICATED":
            listener.onError(CallStatus.UNAUTHENTICATED.withDescription("Integration test").toRuntimeException());
            break;
          case "UNAUTHORIZED":
            listener.onError(CallStatus.UNAUTHORIZED.withDescription("Integration test").toRuntimeException());
            break;
          case "UNIMPLEMENTED":
            listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Integration test").toRuntimeException());
            break;
          case "UNAVAILABLE":
            listener.onError(CallStatus.UNAVAILABLE.withDescription("Integration test").toRuntimeException());
            break;
          default:
            listener.onCompleted();
            break;
        }
      }
    };
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) {
    for (final FlightStatusCode code : FlightStatusCode.values()) {
      if (code.equals(FlightStatusCode.OK)) {
        continue;
      }
      final FlightRuntimeException e = IntegrationAssertions.assertThrows(FlightRuntimeException.class,
          () -> client.doAction(new Action(code.toString())).next());
      if (!e.status().code().equals(code)) {
        throw new AssertionError("Expected status code: " + code + " but got: " + e.status());
      }
    }
  }
}
