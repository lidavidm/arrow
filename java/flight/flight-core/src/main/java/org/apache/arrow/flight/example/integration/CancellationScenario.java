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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.memory.BufferAllocator;

/** Test cancelling an RPC call. */
class CancellationScenario implements Scenario {

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new NoOpFlightProducer() {
      @Override
      public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        // Don't ever call listener.onCompleted(); intentionally spin
        while (!context.isCancelled()) {
        }
      }
    };
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {

  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) {
    final FlightRuntimeException e = IntegrationAssertions.assertThrows(FlightRuntimeException.class, () -> {
      client.listFlights(new Criteria(new byte[0]), CallOptions.timeout(1, TimeUnit.SECONDS)).forEach(v -> {
      });
    });
    if (!FlightStatusCode.TIMED_OUT.equals(e.status().code())) {
      throw new AssertionError("Expected TIMED_OUT but found " + e.status().code(), e);
    }
  }
}
