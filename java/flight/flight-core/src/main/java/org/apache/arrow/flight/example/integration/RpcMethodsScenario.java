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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Test that the RPC methods not covered by regular integration testing work.
 */
class RpcMethodsScenario implements Scenario {

  private final byte[] expectedCriteria = "criteria".getBytes(StandardCharsets.UTF_8);

  private final ActionType actionType1 = new ActionType("action1", "Long description");
  private final ActionType actionType2 = new ActionType("action2", "Long description");

  private final Schema schema = new Schema(Arrays.asList(
      new Field("timestamp", new FieldType(false, new ArrowType.Timestamp(
          org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null), null), Collections.emptyList()),
      Field.nullable("random", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
      Field.nullable("text", new ArrowType.Utf8())
  ));

  private final FlightDescriptor descriptor1 = FlightDescriptor.command(new byte[]{1, 1, 2, 3, 5});
  private final FlightDescriptor descriptor2 = FlightDescriptor.path("foo", "bar");
  private final Ticket ticket = new Ticket("foobar".getBytes(StandardCharsets.UTF_8));
  private final FlightInfo flightInfo = new FlightInfo(schema, descriptor1,
      Collections.singletonList(new FlightEndpoint(ticket, Location.forGrpcInsecure("localhost", 9090))), -1, -1);

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) {
    return new NoOpFlightProducer() {

      @Override
      public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        if (!Arrays.equals(criteria.getExpression(), expectedCriteria)) {
          listener.onError(CallStatus.INVALID_ARGUMENT.toRuntimeException());
          return;
        }
        listener.onNext(flightInfo);
        listener.onNext(flightInfo);
        listener.onCompleted();
      }

      @Override
      public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
        return new SchemaResult(schema);
      }

      @Override
      public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        // Test serialize/deserialize across languages
        listener.onNext(new Result(descriptor1.serialize().array()));
        listener.onNext(new Result(descriptor2.serialize().array()));
        listener.onNext(new Result(flightInfo.serialize().array()));
        listener.onNext(new Result(ticket.serialize().array()));
        listener.onCompleted();
      }

      @Override
      public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onNext(actionType1);
        listener.onNext(actionType2);
        listener.onCompleted();
      }
    };
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception {
    final Schema result = client.getSchema(FlightDescriptor.command(new byte[0])).getSchema();
    IntegrationAssertions.assertEquals(schema, result);

    // Test cross-language serialize/deserialize
    final Iterator<Result> actionResult = client.doAction(new Action(""));
    final FlightDescriptor returnedDescriptor1 = FlightDescriptor
        .deserialize(ByteBuffer.wrap(actionResult.next().getBody()));
    IntegrationAssertions.assertEquals(descriptor1, returnedDescriptor1);
    final FlightDescriptor returnedDescriptor2 = FlightDescriptor
        .deserialize(ByteBuffer.wrap(actionResult.next().getBody()));
    IntegrationAssertions.assertEquals(descriptor2, returnedDescriptor2);
    final FlightInfo returnedInfo = FlightInfo.deserialize(ByteBuffer.wrap(actionResult.next().getBody()));
    IntegrationAssertions.assertEquals(flightInfo, returnedInfo);
    final Ticket returnedTicket = Ticket.deserialize(ByteBuffer.wrap(actionResult.next().getBody()));
    IntegrationAssertions.assertEquals(ticket, returnedTicket);
    IntegrationAssertions.assertFalse("The action should have no more results.", actionResult.hasNext());

    final Iterator<FlightInfo> flights = client.listFlights(new Criteria(expectedCriteria)).iterator();
    IntegrationAssertions.assertEquals(flightInfo, flights.next());
    IntegrationAssertions.assertEquals(flightInfo, flights.next());
    IntegrationAssertions.assertFalse("ListFlights should have no more results.", flights.hasNext());

    final Iterator<ActionType> actions = client.listActions().iterator();
    IntegrationAssertions.assertEquals(actionType1, actions.next());
    IntegrationAssertions.assertEquals(actionType2, actions.next());
    IntegrationAssertions.assertFalse("ListActions should have no more results.", actions.hasNext());
  }
}
