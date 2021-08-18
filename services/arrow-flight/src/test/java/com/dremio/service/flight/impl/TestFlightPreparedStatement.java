/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.flight.impl;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.service.flight.TicketContent;
import com.dremio.service.flight.protector.CancellableUserResponseHandler;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import io.grpc.Status;

/**
 * Tests for FlightPreparedStatement.
 */
public class TestFlightPreparedStatement {

  private static final TestException UNKNOWN_CAUSE = new TestException("cause");
  private static final Exception UNKNOWN = Status.UNKNOWN
    .withCause(UserException.parseError(UNKNOWN_CAUSE).buildSilently())
    .withDescription("Unknown cause.")
    .asRuntimeException();

  private static final TestException CANCELLED_CAUSE = new TestException("cancelled");
  private static final Exception CANCELLED = Status.CANCELLED
    .withCause(UserException.parseError(CANCELLED_CAUSE).buildSilently())
    .withDescription("Client cancelled.")
    .asRuntimeException();

  private static final String command = "command";
  private static final FlightDescriptor flightDescriptor = FlightDescriptor.command(command.getBytes(StandardCharsets.UTF_8));

  private static final Schema schema = new Schema(Collections.singletonList(Field.nullable("test1", ArrowType.Bool.INSTANCE)));
  private static final PreparedStatementHandle preparedStatementHandle = PreparedStatementHandle
    .newBuilder()
    .setServerInfo(ByteString.copyFrom(new byte[]{1, 2, 3}))
    .build();

  private static final UserProtos.CreatePreparedStatementArrowResp response = UserProtos
    .CreatePreparedStatementArrowResp
    .newBuilder()
    .setPreparedStatement(UserProtos.PreparedStatementArrow
      .newBuilder()
      .setArrowSchema(ByteString.copyFrom(schema.toByteArray()))
      .setServerHandle(preparedStatementHandle)
      .build())
    .build();

  private static CancellableUserResponseHandler<UserProtos.CreatePreparedStatementArrowResp> mockHandler;
  private static Location mockLocation;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    mockHandler = mock(CancellableUserResponseHandler.class);
    mockLocation = mock(Location.class);
  }

  @Test
  public void testGetSchemaCancelled() {
    // Arrange
    when(mockHandler.get()).thenThrow(CANCELLED);
    thrown.expectMessage("CANCELLED: Client cancelled.");
    thrown.expect(io.grpc.StatusRuntimeException.class);
    thrown.expectCause(isA(UserException.class));

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(flightDescriptor, command, mockHandler);

    // Act
    flightPreparedStatement.getSchema();
  }

  @Test
  public void testGetSchemaUnknownError() {
    // Arrange
    when(mockHandler.get()).thenThrow(UNKNOWN);
    thrown.expectMessage("UNKNOWN: Unknown cause.");
    thrown.expect(io.grpc.StatusRuntimeException.class);
    thrown.expectCause(isA(UserException.class));

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(flightDescriptor, command, mockHandler);

    // Act
    flightPreparedStatement.getSchema();
  }

  @Test
  public void testGetSchemaSuccessful() {
    // Arrange
    when(mockHandler.get()).thenReturn(response);

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(flightDescriptor, command, mockHandler);

    // Act
    final Schema actual = flightPreparedStatement.getSchema();

    // Assert
    assertEquals(schema, actual);
  }

  @Test
  public void testGetFlightInfoCancelled() {
    // Arrange
    when(mockHandler.get()).thenThrow(CANCELLED);
    thrown.expectMessage("CANCELLED: Client cancelled.");
    thrown.expect(io.grpc.StatusRuntimeException.class);
    thrown.expectCause(isA(UserException.class));

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(flightDescriptor, command, mockHandler);

    // Act
    flightPreparedStatement.getFlightInfo(mockLocation);
  }

  @Test
  public void testGetFlightInfoUnknownError() {
    // Arrange
    when(mockHandler.get()).thenThrow(UNKNOWN);
    thrown.expectMessage("UNKNOWN: Unknown cause.");
    thrown.expect(io.grpc.StatusRuntimeException.class);
    thrown.expectCause(isA(UserException.class));

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(flightDescriptor, command, mockHandler);

    // Act
    flightPreparedStatement.getFlightInfo(mockLocation);
  }

  @Test
  public void testGetFlightInfoSuccessful() {
    // Arrange
    when(mockHandler.get()).thenReturn(response);
    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(flightDescriptor, command, mockHandler);

    final TicketContent.PreparedStatementTicket preparedStatementTicketContent = TicketContent.PreparedStatementTicket.newBuilder()
      .setQuery(command)
      .setHandle(preparedStatementHandle)
      .build();

    final Ticket ticket = new Ticket(preparedStatementTicketContent.toByteArray());
    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, mockLocation);
    final FlightInfo expected = new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);

    // Act
    final FlightInfo actual = flightPreparedStatement.getFlightInfo(mockLocation);

    // Assert
    assertEquals(expected, actual);
  }

  private static class TestException extends Exception {
    public TestException(String message) {
      super(message);
    }
  }
}
