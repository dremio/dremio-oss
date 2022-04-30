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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.service.flight.TicketContent;
import com.dremio.service.flight.protector.CancellableUserResponseHandler;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

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

  @Before
  public void setup() {
    mockHandler = mock(CancellableUserResponseHandler.class);
    mockLocation = mock(Location.class);
  }

  @Test
  public void testGetSchemaCancelled() {
    // Arrange
    when(mockHandler.get()).thenThrow(CANCELLED);

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(mockHandler);

    // Act
    assertThatThrownBy(flightPreparedStatement::getSchema)
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(UserException.class)
      .hasMessage("CANCELLED: Client cancelled.");
  }

  @Test
  public void testGetSchemaUnknownError() {
    // Arrange
    when(mockHandler.get()).thenThrow(UNKNOWN);

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(mockHandler);

    // Act
    assertThatThrownBy(flightPreparedStatement::getSchema)
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(UserException.class)
      .hasMessage("UNKNOWN: Unknown cause.");
  }

  @Test
  public void testGetSchemaSuccessful() {
    // Arrange
    when(mockHandler.get()).thenReturn(response);

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(mockHandler);

    // Act
    final Schema actual = flightPreparedStatement.getSchema();

    // Assert
    assertEquals(schema, actual);
  }

  @Test
  public void testGetFlightInfoCancelled() {
    // Arrange
    when(mockHandler.get()).thenThrow(CANCELLED);

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(mockHandler);

    // Act
    assertThatThrownBy(() -> flightPreparedStatement.getFlightInfo(mockLocation))
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(UserException.class)
      .hasMessage("CANCELLED: Client cancelled.");
  }

  @Test
  public void testGetFlightInfoUnknownError() {
    // Arrange
    when(mockHandler.get()).thenThrow(UNKNOWN);

    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(mockHandler);

    // Act
    assertThatThrownBy(() -> flightPreparedStatement.getFlightInfo(mockLocation))
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(UserException.class)
      .hasMessage("UNKNOWN: Unknown cause.");
  }

  @Test
  public void testGetFlightInfoSuccessful() {
    // Arrange
    when(mockHandler.get()).thenReturn(response);
    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(mockHandler);

    final FlightSql.CommandPreparedStatementQuery command = FlightSql.CommandPreparedStatementQuery.newBuilder()
      .setPreparedStatementHandle(response.getPreparedStatement().toByteString())
      .build();
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    final Ticket ticket = new Ticket(Any.pack(command).toByteArray());
    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, mockLocation);

    final FlightInfo expected = new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);

    // Act
    final FlightInfo actual = flightPreparedStatement.getFlightInfo(mockLocation);

    // Assert
    assertEquals(expected, actual);
  }

  @Test
  public void testGetFlightInfoLegacySuccessful() {
    // Arrange
    when(mockHandler.get()).thenReturn(response);
    final FlightPreparedStatement flightPreparedStatement = new FlightPreparedStatement(mockHandler);

    final String dummyQuery = "select 1";
    final TicketContent.PreparedStatementTicket ticketContent = TicketContent.PreparedStatementTicket.newBuilder()
      .setHandle(response.getPreparedStatement().getServerHandle())
      .setQuery(dummyQuery)
      .build();
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(dummyQuery.getBytes(StandardCharsets.UTF_8));
    final Ticket ticket = new Ticket(ticketContent.toByteArray());
    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, mockLocation);

    final FlightInfo expected = new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);

    // Act
    final FlightInfo actual = flightPreparedStatement.getFlightInfoLegacy(mockLocation, flightDescriptor);

    // Assert
    assertEquals(expected, actual);
  }

  private static class TestException extends Exception {
    public TestException(String message) {
      super(message);
    }
  }
}
