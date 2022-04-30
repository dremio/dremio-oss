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

import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.proto.UserProtos;
import com.dremio.service.flight.TicketContent;
import com.dremio.service.flight.protector.CancellableUserResponseHandler;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

/**
 * Container resulting from execution of a CREATE_PREPARED_STATEMENT job. Results from the job are
 * consumed, and then Flight objects are exposed.
 */
public class FlightPreparedStatement {

  private final CancellableUserResponseHandler<UserProtos.CreatePreparedStatementArrowResp> responseHandler;

  public FlightPreparedStatement(CancellableUserResponseHandler<UserProtos.CreatePreparedStatementArrowResp> responseHandler) {
    this.responseHandler = responseHandler;
  }

  /**
   * Returns a FlightInfo for the PreparedStatement which a given instance manages.
   *
   * @param location The server location.
   * @return The FlightInfo.
   */
  public FlightInfo getFlightInfo(Location location) {
    final UserProtos.CreatePreparedStatementArrowResp createPreparedStatementResp = responseHandler.get();
    final Schema schema = buildSchema(createPreparedStatementResp.getPreparedStatement().getArrowSchema());

    final FlightSql.CommandPreparedStatementQuery command = FlightSql.CommandPreparedStatementQuery.newBuilder()
      .setPreparedStatementHandle(createPreparedStatementResp.getPreparedStatement().toByteString())
      .build();
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    final Ticket ticket = new Ticket(Any.pack(command).toByteArray());

    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, location);
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
  }


  /**
   * Returns a FlightInfo for the PreparedStatement which a given instance manages.
   * This method is for returning a lightweight ticket for non-Flight-SQL queries.
   *
   * @param location The server location.
   * @return The FlightInfo.
   */
  public FlightInfo getFlightInfoLegacy(Location location, FlightDescriptor flightDescriptor) {
    final UserProtos.CreatePreparedStatementArrowResp createPreparedStatementResp = responseHandler.get();
    final Schema schema = buildSchema(createPreparedStatementResp.getPreparedStatement().getArrowSchema());

    final TicketContent.PreparedStatementTicket preparedStatementTicketContent = TicketContent.PreparedStatementTicket.newBuilder()
      .setQuery(FlightWorkManager.getQuery(flightDescriptor))
      .setHandle(createPreparedStatementResp.getPreparedStatement().getServerHandle())
      .build();

    final Ticket ticket = new Ticket(preparedStatementTicketContent.toByteArray());

    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, location);
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
  }

  /**
   * Create an action to create a prepared statement.
   *
   * @return a ActionCreatePreparedStatementResult;
   */
  public ActionCreatePreparedStatementResult createAction() {
    final UserProtos.CreatePreparedStatementArrowResp createPreparedStatementResp = responseHandler.get();
    final Schema schema = buildSchema(createPreparedStatementResp.getPreparedStatement().getArrowSchema());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize schema", e);
    }

    return ActionCreatePreparedStatementResult.newBuilder()
      .setDatasetSchema(ByteString.copyFrom(ByteBuffer.wrap(outputStream.toByteArray())))
      .setParameterSchema(ByteString.EMPTY)
      .setPreparedStatementHandle(createPreparedStatementResp.getPreparedStatement().toByteString())
      .build();
  }


  /**
   * Returns the schema.
   *
   * @return The Schema.
   */
  public Schema getSchema() {
    final UserProtos.CreatePreparedStatementArrowResp resp = responseHandler.get();
    return buildSchema(resp.getPreparedStatement().getArrowSchema());
  }

  public UserProtos.PreparedStatementHandle getServerHandle() {
    UserProtos.CreatePreparedStatementArrowResp createPreparedStatementResp = responseHandler.get();
    return createPreparedStatementResp.getPreparedStatement().getServerHandle();
  }

  public static Schema buildSchema(ByteString arrowSchema) {
    return Schema.deserialize(arrowSchema.asReadOnlyByteBuffer());
  }
}
