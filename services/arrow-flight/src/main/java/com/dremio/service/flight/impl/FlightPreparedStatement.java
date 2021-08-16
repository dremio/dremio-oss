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

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.proto.UserProtos;
import com.dremio.service.flight.TicketContent.PreparedStatementTicket;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * Container resulting from execution of a CREATE_PREPARED_STATEMENT job. Results from the job are
 * consumed, and then Flight objects are exposed.
 */
public class FlightPreparedStatement {

  private final FlightDescriptor flightDescriptor;
  private final String query;
  private final CreatePreparedStatementResponseHandler responseHandler;

  public FlightPreparedStatement(FlightDescriptor flightDescriptor, String query,
                                 CreatePreparedStatementResponseHandler responseHandler) {
    this.flightDescriptor = flightDescriptor;
    this.query = query;
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

    final PreparedStatementTicket preparedStatementTicketContent = PreparedStatementTicket.newBuilder()
      .setQuery(query)
      .setHandle(getServerHandle())
      .build();

    final Ticket ticket = new Ticket(preparedStatementTicketContent.toByteArray());

    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, location);
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
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

  public ActionCreatePreparedStatementResult createAction() {
    final UserProtos.CreatePreparedStatementArrowResp createPreparedStatementResp = responseHandler.get();
    final Schema schema = buildSchema(createPreparedStatementResp.getPreparedStatement().getArrowSchema());

    return ActionCreatePreparedStatementResult.newBuilder()
      .setDatasetSchema(ByteString.copyFrom(schema.toByteArray()))
      .setParameterSchema(ByteString.EMPTY)
      .setPreparedStatementHandle(getServerHandle().toByteString())
      .build();
  }

  public UserProtos.PreparedStatementHandle getServerHandle() {
    UserProtos.CreatePreparedStatementArrowResp createPreparedStatementResp = responseHandler.get();
    return createPreparedStatementResp.getPreparedStatement().getServerHandle();
  }

  private static Schema buildSchema(ByteString arrowSchema) {
    return Schema.deserialize(arrowSchema.asReadOnlyByteBuffer());
  }
}
