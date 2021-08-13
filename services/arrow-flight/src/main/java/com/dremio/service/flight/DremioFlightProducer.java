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

package com.dremio.service.flight;

import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.impl.FlightPreparedStatement;
import com.dremio.service.flight.impl.FlightWorkManager;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A FlightProducer implementation which exposes Dremio's catalog and produces results from SQL queries.
 */
public class DremioFlightProducer implements FlightSqlProducer {

  private final FlightWorkManager flightWorkManager;
  private final Location location;
  private final DremioFlightSessionsManager sessionsManager;
  private final BufferAllocator allocator;

  public DremioFlightProducer(Location location, DremioFlightSessionsManager sessionsManager,
                              Provider<UserWorker> workerProvider, Provider<OptionManager> optionManagerProvider,
                              BufferAllocator allocator,
                              RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) {
    this.location = location;
    this.sessionsManager = sessionsManager;
    this.allocator = allocator;

    flightWorkManager = new FlightWorkManager(workerProvider, optionManagerProvider, runQueryResponseHandlerFactory);
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    if (isFlightSqlTicket(ticket)) {
      FlightSqlProducer.super.getStream(callContext, ticket, serverStreamListener);
      return;
    }

    try {
      final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
      final UserSession session = sessionsManager.getUserSession(callContext.peerIdentity(), headers);
      final TicketContent.PreparedStatementTicket preparedStatementTicket =
        TicketContent.PreparedStatementTicket.parseFrom(ticket.getBytes());

      flightWorkManager.runPreparedStatement(preparedStatementTicket, serverStreamListener, allocator, session);
    } catch (InvalidProtocolBufferException ex) {
      final RuntimeException error =
        CallStatus.INVALID_ARGUMENT.withCause(ex).withDescription("Invalid ticket used in getStream")
          .toRuntimeException();
      serverStreamListener.error(error);
      throw error;
    }
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listFlights is unimplemented").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
    if (isFlightSqlCommand(flightDescriptor)) {
      return FlightSqlProducer.super.getFlightInfo(callContext, flightDescriptor);
    }

    final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
    final UserSession session = sessionsManager.getUserSession(callContext.peerIdentity(), headers);
    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(flightDescriptor, callContext::isCancelled, session);
    return flightPreparedStatement.getFlightInfo(location);
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream,
                            StreamListener<PutResult> streamListener) {
    if (isFlightSqlCommand(flightStream.getDescriptor())) {
      return FlightSqlProducer.super.acceptPut(callContext, flightStream, streamListener);
    }

    throw CallStatus.UNIMPLEMENTED.withDescription("acceptPut is unimplemented").toRuntimeException();
  }

  @Override
  public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {
    if (isFlightSqlAction(action)) {
      FlightSqlProducer.super.doAction(callContext, action, streamListener);
      return;
    }

    throw CallStatus.UNIMPLEMENTED.withDescription("doAction is unimplemented").toRuntimeException();
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listActions is unimplemented").toRuntimeException();
  }

  @Override
  public void createPreparedStatement(
    FlightSql.ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("createPreparedStatement not supported.").toRuntimeException();
  }

  @Override
  public void closePreparedStatement(
    FlightSql.ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> listener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("closePreparedStatement not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoStatement(
    CommandStatementQuery commandStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("PreparedStatement not supported.").toRuntimeException();
  }

  @Override
  public SchemaResult getSchemaStatement(
    CommandStatementQuery commandStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
  }

  @Override
  public void getStreamStatement(CommandStatementQuery commandStatementQuery,
                                 CallContext callContext, Ticket ticket,
                                 ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
  }

  @Override
  public void getStreamPreparedStatement(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, Ticket ticket,
    ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("PreparedStatement not supported.").toRuntimeException();
  }

  @Override
  public Runnable acceptPutStatement(
    FlightSql.CommandStatementUpdate commandStatementUpdate,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
    FlightSql.CommandPreparedStatementUpdate commandPreparedStatementUpdate,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("PreparedStatement with parameter binding not supported.")
      .toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("PreparedStatement with parameter binding not supported.")
      .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo commandGetSqlInfo,
                                         CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetSqlInfo not supported.").toRuntimeException();
  }

  @Override
  public void getStreamSqlInfo(CommandGetSqlInfo commandGetSqlInfo,
                               CallContext callContext, Ticket ticket,
                               ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetSqlInfo not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(
    CommandGetCatalogs commandGetCatalogs, CallContext callContext,
    FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetCatalogs not supported.").toRuntimeException();
  }

  @Override
  public void getStreamCatalogs(CallContext callContext, Ticket ticket,
                                ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetCatalogs not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSchemas(CommandGetSchemas commandGetSchemas,
                                         CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetSchemas not supported.").toRuntimeException();
  }

  @Override
  public void getStreamSchemas(CommandGetSchemas commandGetSchemas,
                               CallContext callContext, Ticket ticket,
                               ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetSchemas not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTables(CommandGetTables commandGetTables,
                                        CallContext callContext,
                                        FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetTables not supported.").toRuntimeException();
  }

  @Override
  public void getStreamTables(CommandGetTables commandGetTables,
                              CallContext callContext, Ticket ticket,
                              ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetTables not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(
    CommandGetTableTypes commandGetTableTypes, CallContext callContext,
    FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetTableTypes not supported.").toRuntimeException();
  }

  @Override
  public void getStreamTableTypes(CallContext callContext, Ticket ticket,
                                  ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetTableTypes not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(
    CommandGetPrimaryKeys commandGetPrimaryKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetPrimaryKeys not supported.").toRuntimeException();
  }

  @Override
  public void getStreamPrimaryKeys(CommandGetPrimaryKeys commandGetPrimaryKeys,
                                   CallContext callContext, Ticket ticket,
                                   ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetPrimaryKeys not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetExportedKeys not supported.").toRuntimeException();
  }

  @Override
  public void getStreamExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext, Ticket ticket,
    ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetExportedKeys not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetImportedKeys not supported.").toRuntimeException();
  }

  @Override
  public void getStreamImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext, Ticket ticket,
    ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetImportedKeys not supported.").toRuntimeException();
  }

  @Override
  public void close() throws Exception {

  }

  /**
   * Helper method to retrieve CallHeaders from the CallContext.
   *
   * @param callContext the CallContext to retrieve headers from.
   * @return CallHeaders retrieved from provided CallContext.
   */
  private CallHeaders retrieveHeadersFromCallContext(CallContext callContext) {
    return callContext.getMiddleware(FlightConstants.HEADER_KEY).headers();
  }

  private boolean isFlightSqlCommand(Any command) {
    return command.is(CommandStatementQuery.class) || command.is(CommandPreparedStatementQuery.class) ||
      command.is(CommandGetCatalogs.class) || command.is(CommandGetSchemas.class) ||
      command.is(CommandGetTables.class) || command.is(CommandGetTableTypes.class) ||
      command.is(CommandGetSqlInfo.class) || command.is(CommandGetPrimaryKeys.class) ||
      command.is(CommandGetExportedKeys.class) || command.is(CommandGetImportedKeys.class);
  }

  private boolean isFlightSqlCommand(byte[] bytes) {
    try {
      Any command = Any.parseFrom(bytes);
      return isFlightSqlCommand(command);
    } catch (InvalidProtocolBufferException e) {
      return false;
    }
  }

  private boolean isFlightSqlCommand(FlightDescriptor flightDescriptor) {
    return isFlightSqlCommand(flightDescriptor.getCommand());
  }

  private boolean isFlightSqlTicket(Ticket ticket) {
    // The byte array on ticket is a serialized FlightSqlCommand
    return isFlightSqlCommand(ticket.getBytes());
  }

  private boolean isFlightSqlAction(Action action) {
    String actionType = action.getType();
    return FlightSqlUtils.FLIGHT_SQL_ACTIONS.stream().anyMatch(action2 -> action2.getType().equals(actionType));
  }
}
