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

import static com.dremio.service.flight.utils.DremioFlightSqlInfoUtils.getNewSqlInfoBuilder;
import static com.google.protobuf.Any.pack;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;

import java.util.Collections;

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.impl.FlightPreparedStatement;
import com.dremio.service.flight.impl.FlightWorkManager;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.dremio.service.usersessions.UserSessionService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * A FlightProducer implementation which exposes Dremio's catalog and produces results from SQL queries.
 */
public class DremioFlightProducer implements FlightSqlProducer {
  private static final Logger logger = LoggerFactory.getLogger(DremioFlightProducer.class);

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

    getStreamLegacy(callContext, ticket, serverStreamListener);
  }

  private void getStreamLegacy(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {

    final TicketContent.PreparedStatementTicket preparedStatementTicket;
    try {
      preparedStatementTicket = TicketContent.PreparedStatementTicket.parseFrom(ticket.getBytes());
    } catch (InvalidProtocolBufferException ex) {
      final RuntimeException error =
        CallStatus.INVALID_ARGUMENT.withCause(ex).withDescription("Invalid ticket used in getStream")
          .toRuntimeException();
      serverStreamListener.error(error);
      throw error;
    }

    UserProtos.PreparedStatementHandle preparedStatementHandle = preparedStatementTicket.getHandle();

    runPreparedStatement(callContext, serverStreamListener, preparedStatementHandle);
  }

  @Override
  public void getStreamPreparedStatement(CommandPreparedStatementQuery commandPreparedStatementQuery,
                                         CallContext callContext, ServerStreamListener serverStreamListener) {
    UserProtos.PreparedStatementArrow preparedStatement;
    try {
      preparedStatement =
        UserProtos.PreparedStatementArrow.parseFrom(commandPreparedStatementQuery.getPreparedStatementHandle());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }

    runPreparedStatement(callContext, serverStreamListener, preparedStatement.getServerHandle());
  }

  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    if (isFlightSqlCommand(descriptor)) {
      return FlightSqlProducer.super.getSchema(context, descriptor);
    }

    return getSchemaLegacy(context, descriptor);
  }

  private SchemaResult getSchemaLegacy(CallContext context, FlightDescriptor descriptor) {
    FlightInfo info = this.getFlightInfo(context, descriptor);
    return new SchemaResult(info.getSchema());
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

    return getFlightInfoLegacy(callContext, flightDescriptor);
  }

  private FlightInfo getFlightInfoLegacy(CallContext callContext, FlightDescriptor flightDescriptor) {
    final UserSession session = getUserSessionData(callContext).getSession();

    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(flightDescriptor, callContext::isCancelled, session);

    return flightPreparedStatement.getFlightInfoLegacy(location, flightDescriptor);
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final UserProtos.PreparedStatementArrow preparedStatement;

    try {
      preparedStatement =
        UserProtos.PreparedStatementArrow.parseFrom(commandPreparedStatementQuery.getPreparedStatementHandle());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }

    final Schema schema = FlightPreparedStatement.buildSchema(preparedStatement.getArrowSchema());
    return getFlightInfoForFlightSqlCommands(commandPreparedStatementQuery, flightDescriptor, schema);
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
  public void createPreparedStatement(ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
                                      CallContext callContext, StreamListener<Result> streamListener) {
    final String query = actionCreatePreparedStatementRequest.getQuery();

    final UserSession session = getUserSessionData(callContext).getSession();
    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(query, callContext::isCancelled, session);

    final ActionCreatePreparedStatementResult action = flightPreparedStatement.createAction();

    streamListener.onNext(new Result(pack(action).toByteArray()));
    streamListener.onCompleted();

  }

  @Override
  public void closePreparedStatement(ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
                                     CallContext callContext, StreamListener<Result> listener) {
    try {
      // Do nothing other than validate the message.
      UserProtos.PreparedStatementHandle.parseFrom(actionClosePreparedStatementRequest.getPreparedStatementHandle());

    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }
    listener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(CommandStatementQuery commandStatementQuery, CallContext callContext,
                                           FlightDescriptor flightDescriptor) {
    final UserSession session = getUserSessionData(callContext).getSession();

    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(commandStatementQuery.getQuery(), callContext::isCancelled, session);

    final TicketStatementQuery ticket =
      TicketStatementQuery.newBuilder()
        .setStatementHandle(flightPreparedStatement.getServerHandle().toByteString())
        .build();

    final Schema schema = flightPreparedStatement.getSchema();
    return getFlightInfoForFlightSqlCommands(ticket, flightDescriptor, schema);
  }

  @Override
  public SchemaResult getSchemaStatement(CommandStatementQuery commandStatementQuery, CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    final FlightInfo info = this.getFlightInfo(callContext, flightDescriptor);
    return new SchemaResult(info.getSchema());
  }

  @Override
  public void getStreamStatement(TicketStatementQuery ticketStatementQuery, CallContext callContext,
                                 ServerStreamListener serverStreamListener) {
    try {
      final UserProtos.PreparedStatementHandle preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(ticketStatementQuery.getStatementHandle());

      runPreparedStatement(callContext, serverStreamListener, preparedStatementHandle);
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INTERNAL.toRuntimeException();
    }
  }

  @Override
  public Runnable acceptPutStatement(CommandStatementUpdate commandStatementUpdate, CallContext callContext,
                                     FlightStream flightStream, StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate commandPreparedStatementUpdate,
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
  public FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo commandGetSqlInfo, CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    return new FlightInfo(
      Schemas.GET_SQL_INFO_SCHEMA,
      flightDescriptor,
      Collections.singletonList(new FlightEndpoint(new Ticket(Any.pack(commandGetSqlInfo).toByteArray()))),
      -1, -1);
  }

  @Override
  public void getStreamSqlInfo(CommandGetSqlInfo commandGetSqlInfo, CallContext callContext,
                               ServerStreamListener serverStreamListener) {
    final UserSession userSession = getUserSessionData(callContext).getSession();
    final UserProtos.ServerMeta serverMeta = flightWorkManager.getServerMeta(callContext::isCancelled, userSession);

    getNewSqlInfoBuilder(serverMeta).send(commandGetSqlInfo.getInfoList(), serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(CommandGetCatalogs commandGetCatalogs, CallContext callContext,
                                          FlightDescriptor flightDescriptor) {
    final Schema catalogsSchema = Schemas.GET_CATALOGS_SCHEMA;
    return getFlightInfoForFlightSqlCommands(commandGetCatalogs, flightDescriptor, catalogsSchema);
  }

  @Override
  public void getStreamCatalogs(CallContext callContext, ServerStreamListener serverStreamListener) {
    try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, allocator)) {
      serverStreamListener.start(vectorSchemaRoot);
      vectorSchemaRoot.setRowCount(0);
      serverStreamListener.putNext();
      serverStreamListener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoSchemas(CommandGetDbSchemas commandGetSchemas,
                                         CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_SCHEMAS_SCHEMA;
    return getFlightInfoForFlightSqlCommands(commandGetSchemas, flightDescriptor, schema);
  }

  @Override
  public void getStreamSchemas(CommandGetDbSchemas commandGetSchemas, CallContext callContext,
                               ServerStreamListener serverStreamListener) {
    final UserSession session = getUserSessionData(callContext).getSession();

    String catalog = commandGetSchemas.hasCatalog() ? commandGetSchemas.getCatalog() : null;
    String schemaFilterPattern =
      commandGetSchemas.hasDbSchemaFilterPattern() ? commandGetSchemas.getDbSchemaFilterPattern() : null;

    flightWorkManager.getSchemas(catalog, schemaFilterPattern, serverStreamListener, allocator,
      callContext::isCancelled, session);
  }

  @Override
  public FlightInfo getFlightInfoTables(CommandGetTables commandGetTables, CallContext callContext,
                                        FlightDescriptor flightDescriptor) {
    final Schema schema;
    if (commandGetTables.getIncludeSchema()) {
      schema = Schemas.GET_TABLES_SCHEMA;
    } else {
      schema = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
    }

    return getFlightInfoForFlightSqlCommands(commandGetTables, flightDescriptor, schema);
  }

  @Override
  public void getStreamTables(CommandGetTables commandGetTables, CallContext callContext,
                              ServerStreamListener serverStreamListener) {
    final UserSession session = getUserSessionData(callContext).getSession();

    flightWorkManager.runGetTables(commandGetTables, serverStreamListener, callContext::isCancelled,
      allocator, session);
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(CommandGetTableTypes commandGetTableTypes, CallContext callContext,
                                            FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_TABLE_TYPES_SCHEMA;

    return getFlightInfoForFlightSqlCommands(commandGetTableTypes, flightDescriptor, schema);
  }

  @Override
  public void getStreamTableTypes(CallContext callContext,
                                  ServerStreamListener serverStreamListener) {
    flightWorkManager.runGetTablesTypes(serverStreamListener, allocator);
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(
    CommandGetPrimaryKeys commandGetPrimaryKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_PRIMARY_KEYS_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public void getStreamPrimaryKeys(CommandGetPrimaryKeys commandGetPrimaryKeys,
                                   CallContext callContext,
                                   ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetPrimaryKeys not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_EXPORTED_KEYS_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public void getStreamExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext,
    ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetExportedKeys not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_IMPORTED_KEYS_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(
    CommandGetCrossReference commandGetCrossReference,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_CROSS_REFERENCE_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public void getStreamImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext,
    ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetImportedKeys not supported.").toRuntimeException();
  }

  @Override
  public void getStreamCrossReference(
    CommandGetCrossReference commandGetCrossReference,
    CallContext callContext, ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetCrossReference not supported.").toRuntimeException();
  }

  @Override
  public void close() throws Exception {

  }

  private void runPreparedStatement(CallContext callContext,
                                    ServerStreamListener serverStreamListener,
                                    UserProtos.PreparedStatementHandle preparedStatementHandle) {
    final UserSessionService.UserSessionData sessionData = getUserSessionData(callContext);
    final ChangeTrackingUserSession userSession = new ChangeTrackingUserSession(sessionData.getSession());

    flightWorkManager.runPreparedStatement(preparedStatementHandle, serverStreamListener, allocator, userSession,
      () -> {
        if (userSession.isUpdated()) {
          sessionsManager.updateSession(sessionData);
        }
      }
    );
  }

  /**
   * Helper method to retrieve CallHeaders from the CallContext.
   *
   * @param callContext the CallContext to retrieve headers from.
   * @return CallHeaders retrieved from provided CallContext.
   */
  private CallHeaders retrieveHeadersFromCallContext(CallContext callContext) {
    return callContext.getMiddleware(DremioFlightService.FLIGHT_CLIENT_PROPERTIES_MIDDLEWARE_KEY).headers();
  }

  private UserSessionService.UserSessionData getUserSessionData(CallContext callContext) {
    final CallHeaders incomingHeaders = retrieveHeadersFromCallContext(callContext);
    UserSessionService.UserSessionData sessionData = sessionsManager.getUserSession(callContext.peerIdentity(), incomingHeaders);

    if (sessionData == null) {
      try {
        sessionData = sessionsManager.createUserSession(callContext.peerIdentity(), incomingHeaders);
        sessionsManager.decorateResponse(callContext, sessionData);
      } catch (Exception e) {
        logger.error("Unable to create user session", e);
        throw CallStatus.INTERNAL.toRuntimeException();
      }
    }

    return sessionData;
  }

  private <T extends Message> FlightInfo getFlightInfoForFlightSqlCommands(
    T command, FlightDescriptor flightDescriptor, Schema schema) {
    final Ticket ticket = new Ticket(pack(command).toByteArray());

    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, location);
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
  }

  private boolean isFlightSqlCommand(Any command) {
    return command.is(CommandStatementQuery.class) || command.is(CommandPreparedStatementQuery.class) ||
      command.is(CommandGetCatalogs.class) || command.is(CommandGetDbSchemas.class) ||
      command.is(CommandGetTables.class) || command.is(CommandGetTableTypes.class) ||
      command.is(CommandGetSqlInfo.class) || command.is(CommandGetPrimaryKeys.class) ||
      command.is(CommandGetExportedKeys.class) || command.is(CommandGetImportedKeys.class) ||
      command.is(TicketStatementQuery.class);
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
