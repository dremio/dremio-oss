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

import static com.google.protobuf.Any.pack;
import static java.util.Collections.singletonList;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightConstants;
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
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.impl.FlightPreparedStatement;
import com.dremio.service.flight.impl.FlightWorkManager;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * A FlightProducer implementation which exposes Dremio's catalog and produces results from SQL queries.
 */
public class DremioFlightProducer implements FlightSqlProducer {

  private final FlightWorkManager flightWorkManager;
  private final Location location;
  private final DremioFlightSessionsManager sessionsManager;
  private final BufferAllocator allocator;
  private final Cache<UserProtos.PreparedStatementHandle, FlightPreparedStatement> flightPreparedStatementCache;

  public DremioFlightProducer(Location location, DremioFlightSessionsManager sessionsManager,
                              Provider<UserWorker> workerProvider, Provider<OptionManager> optionManagerProvider,
                              BufferAllocator allocator,
                              RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) {
    this.location = location;
    this.sessionsManager = sessionsManager;
    this.allocator = allocator;

    flightWorkManager = new FlightWorkManager(workerProvider, optionManagerProvider, runQueryResponseHandlerFactory);
    flightPreparedStatementCache = CacheBuilder.newBuilder()
      .maximumSize(1024)
      .expireAfterAccess(30, TimeUnit.MINUTES)
      .build();
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
                                         CallContext callContext, Ticket ticket,
                                         ServerStreamListener serverStreamListener) {
    UserProtos.PreparedStatementHandle preparedStatementHandle;
    try {
      preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(commandPreparedStatementQuery.getPreparedStatementHandle());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }

    // Check if given PreparedStatement is cached
    FlightPreparedStatement preparedStatement = flightPreparedStatementCache.getIfPresent(preparedStatementHandle);
    if (preparedStatement == null) {
      throw CallStatus.NOT_FOUND.withDescription("PreparedStatement not found.").toRuntimeException();
    }

    runPreparedStatement(callContext, serverStreamListener, preparedStatementHandle);
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
    final UserSession session = getUserSessionFromCallContext(callContext);
    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(flightDescriptor, callContext::isCancelled, session);
    return flightPreparedStatement.getFlightInfo(location);
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final UserProtos.PreparedStatementHandle preparedStatementHandle;

    try {
      preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(commandPreparedStatementQuery.getPreparedStatementHandle());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }

    FlightPreparedStatement preparedStatement = flightPreparedStatementCache.getIfPresent(preparedStatementHandle);
    if (preparedStatement == null) {
      throw CallStatus.NOT_FOUND.withDescription("PreparedStatement not found.").toRuntimeException();
    }

    Schema schema = preparedStatement.getSchema();
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
  public void createPreparedStatement(
    ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> streamListener) {
    final FlightDescriptor flightDescriptor =
      FlightDescriptor.command(actionCreatePreparedStatementRequest.getQuery().getBytes(StandardCharsets.UTF_8));

    final UserSession session = getUserSessionFromCallContext(callContext);
    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(flightDescriptor, callContext::isCancelled, session);

    flightPreparedStatementCache.put(flightPreparedStatement.getServerHandle(), flightPreparedStatement);

    final ActionCreatePreparedStatementResult action = flightPreparedStatement.createAction();

    streamListener.onNext(new Result(pack(action).toByteArray()));
    streamListener.onCompleted();

  }

  @Override
  public void closePreparedStatement(
    ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> listener) {
    try {
      UserProtos.PreparedStatementHandle preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(actionClosePreparedStatementRequest.getPreparedStatementHandle());

      flightPreparedStatementCache.invalidate(preparedStatementHandle);
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }
  }

  @Override
  public FlightInfo getFlightInfoStatement(
    CommandStatementQuery commandStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
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
  public Runnable acceptPutStatement(
    CommandStatementUpdate commandStatementUpdate,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
    CommandPreparedStatementUpdate commandPreparedStatementUpdate,
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
    return getFlightInfoForFlightSqlCommands(commandGetCatalogs, flightDescriptor, getSchemaCatalogs().getSchema());
  }

  @Override
  public void getStreamCatalogs(CallContext callContext, Ticket ticket,
                                ServerStreamListener serverStreamListener) {
    final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
    final UserSession session = sessionsManager.getUserSession(callContext.peerIdentity(), headers);

    flightWorkManager.getCatalogs(serverStreamListener, allocator, callContext::isCancelled, session);
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
    final Schema schema = getSchemaTables().getSchema();

    return getFlightInfoForFlightSqlCommands(commandGetTables, flightDescriptor, schema);
  }

  @Override
  public void getStreamTables(CommandGetTables commandGetTables,
                              CallContext callContext, Ticket ticket,
                              ServerStreamListener serverStreamListener) {
    final UserSession session = getUserSessionFromCallContext(callContext);

    flightWorkManager.runGetTables(commandGetTables, serverStreamListener, callContext::isCancelled,
      allocator, session);
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

  private void runPreparedStatement(CallContext callContext,
                                    ServerStreamListener serverStreamListener,
                                    UserProtos.PreparedStatementHandle preparedStatementHandle) {
    final UserSession session = getUserSessionFromCallContext(callContext);
    flightWorkManager.runPreparedStatement(preparedStatementHandle, serverStreamListener, allocator, session);
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

  private UserSession getUserSessionFromCallContext(CallContext callContext) {
    final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
    return sessionsManager.getUserSession(callContext.peerIdentity(), headers);
  }

  private <T extends Message> FlightInfo getFlightInfoForFlightSqlCommands(
    T command, FlightDescriptor flightDescriptor, Schema schema) {
    final Ticket ticket = new Ticket(pack(command).toByteArray());

    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, location);
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
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
