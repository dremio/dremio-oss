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

import static org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
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

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.service.flight.impl.FlightWorkManager;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;

/**
 * A FlightProducer implementation which exposes Dremio's catalog and produces results from SQL queries.
 */
public class DremioFlightSqlProducer implements FlightSqlProducer {
  private final FlightWorkManager flightWorkManager;
  private final DremioFlightSessionsManager sessionsManager;
  private final BufferAllocator allocator;

  public DremioFlightSqlProducer(DremioFlightSessionsManager sessionsManager,
                                 Provider<UserWorker> workerProvider, Provider<OptionManager> optionManagerProvider,
                                 BufferAllocator allocator,
                                 RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) {
    this.sessionsManager = sessionsManager;
    this.allocator = allocator;

    flightWorkManager = new FlightWorkManager(workerProvider, optionManagerProvider, runQueryResponseHandlerFactory);
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listFlights is unimplemented").toRuntimeException();
  }

  @Override
  public void createPreparedStatement(
    ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("createPreparedStatement not supported.").toRuntimeException();
  }

  @Override
  public void closePreparedStatement(
    ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
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
}
