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

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.DremioFlightServiceOptions;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BackpressureHandlingResponseHandler;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BasicResponseHandler;
import com.dremio.service.flight.protector.CancellableUserResponseHandler;
import com.google.common.annotations.VisibleForTesting;

/**
 * Manager class for submitting jobs to a UserWorker and optionally returning the appropriate Dremio Flight
 * Server container to use the outcome of the submitted job.
 */
public class FlightWorkManager {

  private final Provider<UserWorker> workerProvider;
  private final Provider<OptionManager> optionManagerProvider;
  private final RunQueryResponseHandlerFactory runQueryResponseHandlerFactory;

  public FlightWorkManager(Provider<UserWorker> workerProvider,
                           Provider<OptionManager> optionManagerProvider,
                           RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) {
    this.workerProvider = workerProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.runQueryResponseHandlerFactory = runQueryResponseHandlerFactory;
  }

  /**
   * Submits a CREATE_PREPARED_STATEMENT job to a worker and returns a FlightPreparedStatement.
   *
   * @param flightDescriptor   The client request containing the query to execute.
   * @param isRequestCancelled A supplier to evaluate if the client cancelled the request.
   * @param userSession        The session for the user which made the request.
   * @return A FlightPreparedStatement which consumes the result of the job.
   */
  public FlightPreparedStatement createPreparedStatement(FlightDescriptor flightDescriptor,
                                                         Supplier<Boolean> isRequestCancelled,
                                                         UserSession userSession) {
    final String query = getQuery(flightDescriptor);

    final UserProtos.CreatePreparedStatementArrowReq createPreparedStatementReq =
      UserProtos.CreatePreparedStatementArrowReq.newBuilder()
        .setSqlQuery(query)
        .build();

    final UserBitShared.ExternalId prepareExternalId = ExternalIdHelper.generateExternalId();
    final UserRequest userRequest =
      new UserRequest(UserProtos.RpcType.CREATE_PREPARED_STATEMENT_ARROW, createPreparedStatementReq);

    final CancellableUserResponseHandler<UserProtos.CreatePreparedStatementArrowResp>
      createPreparedStatementResponseHandler =
      new CancellableUserResponseHandler<>(prepareExternalId, userSession,
        workerProvider, isRequestCancelled, UserProtos.CreatePreparedStatementArrowResp.class);

    workerProvider.get().submitWork(prepareExternalId, userSession, createPreparedStatementResponseHandler,
      userRequest, TerminationListenerRegistry.NOOP);

    return new FlightPreparedStatement(flightDescriptor, query, createPreparedStatementResponseHandler);
  }

  public void runPreparedStatement(UserProtos.PreparedStatementHandle preparedStatementHandle,
                                   FlightProducer.ServerStreamListener listener, BufferAllocator allocator,
                                   UserSession userSession) {
    final UserBitShared.ExternalId runExternalId = ExternalIdHelper.generateExternalId();
    final UserRequest userRequest =
      new UserRequest(UserProtos.RpcType.RUN_QUERY,
        UserProtos.RunQuery.newBuilder()
          .setType(UserBitShared.QueryType.PREPARED_STATEMENT)
          .setPriority(UserProtos.QueryPriority.newBuilder()
            .setWorkloadType(UserBitShared.WorkloadType.FLIGHT)
            .setWorkloadClass(UserBitShared.WorkloadClass.GENERAL))
          .setSource(UserProtos.SubmissionSource.FLIGHT)
          .setPreparedStatementHandle(preparedStatementHandle)
          .build());

    final UserResponseHandler responseHandler = runQueryResponseHandlerFactory.getHandler(runExternalId, userSession,
      workerProvider, optionManagerProvider, listener, allocator);

    workerProvider.get()
      .submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);
  }

  /**
   * Submits a GET_CATALOGS job to a worker and sends the response to given ServerStreamListener.
   *
   * @param listener    ServerStreamListener listening to the job result.
   * @param allocator   BufferAllocator used to allocate the response VectorSchemaRoot.
   * @param userSession The session for the user which made the request.
   */
  public void getCatalogs(FlightProducer.ServerStreamListener listener, BufferAllocator allocator,
                          Supplier<Boolean> isRequestCancelled, UserSession userSession) {
    final UserBitShared.ExternalId runExternalId = ExternalIdHelper.generateExternalId();
    final UserRequest userRequest =
      new UserRequest(UserProtos.RpcType.GET_CATALOGS, UserProtos.GetCatalogsReq.newBuilder().build());

    final CancellableUserResponseHandler<UserProtos.GetCatalogsResp> responseHandler =
      new CancellableUserResponseHandler<>(runExternalId, userSession, workerProvider, isRequestCancelled,
        UserProtos.GetCatalogsResp.class);

    workerProvider.get()
      .submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);

    UserProtos.GetCatalogsResp response = responseHandler.get();
    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA,
      allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");

      int i = 0;
      for (UserProtos.CatalogMetadata catalogMetadata : response.getCatalogsList()) {
        catalogNameVector.setSafe(i, new Text(catalogMetadata.getCatalogName()));
        i++;
      }

      vectorSchemaRoot.setRowCount(response.getCatalogsCount());
      listener.putNext();
      listener.completed();
    }
    workerProvider.get().submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);
  }

  /**
   * Submits a GET_TABLES job to a worker and sends the response to given ServerStreamListener.
   *
   * @param listener    ServerStreamListener listening to the job result.
   * @param allocator   BufferAllocator used to allocate the response VectorSchemaRoot.
   * @param userSession The session for the user which made the request.
   */
  public void runGetTables(FlightSql.CommandGetTables commandGetTables,
                           FlightProducer.ServerStreamListener listener,
                           Supplier<Boolean> isRequestCancelled,
                           BufferAllocator allocator,
                           UserSession userSession) {
    final UserBitShared.ExternalId runExternalId = ExternalIdHelper.generateExternalId();
    final UserProtos.GetTablesReq.Builder builder = UserProtos.GetTablesReq.newBuilder();

    if (commandGetTables.hasSchemaFilterPattern()) {
      builder.setSchemaNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(commandGetTables.getSchemaFilterPattern().getValue()).build());
    }

    if (commandGetTables.hasTableNameFilterPattern()) {
      builder.setTableNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(commandGetTables.getTableNameFilterPattern().getValue()).build());
    }

    if (commandGetTables.hasCatalog()) {
      builder.setCatalogNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(commandGetTables.getCatalog().getValue()).build());
    }

    if (!commandGetTables.getTableTypesList().isEmpty()) {
      builder.addAllTableTypeFilter(commandGetTables.getTableTypesList());
    }

    final UserRequest userRequest =
      new UserRequest(UserProtos.RpcType.GET_TABLES, builder.build());

    final CancellableUserResponseHandler<UserProtos.GetTablesResp> responseHandler =
      new CancellableUserResponseHandler<>(runExternalId, userSession, workerProvider, isRequestCancelled,
        UserProtos.GetTablesResp.class);

    workerProvider.get().submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);

    final UserProtos.GetTablesResp getTablesResp = responseHandler.get();

    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA,
      allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");
      VarCharVector schemaNameVector = (VarCharVector) vectorSchemaRoot.getVector("schema_name");
      VarCharVector tableNameVector = (VarCharVector) vectorSchemaRoot.getVector("table_name");
      VarCharVector tableTypeVector = (VarCharVector) vectorSchemaRoot.getVector("table_type");

      final int tablesCount = getTablesResp.getTablesCount();
      final IntStream range = IntStream.range(0, tablesCount);

      range.forEach(i ->{
        final UserProtos.TableMetadata tables = getTablesResp.getTables(i);
        catalogNameVector.setSafe(i, new Text(tables.getCatalogName()));
        schemaNameVector.setSafe(i, new Text(tables.getSchemaName()));
        tableNameVector.setSafe(i, new Text(tables.getTableName()));
        tableTypeVector.setSafe(i, new Text(tables.getType()));
      });

      vectorSchemaRoot.setRowCount(tablesCount);
      listener.putNext();
      listener.completed();
    }
  }

  @VisibleForTesting
  static String getQuery(FlightDescriptor descriptor) {
    if (!descriptor.isCommand()) {
      throw CallStatus.UNIMPLEMENTED
        .withDescription("FlightDescriptor type Path is unimplemented.")
        .toRuntimeException();
    }
    if (descriptor.getCommand() == null) {
      throw CallStatus.UNIMPLEMENTED
        .withDescription("FlightDescriptor type Cmd must have content in the cmd member.")
        .toRuntimeException();
    }
    byte[] rawBytes = descriptor.getCommand();
    return new String(rawBytes, StandardCharsets.UTF_8);
  }

  /**
   * A factory to create RunQueryResponseHandlers.
   */
  @VisibleForTesting
  public interface RunQueryResponseHandlerFactory {
    RunQueryResponseHandlerFactory DEFAULT = new RunQueryResponseHandlerFactory() {
    };

    default UserResponseHandler getHandler(UserBitShared.ExternalId runExternalId,
                                           UserSession userSession,
                                           Provider<UserWorker> workerProvider,
                                           Provider<OptionManager> optionManagerProvider,
                                           FlightProducer.ServerStreamListener clientListener,
                                           BufferAllocator allocator) {

      if (optionManagerProvider.get().getOption(DremioFlightServiceOptions.ENABLE_BACKPRESSURE_HANDLING)) {
        return new BackpressureHandlingResponseHandler(runExternalId, userSession, workerProvider, clientListener,
          allocator);
      } else {
        return new BasicResponseHandler(runExternalId, userSession, workerProvider, clientListener, allocator);
      }
    }
  }
}
