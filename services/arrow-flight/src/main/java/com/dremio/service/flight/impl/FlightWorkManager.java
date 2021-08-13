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

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

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
import com.dremio.service.flight.TicketContent;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BackpressureHandlingResponseHandler;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BasicResponseHandler;
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
                                                         Supplier<Boolean> isRequestCancelled, UserSession userSession) {
    final String query = getQuery(flightDescriptor);

    final UserProtos.CreatePreparedStatementArrowReq createPreparedStatementReq =
      UserProtos.CreatePreparedStatementArrowReq.newBuilder()
        .setSqlQuery(query)
        .build();

    final UserBitShared.ExternalId prepareExternalId = ExternalIdHelper.generateExternalId();
    final UserRequest userRequest =
      new UserRequest(UserProtos.RpcType.CREATE_PREPARED_STATEMENT_ARROW, createPreparedStatementReq);

    final CreatePreparedStatementResponseHandler createPreparedStatementResponseHandler =
      new CreatePreparedStatementResponseHandler(prepareExternalId, userSession, workerProvider, isRequestCancelled);

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

    workerProvider.get().submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);
  }

  public void runGetTables(FlightSql.CommandGetTables commandGetTables, FlightProducer.ServerStreamListener listener, BufferAllocator allocator,
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

    final UserResponseHandler responseHandler = new GetTablesResponseHandler(allocator, listener);

    workerProvider.get().submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);
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
