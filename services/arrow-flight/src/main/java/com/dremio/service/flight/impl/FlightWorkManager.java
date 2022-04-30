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

import static com.dremio.common.types.Types.getJdbcTypeCode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Provider;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import com.dremio.service.catalog.TableType;
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

  private static final byte[] EMPTY_SERIALIZED_SCHEMA = getSerializedSchema(Collections.emptyList());

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
    return createPreparedStatement(query, isRequestCancelled, userSession);
  }

  /**
   * Submits a CREATE_PREPARED_STATEMENT job to a worker and returns a FlightPreparedStatement.
   *
   * @param query               The query which will be executed.
   * @param isRequestCancelled  A supplier to evaluate if the client cancelled the request.
   * @param userSession         The session for the user which made the request.
   * @return A FlightPreparedStatement which consumes the result of the job.
   */
  public FlightPreparedStatement createPreparedStatement(String query,
                                                         Supplier<Boolean> isRequestCancelled,
                                                         UserSession userSession) {
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

    return new FlightPreparedStatement(createPreparedStatementResponseHandler);
  }

  public void runPreparedStatement(UserProtos.PreparedStatementHandle preparedStatementHandle,
                                   FlightProducer.ServerStreamListener listener, BufferAllocator allocator,
                                   UserSession userSession, Runnable queryCompletionCallback) {
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
      workerProvider, optionManagerProvider, listener, allocator, queryCompletionCallback);

    workerProvider.get()
      .submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);
  }

  /**
   * Retrieve the table types and sends the response to given ServerStreamListener.
   *
   * @param listener    ServerStreamListener listening to the job result.
   * @param allocator   BufferAllocator used to allocate the response VectorSchemaRoot.
   */
  public void runGetTablesTypes(FlightProducer.ServerStreamListener listener,
                                BufferAllocator allocator) {
    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA,
      allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector tableTypeVector = (VarCharVector) vectorSchemaRoot.getVector("table_type");

      final List<TableType> tableTypes = Arrays.stream(TableType.values())
        .filter(tableType -> tableType != TableType.UNKNOWN_TABLE_TYPE && tableType != TableType.UNRECOGNIZED)
        .collect(Collectors.toList());
      final int tablesCount = tableTypes.size();
      final IntStream range = IntStream.range(0, tablesCount);

      range.forEach(i -> tableTypeVector.setSafe(i, new Text(String.valueOf(tableTypes.get(i)))));

      vectorSchemaRoot.setRowCount(tablesCount);
      listener.putNext();
      listener.completed();
    }
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

    setParameterForGetTablesExecution(commandGetTables, builder);

    final UserRequest userRequest =
      new UserRequest(UserProtos.RpcType.GET_TABLES, builder.build());

    final CancellableUserResponseHandler<UserProtos.GetTablesResp> responseHandler =
      new CancellableUserResponseHandler<>(runExternalId, userSession, workerProvider, isRequestCancelled,
        UserProtos.GetTablesResp.class);

    workerProvider.get().submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);

    final UserProtos.GetTablesResp getTablesResp = responseHandler.get();

    final boolean includeSchema = commandGetTables.getIncludeSchema();

    final Map<UserProtos.TableMetadata, List<Field>> tableToFields;
    if (includeSchema) {
      tableToFields = runGetColumns(isRequestCancelled, userSession, runExternalId);
    } else {
      tableToFields = null;
    }

    final Schema schema = includeSchema ? FlightSqlProducer.Schemas.GET_TABLES_SCHEMA :
      FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;

    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema,allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");
      VarCharVector schemaNameVector = (VarCharVector) vectorSchemaRoot.getVector("db_schema_name");
      VarCharVector tableNameVector = (VarCharVector) vectorSchemaRoot.getVector("table_name");
      VarCharVector tableTypeVector = (VarCharVector) vectorSchemaRoot.getVector("table_type");
      VarBinaryVector schemaVector = (VarBinaryVector) vectorSchemaRoot.getVector("table_schema");

      final int tablesCount = getTablesResp.getTablesCount();
      final IntStream range = IntStream.range(0, tablesCount);

      range.forEach(i -> {
        final UserProtos.TableMetadata table = getTablesResp.getTables(i);
        catalogNameVector.setNull(i);
        schemaNameVector.setSafe(i, new Text(table.getSchemaName()));
        tableTypeVector.setSafe(i, new Text(table.getType()));

        final String tableName = table.getTableName();
        tableNameVector.setSafe(i, new Text(tableName));

        if (includeSchema) {
          List<Field> fields = tableToFields.get(UserProtos.TableMetadata.newBuilder()
            .setSchemaName(table.getSchemaName())
            .setTableName(table.getTableName())
            .build());
          schemaVector.setSafe(i, getSerializedSchema(fields));
        }
      });

      vectorSchemaRoot.setRowCount(tablesCount);
      listener.putNext();
      listener.completed();
    }
  }

  /**
   * Gets a serialized schema from a list of {@link Field}.
   *
   * @param fields The Schema fields.
   * @return A serialized {@link Schema}.
   */
  @VisibleForTesting
  protected static byte[] getSerializedSchema(List<Field> fields) {
    if (fields == null) {
      return Arrays.copyOf(EMPTY_SERIALIZED_SCHEMA, EMPTY_SERIALIZED_SCHEMA.length);
    }
    final ByteArrayOutputStream columnOutputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(columnOutputStream)), new Schema(fields));
    } catch (final IOException e) {
      throw new RuntimeException("Failed to serialize schema", e);
    }
    return columnOutputStream.toByteArray();
  }

  /**
   * Set in the Tables request object the parameter that user passed via CommandGetTables.
   *
   * @param commandGetTables The command sent by the user.
   * @param builder          A builder which holds information that will be used to execute the job.
   */
  private void setParameterForGetTablesExecution(FlightSql.CommandGetTables commandGetTables, UserProtos.GetTablesReq.Builder builder) {
    if (commandGetTables.hasDbSchemaFilterPattern()) {
      builder.setSchemaNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(commandGetTables.getDbSchemaFilterPattern()).build());
    }

    if (commandGetTables.hasTableNameFilterPattern()) {
      builder.setTableNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(commandGetTables.getTableNameFilterPattern()).build());
    }

    if (!commandGetTables.getTableTypesList().isEmpty()) {
      builder.addAllTableTypeFilter(commandGetTables.getTableTypesList());
    }
  }

  /**
   * Run the GET_COLUMNS jobs when includeSchema is true.
   *
   * @param isRequestCancelled  A supplier to evaluate if the client cancelled the request.
   * @param userSession         The session for the user which made the request.
   * @param runExternalId       The id of the query to be run.
   * @return A map with tables and its fields.
   */
  private Map<UserProtos.TableMetadata, List<Field>> runGetColumns(Supplier<Boolean> isRequestCancelled, UserSession userSession,
                             UserBitShared.ExternalId runExternalId) {
    final UserBitShared.ExternalId columnRunExternalId = ExternalIdHelper.generateExternalId();

    final UserProtos.GetColumnsReq.Builder columnBuilder = UserProtos.GetColumnsReq.newBuilder();
    final UserRequest columnsRequest = new UserRequest(UserProtos.RpcType.GET_COLUMNS, columnBuilder.build());

    final CancellableUserResponseHandler<UserProtos.GetColumnsResp> columnResponseHandler =
      new CancellableUserResponseHandler<>(runExternalId, userSession, workerProvider, isRequestCancelled,
        UserProtos.GetColumnsResp.class);

    workerProvider.get().submitWork(columnRunExternalId, userSession, columnResponseHandler,
      columnsRequest, TerminationListenerRegistry.NOOP);

    final UserProtos.GetColumnsResp getColumnsResp = columnResponseHandler.get();

    return buildArrowFieldsByTableMap(getColumnsResp);
  }

  protected static Map<UserProtos.TableMetadata, List<Field>> buildArrowFieldsByTableMap(
    UserProtos.GetColumnsResp getColumnsResp) {
    Map<UserProtos.TableMetadata, List<Field>> result = new HashMap<>();

    final IntStream columnsRange = IntStream.range(0, getColumnsResp.getColumnsCount());

    columnsRange.forEach(i -> {
      final UserProtos.ColumnMetadata columns = getColumnsResp.getColumns(i);

      final UserProtos.TableMetadata tableMetadata = UserProtos.TableMetadata.newBuilder()
        .setSchemaName(columns.getSchemaName())
        .setTableName(columns.getTableName())
        .build();
      final List<Field> fields = result.computeIfAbsent(tableMetadata, tableName_ -> new ArrayList<>());

      final Field field = new Field(
        columns.getColumnName(),
        new FieldType(
          columns.getIsNullable(),
          getArrowType(getJdbcTypeCode(columns.getDataType()),
            columns.getNumericPrecision(), columns.getNumericScale()),
          null),
        null);
      fields.add(field);
    });

    return result;
  }

  /**
   * Convert Dremio data type to an arrowType.
   *
   * @param dataType  dremio data type.
   * @param precision numeric precision in case the type is numeric.
   * @param scale     scale in case the type is numeric.
   * @return          the Arrow type that is equivalent to dremio type.
   */
  private static ArrowType getArrowType(final int dataType, final int precision, final int scale) {
    return JdbcToArrowUtils.getArrowTypeFromJdbcType(new JdbcFieldInfo(dataType, precision, scale), JdbcToArrowUtils.getUtcCalendar());
  }

  /**
   * Submits a GET_SCHEMAS job to a worker and sends the response to given ServerStreamListener.
   *
   * @param catalog             catalog name to filter schemas
   * @param schemaFilterPattern pattern to filter schemas
   * @param listener            ServerStreamListener listening to the job result.
   * @param allocator           BufferAllocator used to allocate the response VectorSchemaRoot.
   * @param isRequestCancelled  A supplier to evaluate if the client cancelled the request.
   * @param userSession         The session for the user which made the request.
   */
  public void getSchemas(String catalog, String schemaFilterPattern,
                         FlightProducer.ServerStreamListener listener,
                         BufferAllocator allocator,
                         Supplier<Boolean> isRequestCancelled,
                         UserSession userSession) {
    final UserBitShared.ExternalId runExternalId = ExternalIdHelper.generateExternalId();

    final UserProtos.GetSchemasReq.Builder reqBuilder = UserProtos.GetSchemasReq.newBuilder();
    if (catalog != null) {
      UserProtos.LikeFilter filter = UserProtos.LikeFilter.newBuilder().setPattern(catalog).build();
      reqBuilder.setCatalogNameFilter(filter);
    }
    if (schemaFilterPattern != null) {
      UserProtos.LikeFilter filter = UserProtos.LikeFilter.newBuilder().setPattern(schemaFilterPattern).build();
      reqBuilder.setSchemaNameFilter(filter);
    }

    final UserRequest userRequest = new UserRequest(UserProtos.RpcType.GET_SCHEMAS, reqBuilder.build());

    final CancellableUserResponseHandler<UserProtos.GetSchemasResp> responseHandler =
      new CancellableUserResponseHandler<>(runExternalId, userSession, workerProvider, isRequestCancelled,
        UserProtos.GetSchemasResp.class);

    workerProvider.get()
      .submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);

    final UserProtos.GetSchemasResp response = responseHandler.get();
    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA,
      allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");
      VarCharVector schemaNameVector = (VarCharVector) vectorSchemaRoot.getVector("db_schema_name");

      int i = 0;
      for (UserProtos.SchemaMetadata schemaMetadata : response.getSchemasList()) {
        catalogNameVector.setNull(i);
        schemaNameVector.setSafe(i, new Text(schemaMetadata.getSchemaName()));
        i++;
      }

      vectorSchemaRoot.setRowCount(response.getSchemasCount());
      listener.putNext();
      listener.completed();
    }
  }

  public UserProtos.ServerMeta getServerMeta(Supplier<Boolean> isRequestCancelled, UserSession userSession) {
    final UserBitShared.ExternalId runExternalId = ExternalIdHelper.generateExternalId();
    final UserProtos.GetServerMetaReq.Builder reqBuilder = UserProtos.GetServerMetaReq.newBuilder();
    final UserRequest userRequest = new UserRequest(UserProtos.RpcType.GET_SERVER_META, reqBuilder.build());

    final CancellableUserResponseHandler<UserProtos.GetServerMetaResp> responseHandler =
      new CancellableUserResponseHandler<>(runExternalId, userSession, workerProvider, isRequestCancelled,
        UserProtos.GetServerMetaResp.class);

    workerProvider.get()
      .submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);

    final UserProtos.GetServerMetaResp response = responseHandler.get();

    return response.getServerMeta();
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
                                           BufferAllocator allocator,
                                           Runnable queryCompletionCallback) {

      if (optionManagerProvider.get().getOption(DremioFlightServiceOptions.ENABLE_BACKPRESSURE_HANDLING)) {
        return new BackpressureHandlingResponseHandler(runExternalId, userSession, workerProvider, clientListener,
          allocator, queryCompletionCallback);
      } else {
        return new BasicResponseHandler(runExternalId, userSession, workerProvider, clientListener, allocator, queryCompletionCallback);
      }
    }
  }
}
