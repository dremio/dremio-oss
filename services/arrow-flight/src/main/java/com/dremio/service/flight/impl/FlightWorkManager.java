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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Provider;

import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.expression.CompleteType;
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
import com.dremio.service.flight.utils.TypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Manager class for submitting jobs to a UserWorker and optionally returning the appropriate Dremio Flight
 * Server container to use the outcome of the submitted job.
 */
public class FlightWorkManager {

  private final Provider<UserWorker> workerProvider;
  private final Provider<OptionManager> optionManagerProvider;
  private final RunQueryResponseHandlerFactory runQueryResponseHandlerFactory;

  private static final byte[] EMPTY_SERIALIZED_SCHEMA = getSerializedSchema(Collections.emptyList());

  /**
   * Data returned by Flight SQL's CommandXdbcGetTypeInfo.
   * All values originally came from Dremio's legacy ODBC driver.
   */
  private static final List<TypeInfo> TYPE_INFOS =
    ImmutableList.of(
      new TypeInfo("NATIONAL CHARACTER VARYING", -9, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "WVARCHAR", null, null, -9, null, null),
      new TypeInfo("WVARCHAR", -9, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "WVARCHAR", null, null, -9, null, null),
      new TypeInfo("NATIONAL CHARACTER", -8, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "WCHAR", null, null, -8, null, null),
      new TypeInfo("WCHAR", -8, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "WCHAR", null, null, -8, null, null),
      new TypeInfo("BIT", -7, 1, null, null, null, 1, 0, 2, null, 0, null, "BIT", null, null, -7, null, null),
      new TypeInfo("BOOLEAN", -7, 1, null, null, null, 1, 0, 2, null, 0, null, "BIT", null, null, -7, null, null),
      new TypeInfo("TINYINT", -6, 3, null, null, null, 1, 0, 2, 0, 0, null, "TINYINT", null, null, -6, null, 2),
      new TypeInfo("BIGINT", -5, 19, null, null, null, 1, 0, 2, 0, 0, null, "BIGINT", null, null, -5, null, 2),
      new TypeInfo("BINARY VARYING", -3, 2147483647, null, null, "max length", 1, 0, 2, null, 0, null, "VARBINARY", null, null, -3, null, null),
      new TypeInfo("VARBINARY", -3, 2147483647, null, null, "max length", 1, 0, 2, null, 0, null, "VARBINARY", null, null, -3, null, null),
      new TypeInfo("BINARY", -2, 2147483647, null, null, "length", 1, 0, 2, null, 0, null, "BINARY", null, null, -2, null, null),
      new TypeInfo("CHAR", 1, 2147483647, "'", "'", "length", 1, 0, 3, null, 0, null, "CHAR", null, null, 1, null, null),
      new TypeInfo("CHARACTER", 1, 2147483647, "'", "'", "length", 1, 0, 3, null, 0, null, "CHAR", null, null, 1, null, null),
      new TypeInfo("DECIMAL", 3, 38, null, null, null, 1, 0, 2, 0, 0, null, "DECIMAL", 0, 0, 3, null, 10),
      new TypeInfo("INTEGER", 4, 10, null, null, null, 1, 0, 2, 0, 0, null, "INTEGER", null, null, 4, null, 2),
      new TypeInfo("SMALLINT", 5, 5, null, null, null, 1, 0, 2, 0, 0, null, "SMALLINT", null, null, 5, null, 2),
      new TypeInfo("FLOAT", 7, 24, null, null, null, 1, 0, 2, 0, 0, null, "REAL", 0, 0, 7, null, 2),
      new TypeInfo("REAL", 7, 24, null, null, null, 1, 0, 2, 0, 0, null, "REAL", 0, 0, 7, null, 2),
      new TypeInfo("DOUBLE", 8, 53, null, null, null, 1, 0, 2, 0, 0, null, "DOUBLE", 0, 0, 8, null, 2),
      new TypeInfo("ANY", 12, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "VARCHAR", null, null, 12, null, null),
      new TypeInfo("ARRAY", 12, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "VARCHAR", null, null, 12, null, null),
      new TypeInfo("CHARACTER VARYING", 12, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "VARCHAR", null, null, 12, null, null),
      new TypeInfo("MAP", 12, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "VARCHAR", null, null, 12, null, null),
      new TypeInfo("NULL", 12, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "VARCHAR", null, null, 12, null, null),
      new TypeInfo("UNION", 12, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "VARCHAR", null, null, 12, null, null),
      new TypeInfo("VARCHAR", 12, 2147483647, "'", "'", "max length", 1, 0, 3, null, 0, null, "VARCHAR", null, null, 12, null, null),
      new TypeInfo("DATE", 91, 10, null, null, null, 1, 0, 2, null, 0, null, "TYPE_DATE", null, null, 9, 1, null),
      new TypeInfo("TIME", 92, 12, null, null, null, 1, 0, 2, null, 0, null, "TYPE_TIME", 0, 3, 9, 2, null),
      new TypeInfo("TIME WITH TIME ZONE", 93, 23, null, null, null, 1, 0, 2, null, 0, null, "TYPE_TIMESTAMP", 0, 3, 9, 3, null),
      new TypeInfo("TIMESTAMP WITH TIME ZONE", 93, 23, null, null, null, 1, 0, 2, null, 0, null, "TYPE_TIMESTAMP", 0, 3, 9, 3, null),
      new TypeInfo("TIMESTAMP", 93, 23, null, null, null, 1, 0, 2, null, 0, null, "TYPE_TIMESTAMP", 0, 3, 9, 3, null),
      new TypeInfo("INTERVAL YEAR TO MONTH", 107, 5, null, null, null, 1, 0, 2, null, 0, null, "INTERVAL_YEAR_TO_MONTH", null, null, 10, 7, null),
      new TypeInfo("INTERVAL", 110, 18, null, null, null, 1, 0, 2, null, 0, null, "INTERVAL_DAY_TO_SECOND", null, null, 10, 10, null),
      new TypeInfo("INTERVAL DAY TO SECOND", 110, 18, null, null, null, 1, 0, 2, null, 0, null, "INTERVAL_DAY_TO_SECOND", null, null, 10, 10, null));

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
      final IntStream range = range(0, tablesCount);

      range.forEach(i -> tableTypeVector.setSafe(i, new Text(String.valueOf(tableTypes.get(i)))));

      vectorSchemaRoot.setRowCount(tablesCount);
      listener.putNext();
      listener.completed();
    }
  }

  /**
   * Retrieve the type info values and sends the response to given ServerStreamListener.
   *
   * @param listener  ServerStreamListener listening to the job result.
   * @param allocator BufferAllocator used to allocate the response VectorSchemaRo
   */
  public void runGetTypeInfo(ServerStreamListener listener,
                             BufferAllocator allocator) {
    Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");

    try (VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA,
      allocator)) {
      listener.start(root);

      root.allocateNew();

      VarCharVector typeNameVector = (VarCharVector) root.getVector("type_name");
      IntVector dataTypeVector = (IntVector) root.getVector("data_type");
      IntVector columnSizeVector = (IntVector) root.getVector("column_size");
      VarCharVector literalPrefixVector = (VarCharVector) root.getVector("literal_prefix");
      VarCharVector literalSuffixVector = (VarCharVector) root.getVector("literal_suffix");
      ListVector createParamsVector = (ListVector) root.getVector("create_params");
      IntVector nullableVector = (IntVector) root.getVector("nullable");
      BitVector caseSensitiveVector = (BitVector) root.getVector("case_sensitive");
      IntVector searchableVector = (IntVector) root.getVector("searchable");
      BitVector unsignedAttributeVector = (BitVector) root.getVector("unsigned_attribute");
      BitVector fixedPrecScaleVector = (BitVector) root.getVector("fixed_prec_scale");
      BitVector autoIncrementVector = (BitVector) root.getVector("auto_increment");
      VarCharVector localTypeNameVector = (VarCharVector) root.getVector("local_type_name");
      IntVector minimumScaleVector = (IntVector) root.getVector("minimum_scale");
      IntVector maximumScaleVector = (IntVector) root.getVector("maximum_scale");
      IntVector sqlDataTypeVector = (IntVector) root.getVector("sql_data_type");
      IntVector sqlDatetimeSubVector = (IntVector) root.getVector("datetime_subcode");
      IntVector numPrecRadixVector = (IntVector) root.getVector("num_prec_radix");

      for (int i = 0; i < TYPE_INFOS.size(); i++) {
        TypeInfo typeInfo = TYPE_INFOS.get(i);
        if (typeInfo.getTypeName() != null) {
          typeNameVector.setSafe(i, new Text(typeInfo.getTypeName()));
        }
        if (typeInfo.getDataType() != null) {
          dataTypeVector.setSafe(i, typeInfo.getDataType());
        }
        if (typeInfo.getColumnSize() != null) {
          columnSizeVector.setSafe(i, typeInfo.getColumnSize());
        }
        if (typeInfo.getLiteralPrefix() != null) {
          literalPrefixVector.setSafe(i, new Text(typeInfo.getLiteralPrefix()));
        }
        if (typeInfo.getLiteralSuffix() != null) {
          literalSuffixVector.setSafe(i, new Text(typeInfo.getLiteralSuffix()));
        }
        if (typeInfo.getCreateParams() != null) {
          fillListVector(createParamsVector, i,typeInfo.getCreateParams());
        }
        if (typeInfo.getNullable() != null) {
          nullableVector.setSafe(i, typeInfo.getNullable());
        }
        if (typeInfo.getCaseSensitive() != null) {
          caseSensitiveVector.setSafe(i, typeInfo.getCaseSensitive());
        }
        if (typeInfo.getSearchable() != null) {
          searchableVector.setSafe(i, typeInfo.getSearchable());
        }
        if (typeInfo.getUnsignedAttribute() != null) {
          unsignedAttributeVector.setSafe(i, typeInfo.getUnsignedAttribute());
        }
        if (typeInfo.getFixedPrecScale() != null) {
          fixedPrecScaleVector.setSafe(i, typeInfo.getFixedPrecScale());
        }
        if (typeInfo.getAutoUniqueValue() != null) {
          autoIncrementVector.setSafe(i, typeInfo.getAutoUniqueValue());
        }
        if (typeInfo.getLocalTypeName() != null) {
          localTypeNameVector.setSafe(i, new Text(typeInfo.getLocalTypeName()));
        }
        if (typeInfo.getMinimumScale() != null) {
          minimumScaleVector.setSafe(i, typeInfo.getMinimumScale());
        }
        if (typeInfo.getMaximumScale() != null) {
          maximumScaleVector.setSafe(i, typeInfo.getMaximumScale());
        }
        if (typeInfo.getSqlDataType() != null) {
          sqlDataTypeVector.setSafe(i, typeInfo.getSqlDataType());
        }
        if (typeInfo.getSqlDatetimeSub() != null) {
          sqlDatetimeSubVector.setSafe(i, typeInfo.getSqlDatetimeSub());
        }
        if (typeInfo.getNumPrecRadix() != null) {
          numPrecRadixVector.setSafe(i, typeInfo.getNumPrecRadix());
        }
      }

      root.setRowCount(TYPE_INFOS.size());
      listener.putNext();
      listener.completed();
    }
  }

  private void fillListVector(ListVector listVector, int i, String s) {
    try (ArrowBuf buf = listVector.getAllocator().buffer(1024)) {
      UnionListWriter writer = listVector.getWriter();
      writer.setPosition(i);
      writer.startList();

      String[] split = s.split(",");
      range(0, split.length)
        .forEach(j -> {
          byte[] bytes = split[j].getBytes(UTF_8);
          Preconditions.checkState(bytes.length < 1024,
            "The amount of bytes is greater than what the ArrowBuf supports");
          buf.setBytes(0, bytes);
          writer.varChar().writeVarChar(0, bytes.length, buf);
        });
      writer.endList();
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

    final UserProtos.GetTablesReq getTablesReq = builder.build();

    final UserRequest userRequest = new UserRequest(UserProtos.RpcType.GET_TABLES, getTablesReq);

    final CancellableUserResponseHandler<UserProtos.GetTablesResp> responseHandler =
      new CancellableUserResponseHandler<>(runExternalId, userSession, workerProvider, isRequestCancelled,
        UserProtos.GetTablesResp.class);

    workerProvider.get().submitWork(runExternalId, userSession, responseHandler, userRequest, TerminationListenerRegistry.NOOP);

    final UserProtos.GetTablesResp getTablesResp = responseHandler.get();

    final boolean includeSchema = commandGetTables.getIncludeSchema();

    final Map<UserProtos.TableMetadata, List<Field>> tableToFields;
    if (includeSchema) {
      tableToFields = runGetColumns(isRequestCancelled, userSession, runExternalId, getTablesReq);
    } else {
      tableToFields = null;
    }

    final Schema schema = includeSchema ? FlightSqlProducer.Schemas.GET_TABLES_SCHEMA :
      FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;

    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");
      VarCharVector schemaNameVector = (VarCharVector) vectorSchemaRoot.getVector("db_schema_name");
      VarCharVector tableNameVector = (VarCharVector) vectorSchemaRoot.getVector("table_name");
      VarCharVector tableTypeVector = (VarCharVector) vectorSchemaRoot.getVector("table_type");
      VarBinaryVector schemaVector = (VarBinaryVector) vectorSchemaRoot.getVector("table_schema");

      final int tablesCount = getTablesResp.getTablesCount();
      final IntStream range = range(0, tablesCount);

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
    if (EMPTY_SERIALIZED_SCHEMA == null && fields == null) {
      fields = Collections.emptyList();
    } else if (fields == null) {
      return Arrays.copyOf(EMPTY_SERIALIZED_SCHEMA, EMPTY_SERIALIZED_SCHEMA.length);
    }

    final ByteArrayOutputStream columnOutputStream = new ByteArrayOutputStream();
    final Schema schema = new Schema(fields);

    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(columnOutputStream)), schema);
    } catch (final IOException e) {
      throw new RuntimeException("IO Error when serializing schema '" + schema + "'.", e);
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
                                                                   UserBitShared.ExternalId runExternalId, UserProtos.GetTablesReq getTablesReq) {
    final UserBitShared.ExternalId columnRunExternalId = ExternalIdHelper.generateExternalId();

    final UserProtos.GetColumnsReq.Builder columnBuilder = UserProtos.GetColumnsReq.newBuilder();
    columnBuilder.setSupportsComplexTypes(userSession.isSupportComplexTypes());

    // Reuse GetTablesReq to filter the columns in the GET_COLUMNS call
    columnBuilder.setCatalogNameFilter(getTablesReq.getCatalogNameFilter());
    columnBuilder.setSchemaNameFilter(getTablesReq.getSchemaNameFilter());
    columnBuilder.setTableNameFilter(getTablesReq.getTableNameFilter());

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
    Map<UserProtos.TableMetadata, List<Field>> result = new LinkedHashMap<>();

    final IntStream columnsRange = range(0, getColumnsResp.getColumnsCount());

    columnsRange.forEach(i -> {
      final UserProtos.ColumnMetadata columnMetadata = getColumnsResp.getColumns(i);

      final UserProtos.TableMetadata tableMetadata = UserProtos.TableMetadata.newBuilder()
        .setSchemaName(columnMetadata.getSchemaName())
        .setTableName(columnMetadata.getTableName())
        .build();
      final List<Field> fields = result.computeIfAbsent(tableMetadata, tableName_ -> new ArrayList<>());

      final ArrowType columnArrowType = getArrowType(
        columnMetadata.getDataType(),
        columnMetadata.getNumericPrecision(),
        columnMetadata.getNumericScale());

      List<Field> columnArrowTypeChildren;
      // Arrow complex types may require children fields for parsing the schema on C++
      switch (columnArrowType.getTypeID()) {
        case List:
        case LargeList:
        case FixedSizeList:
          columnArrowTypeChildren = Collections.singletonList(Field.notNullable(BaseRepeatedValueVector.DATA_VECTOR_NAME,
            ZeroVector.INSTANCE.getField().getType()));
          break;
        case Map:
          columnArrowTypeChildren = Collections.singletonList(Field.notNullable(MapVector.DATA_VECTOR_NAME, new ArrowType.List()));
          break;
        case Struct:
          columnArrowTypeChildren = Collections.emptyList();
          break;
        default:
          columnArrowTypeChildren = null;
          break;
      }

      final Field field = new Field(
        columnMetadata.getColumnName(),
        new FieldType(
          columnMetadata.getIsNullable(),
          columnArrowType,
          null,
          createFlightSqlColumnMetadata(columnMetadata, columnArrowType)),
        columnArrowTypeChildren);
      fields.add(field);
    });

    return result;
  }

  private static Map<String, String> createFlightSqlColumnMetadata(final UserProtos.ColumnMetadata columnMetadata,
                                                                   final ArrowType columnArrowType) {
    final FlightSqlColumnMetadata.Builder columnMetadataBuilder = new FlightSqlColumnMetadata.Builder()
      .schemaName(columnMetadata.getSchemaName())
      .tableName(columnMetadata.getTableName())
      .typeName(columnMetadata.getDataType())
      // Set same values as PreparedStatementProvider#serializeColumn
      .isAutoIncrement(false)
      .isCaseSensitive(false)
      .isReadOnly(true)
      .isSearchable(true);

    Integer scale = columnMetadata.hasNumericScale() ? columnMetadata.getNumericScale() : null;
    Integer precision = columnMetadata.hasNumericPrecision() ? columnMetadata.getNumericPrecision() : null;
    if (columnArrowType != null) {
      final CompleteType type = new CompleteType(columnArrowType);
      if (type.getPrecision() != null) {
        precision = type.getPrecision();
      }
      if (type.getScale() != null) {
        scale = type.getScale();
      }
    }
    if (precision != null) {
      columnMetadataBuilder.precision(precision);
    }
    if (scale != null) {
      columnMetadataBuilder.scale(scale);
    }

    return columnMetadataBuilder.build().getMetadataMap();
  }

  /**
   * Convert Dremio data type to an arrowType.
   *
   * @param dataType  dremio data type.
   * @param precision numeric precision in case the type is numeric.
   * @param scale     scale in case the type is numeric.
   * @return          the Arrow type that is equivalent to dremio type.
   */
  private static ArrowType getArrowType(final String dataType, final int precision, final int scale) {
    // Should be close to com.dremio.common.types.Types and com.dremio.exec.store.ischema.Column
    // as they're used for GetColummsReq as well
    switch (dataType) {
      case "ARRAY":
        return new ArrowType.List();
      case "BIGINT":
        return new ArrowType.Int(64, true);
      case "INTEGER":
        return new ArrowType.Int(32, true);
      case "SMALLINT":
        return new ArrowType.Int(16, true);
      case "TINYINT":
        return new ArrowType.Int(8, true);
      case "BINARY VARYING":
      case "BINARY":
        return new ArrowType.Binary();
      case "BOOLEAN":
        return new ArrowType.Bool();
      case "NATIONAL CHARACTER VARYING":
      case "CHARACTER VARYING":
      case "NATIONAL CHARACTER":
      case "CHARACTER":
        return new ArrowType.Utf8();
      case "DATE":
        return new ArrowType.Date(DateUnit.MILLISECOND);
      case "DECIMAL":
        return new ArrowType.Decimal(precision, scale, 128);
      case "DOUBLE":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "FLOAT":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "INTERVAL YEAR TO MONTH":
        return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
      case "INTERVAL DAY TO SECOND":
        return new ArrowType.Interval(IntervalUnit.DAY_TIME);
      case "ROW":
        return new ArrowType.Struct();
      case "MAP":
        return new ArrowType.Map(false);
      case "TIME":
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case "TIMESTAMP":
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, JdbcToArrowUtils.getUtcCalendar().getTimeZone().getID());
      default:
        return new ArrowType.Null();
    }
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
    return new String(rawBytes, UTF_8);
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
