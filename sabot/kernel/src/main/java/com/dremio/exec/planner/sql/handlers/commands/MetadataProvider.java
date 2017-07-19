/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.sql.handlers.commands;

import static com.dremio.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.COLS_COL_COLUMN_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.SCHS_COL_SCHEMA_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_SCHEMA;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.TBLS_COL_TABLE_TYPE;
import static com.dremio.exec.store.ischema.InfoSchemaTableType.CATALOGS;
import static com.dremio.exec.store.ischema.InfoSchemaTableType.COLUMNS;
import static com.dremio.exec.store.ischema.InfoSchemaTableType.SCHEMATA;
import static com.dremio.exec.store.ischema.InfoSchemaTableType.TABLES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.calcite.schema.SchemaPlus;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.CatalogMetadata;
import com.dremio.exec.proto.UserProtos.ColumnMetadata;
import com.dremio.exec.proto.UserProtos.GetCatalogsReq;
import com.dremio.exec.proto.UserProtos.GetCatalogsResp;
import com.dremio.exec.proto.UserProtos.GetColumnsReq;
import com.dremio.exec.proto.UserProtos.GetColumnsResp;
import com.dremio.exec.proto.UserProtos.GetSchemasReq;
import com.dremio.exec.proto.UserProtos.GetSchemasResp;
import com.dremio.exec.proto.UserProtos.GetTablesReq;
import com.dremio.exec.proto.UserProtos.GetTablesResp;
import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.SchemaMetadata;
import com.dremio.exec.proto.UserProtos.TableMetadata;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaConfig.SchemaInfoProvider;
import com.dremio.exec.store.SchemaTreeProvider;
import com.dremio.exec.store.ischema.ExprNode;
import com.dremio.exec.store.ischema.InfoSchemaFilter;
import com.dremio.exec.store.ischema.InfoSchemaFilter.ConstantExprNode;
import com.dremio.exec.store.ischema.InfoSchemaFilter.FieldExprNode;
import com.dremio.exec.store.ischema.InfoSchemaFilter.FunctionExprNode;
import com.dremio.exec.store.ischema.InfoSchemaTableType;
import com.dremio.exec.store.ischema.Records.Catalog;
import com.dremio.exec.store.ischema.Records.Column;
import com.dremio.exec.store.ischema.Records.Schema;
import com.dremio.exec.store.ischema.Records.Table;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.dremio.exec.work.protector.ResponseSenderHandler;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

/**
 * Contains worker {@link Runnable} classes for providing the metadata and related helper methods.
 */
public class MetadataProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataProvider.class);

  private static final String IN_FUNCTION = "in";
  private static final String LIKE_FUNCTION = "like";
  private static final String AND_FUNCTION = "booleanand";
  private static final String OR_FUNCTION = "booleanor";

  /**
   * Super class for all metadata provider runnable classes.
   */
  public abstract static class MetadataCommand<R> implements CommandRunner<R> {
    protected final UserSession session;
    protected final SabotContext dContext;

    protected MetadataCommand(
        final UserSession session,
        final SabotContext dContext) {
      this.session = Preconditions.checkNotNull(session);
      this.dContext = Preconditions.checkNotNull(dContext);
    }

    @Override
    public double plan() throws Exception {
      return 1;
    }

    @Override
    public CommandType getCommandType() {
      return CommandType.SYNC_RESPONSE;
    }

    @Override
    public String getDescription() {
      return "metadata; direct";
    }
  }

  public static class CatalogsHandler extends ResponseSenderHandler<GetCatalogsResp> {

    public CatalogsHandler(ResponseSender sender) {
      super(RpcType.CATALOGS, GetCatalogsResp.class, sender);
    }

    @Override
    protected GetCatalogsResp getException(UserException ex) {
      final GetCatalogsResp.Builder respBuilder = GetCatalogsResp.newBuilder();
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get catalogs", ex));
      return respBuilder.build();
    }

  }

  /**
   * Runnable that fetches the catalog metadata for given {@link GetCatalogsReq} and sends response at the end.
   */
  public static class CatalogsProvider extends MetadataCommand<GetCatalogsResp> {
    private static final Ordering<CatalogMetadata> CATALOGS_ORDERING = new Ordering<CatalogMetadata>() {
      @Override
      public int compare(CatalogMetadata left, CatalogMetadata right) {
        return Ordering.natural().compare(left.getCatalogName(), right.getCatalogName());
      }
    };
    private final GetCatalogsReq req;
    private final QueryId queryId;

    public CatalogsProvider(
        final QueryId queryId,
        final UserSession session,
        final SabotContext dContext,
        final GetCatalogsReq req) {
      super(session, dContext);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetCatalogsResp execute() throws Exception {
      final GetCatalogsResp.Builder respBuilder = GetCatalogsResp.newBuilder();
      final InfoSchemaFilter filter = createInfoSchemaFilter(req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null, null, null, null, null);

      final SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(dContext);
      final PojoRecordReader<Catalog> records = getPojoRecordReader(CATALOGS, filter, schemaTreeProvider, session);

      List<CatalogMetadata> metadata = new ArrayList<>();
      for(Catalog c : records) {
        final CatalogMetadata.Builder catBuilder = CatalogMetadata.newBuilder();
        catBuilder.setCatalogName(c.CATALOG_NAME);
        catBuilder.setDescription(c.CATALOG_DESCRIPTION);
        catBuilder.setConnect(c.CATALOG_CONNECT);

        metadata.add(catBuilder.build());
      }

      // Reorder results according to JDBC spec
      Collections.sort(metadata, CATALOGS_ORDERING);

      respBuilder.setQueryId(queryId);
      respBuilder.addAllCatalogs(metadata);
      respBuilder.setStatus(RequestStatus.OK);
      return respBuilder.build();
    }
  }


  public static class SchemasHandler extends ResponseSenderHandler<GetSchemasResp> {

    public SchemasHandler(ResponseSender sender) {
      super(RpcType.SCHEMAS, GetSchemasResp.class, sender);
    }

    @Override
    protected GetSchemasResp getException(UserException e) {
      final GetSchemasResp.Builder respBuilder = GetSchemasResp.newBuilder();
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get schemas", e));
      return respBuilder.build();
    }

  }

  /**
   * Runnable that fetches the schema metadata for given {@link GetSchemasReq} and sends response at the end.
   */
  public static class SchemasProvider extends MetadataCommand<GetSchemasResp> {
    private static final Ordering<SchemaMetadata> SCHEMAS_ORDERING = new Ordering<SchemaMetadata>() {
      @Override
      public int compare(SchemaMetadata left, SchemaMetadata right) {
        return ComparisonChain.start()
            .compare(left.getCatalogName(), right.getCatalogName())
            .compare(left.getSchemaName(), right.getSchemaName())
            .result();
      };
    };
    private final QueryId queryId;
    private final GetSchemasReq req;

    public SchemasProvider(
        final QueryId queryId,
        final UserSession session,
        final SabotContext dContext,
        final GetSchemasReq req) {
      super(session, dContext);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetSchemasResp execute() throws Exception {
      final GetSchemasResp.Builder respBuilder = GetSchemasResp.newBuilder();

      final InfoSchemaFilter filter = createInfoSchemaFilter(
          req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null,
          req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null,
          null, null, null);

      final SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(dContext);
      final PojoRecordReader<Schema> records = getPojoRecordReader(SCHEMATA, filter, schemaTreeProvider, session);

      List<SchemaMetadata> metadata = new ArrayList<>();
      for(Schema s : records) {
        final SchemaMetadata.Builder schemaBuilder = SchemaMetadata.newBuilder();
        schemaBuilder.setCatalogName(s.CATALOG_NAME);
        schemaBuilder.setSchemaName(s.SCHEMA_NAME);
        schemaBuilder.setOwner(s.SCHEMA_OWNER);
        schemaBuilder.setType(s.TYPE);
        schemaBuilder.setMutable(s.IS_MUTABLE);

        metadata.add(schemaBuilder.build());
      }
      // Reorder results according to JDBC spec
      Collections.sort(metadata, SCHEMAS_ORDERING);

      respBuilder.setQueryId(queryId);
      respBuilder.addAllSchemas(metadata);
      respBuilder.setStatus(RequestStatus.OK);

      return respBuilder.build();
    }
  }

  public static class TablesHandler extends ResponseSenderHandler<GetTablesResp> {

    public TablesHandler(ResponseSender sender) {
      super(RpcType.TABLES, GetTablesResp.class, sender);
    }

    @Override
    protected GetTablesResp getException(UserException e) {
      final GetTablesResp.Builder respBuilder = GetTablesResp.newBuilder();
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get tables", e));
      return respBuilder.build();
    }

  }

  /**
   * Runnable that fetches the table metadata for given {@link GetTablesReq} and sends response at the end.
   */
  public static class TablesProvider extends MetadataCommand<GetTablesResp> {
    private static final Ordering<TableMetadata> TABLES_ORDERING = new Ordering<TableMetadata>() {
      @Override
      public int compare(TableMetadata left, TableMetadata right) {
        return ComparisonChain.start()
            .compare(left.getType(), right.getType())
            .compare(left.getCatalogName(), right.getCatalogName())
            .compare(left.getSchemaName(), right.getSchemaName())
            .compare(left.getTableName(), right.getTableName())
            .result();
      }
    };

    private final QueryId queryId;
    private final GetTablesReq req;

    public TablesProvider(
        final QueryId queryId,
        final UserSession session,
        final SabotContext dContext,
        final GetTablesReq req) {
      super(session, dContext);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetTablesResp execute() throws Exception {
      final GetTablesResp.Builder respBuilder = GetTablesResp.newBuilder();

      final InfoSchemaFilter filter = createInfoSchemaFilter(
          req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null,
          req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null,
          req.hasTableNameFilter() ? req.getTableNameFilter() : null,
          req.getTableTypeFilterCount() != 0 ? req.getTableTypeFilterList() : null,
          null
      );

      final SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(dContext);
      final PojoRecordReader<Table> records =
          getPojoRecordReader(TABLES, filter, schemaTreeProvider, session);

      List<TableMetadata> metadata = new ArrayList<>();
      for (Table t : records) {
        final TableMetadata.Builder tableBuilder = TableMetadata.newBuilder();
        tableBuilder.setCatalogName(t.TABLE_CATALOG);
        tableBuilder.setSchemaName(t.TABLE_SCHEMA);
        tableBuilder.setTableName(t.TABLE_NAME);
        tableBuilder.setType(t.TABLE_TYPE);

        metadata.add(tableBuilder.build());
      }

      // Reorder results according to JDBC/ODBC spec
      Collections.sort(metadata, TABLES_ORDERING);

      respBuilder.setQueryId(queryId);
      respBuilder.addAllTables(metadata);
      respBuilder.setStatus(RequestStatus.OK);
      return respBuilder.build();
    }
  }

  public static class ColumnsHandler extends ResponseSenderHandler<GetColumnsResp> {

    public ColumnsHandler(ResponseSender sender) {
      super(RpcType.COLUMNS, GetColumnsResp.class, sender);
    }

    @Override
    protected GetColumnsResp getException(UserException e) {
      final GetColumnsResp.Builder respBuilder = GetColumnsResp.newBuilder();
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get columns", e));
      return respBuilder.build();
    }

  }

  /**
   * Runnable that fetches the column metadata for given {@link GetColumnsReq} and sends response at the end.
   */
  public static class ColumnsProvider extends MetadataCommand<GetColumnsResp> {
    private static final Ordering<ColumnMetadata> COLUMNS_ORDERING = new Ordering<ColumnMetadata>() {
      @Override
      public int compare(ColumnMetadata left, ColumnMetadata right) {
        return ComparisonChain.start()
            .compare(left.getCatalogName(), right.getCatalogName())
            .compare(left.getSchemaName(), right.getSchemaName())
            .compare(left.getTableName(), right.getTableName())
            .compare(left.getOrdinalPosition(), right.getOrdinalPosition())
            .result();
      }
    };

    private final QueryId queryId;
    private final GetColumnsReq req;

    public ColumnsProvider(
        final QueryId queryId,
        final UserSession session,
        final SabotContext dContext,
        final GetColumnsReq req) {
      super(session, dContext);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetColumnsResp execute() throws Exception {
      final GetColumnsResp.Builder respBuilder = GetColumnsResp.newBuilder();

      final InfoSchemaFilter filter = createInfoSchemaFilter(
          req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null,
          req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null,
          req.hasTableNameFilter() ? req.getTableNameFilter() : null,
          null,
          req.hasColumnNameFilter() ? req.getColumnNameFilter() : null
      );

      final SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(dContext);
      final PojoRecordReader<Column> records =
          getPojoRecordReader(COLUMNS, filter, schemaTreeProvider, session);

      List<ColumnMetadata> metadata = new ArrayList<>();
      for (Column c : records) {
        final ColumnMetadata.Builder columnBuilder = ColumnMetadata.newBuilder();
        columnBuilder.setCatalogName(c.TABLE_CATALOG);
        columnBuilder.setSchemaName(c.TABLE_SCHEMA);
        columnBuilder.setTableName(c.TABLE_NAME);
        columnBuilder.setColumnName(c.COLUMN_NAME);
        columnBuilder.setOrdinalPosition(c.ORDINAL_POSITION);
        if (c.COLUMN_DEFAULT != null) {
          columnBuilder.setDefaultValue(c.COLUMN_DEFAULT);
        }

        if ("YES".equalsIgnoreCase(c.IS_NULLABLE)) {
          columnBuilder.setIsNullable(true);
        } else {
          columnBuilder.setIsNullable(false);
        }
        columnBuilder.setDataType(c.DATA_TYPE);
        if (c.CHARACTER_MAXIMUM_LENGTH != null) {
          columnBuilder.setCharMaxLength(c.CHARACTER_MAXIMUM_LENGTH);
        }

        if (c.CHARACTER_OCTET_LENGTH != null) {
          columnBuilder.setCharOctetLength(c.CHARACTER_OCTET_LENGTH);
        }

        if (c.NUMERIC_SCALE != null) {
          columnBuilder.setNumericScale(c.NUMERIC_SCALE);
        }

        if (c.NUMERIC_PRECISION != null) {
          columnBuilder.setNumericPrecision(c.NUMERIC_PRECISION);
        }

        if (c.NUMERIC_PRECISION_RADIX != null) {
          columnBuilder.setNumericPrecisionRadix(c.NUMERIC_PRECISION_RADIX);
        }

        if (c.DATETIME_PRECISION != null) {
          columnBuilder.setDateTimePrecision(c.DATETIME_PRECISION);
        }

        if (c.INTERVAL_TYPE != null) {
          columnBuilder.setIntervalType(c.INTERVAL_TYPE);
        }

        if (c.INTERVAL_PRECISION != null) {
          columnBuilder.setIntervalPrecision(c.INTERVAL_PRECISION);
        }

        if (c.COLUMN_SIZE != null) {
          columnBuilder.setColumnSize(c.COLUMN_SIZE);
        }

        metadata.add(columnBuilder.build());
      }

      // Reorder results according to JDBC/ODBC spec
      Collections.sort(metadata, COLUMNS_ORDERING);

      respBuilder.setQueryId(queryId);
      respBuilder.addAllColumns(metadata);
      respBuilder.setStatus(RequestStatus.OK);
      return respBuilder.build();
    }
  }

  /**
   * Helper method to create a {@link InfoSchemaFilter} that combines the given filters with an AND.
   * @param catalogNameFilter Optional filter on <code>catalog name</code>
   * @param schemaNameFilter Optional filter on <code>schema name</code>
   * @param tableNameFilter Optional filter on <code>table name</code>
   * @param tableTypeFilter Optional filter on <code>table type</code>
   * @param columnNameFilter Optional filter on <code>column name</code>
   * @return
   */
  private static InfoSchemaFilter createInfoSchemaFilter(final LikeFilter catalogNameFilter,
      final LikeFilter schemaNameFilter, final LikeFilter tableNameFilter, List<String> tableTypeFilter, final LikeFilter columnNameFilter) {

    FunctionExprNode exprNode = createLikeFunctionExprNode(CATS_COL_CATALOG_NAME,  catalogNameFilter);

    exprNode = combineFunctions(AND_FUNCTION,
        exprNode,
        combineFunctions(OR_FUNCTION,
            createLikeFunctionExprNode(SHRD_COL_TABLE_SCHEMA, schemaNameFilter),
            createLikeFunctionExprNode(SCHS_COL_SCHEMA_NAME, schemaNameFilter)
        )
    );

    exprNode = combineFunctions(AND_FUNCTION,
        exprNode,
        createLikeFunctionExprNode(SHRD_COL_TABLE_NAME, tableNameFilter)
    );

    exprNode = combineFunctions(AND_FUNCTION,
        exprNode,
        createInFunctionExprNode(TBLS_COL_TABLE_TYPE, tableTypeFilter)
        );

    exprNode = combineFunctions(AND_FUNCTION,
        exprNode,
        createLikeFunctionExprNode(COLS_COL_COLUMN_NAME, columnNameFilter)
    );

    return exprNode != null ? new InfoSchemaFilter(exprNode) : null;
  }

  /**
   * Helper method to create {@link FunctionExprNode} from {@link LikeFilter}.
   * @param fieldName Name of the filed on which the like expression is applied.
   * @param likeFilter
   * @return {@link FunctionExprNode} for given arguments. Null if the <code>likeFilter</code> is null.
   */
  private static FunctionExprNode createLikeFunctionExprNode(String fieldName, LikeFilter likeFilter) {
    if (likeFilter == null) {
      return null;
    }

    return new FunctionExprNode(LIKE_FUNCTION,
        likeFilter.hasEscape() ?
            ImmutableList.of(
                new FieldExprNode(fieldName),
                new ConstantExprNode(likeFilter.getPattern()),
                new ConstantExprNode(likeFilter.getEscape())) :
            ImmutableList.of(
                new FieldExprNode(fieldName),
                new ConstantExprNode(likeFilter.getPattern()),
                new ConstantExprNode("\\"))
    );
  }

  /**
   * Helper method to create {@link FunctionExprNode} from {@code List<String>}.
   * @param fieldName Name of the filed on which the like expression is applied.
   * @param valuesFilter a list of values
   * @return {@link FunctionExprNode} for given arguments. Null if the <code>valuesFilter</code> is null.
   */
  private static FunctionExprNode createInFunctionExprNode(String fieldName, List<String> valuesFilter) {
    if (valuesFilter == null) {
      return null;
    }

    ImmutableList.Builder<ExprNode> nodes = ImmutableList.builder();
    nodes.add(new FieldExprNode(fieldName));
    for(String type: valuesFilter) {
      nodes.add(new ConstantExprNode(type));
    }

    return new FunctionExprNode(IN_FUNCTION, nodes.build());
  }

  /** Helper method to combine two {@link FunctionExprNode}s with a given <code>functionName</code>. If one of them is
   * null, other one is returned as it is.
   */
  private static FunctionExprNode combineFunctions(final String functionName,
      final FunctionExprNode func1, final FunctionExprNode func2) {
    if (func1 == null) {
      return func2;
    }

    if (func2 == null) {
      return func1;
    }

    return new FunctionExprNode(functionName, ImmutableList.<ExprNode>of(func1, func2));
  }

  /**
   * Helper method to create a {@link PojoRecordReader} for given arguments.
   * @param tableType
   * @param filter
   * @param provider
   * @param userSession
   * @return
   */
  private static <S> PojoRecordReader<S> getPojoRecordReader(final InfoSchemaTableType tableType, final InfoSchemaFilter filter,
      final SchemaTreeProvider provider, final UserSession userSession) {
    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(userSession.getCredentials().getUserName())
        .setProvider(newSchemaConfigInfoProvider(userSession, provider))
        .setIgnoreAuthErrors(true)
        .exposeSubSchemasAsTopLevelSchemas(true)
        .build();
    final SchemaPlus rootSchema = provider.getRootSchema(schemaConfig);
    return tableType.getRecordReader(userSession.getCatalogName(), rootSchema, filter);
  }

  /**
   * Helper method to create a {@link SchemaInfoProvider} instance for metadata purposes.
   * @param session
   * @return
   */
  private static SchemaInfoProvider newSchemaConfigInfoProvider(final UserSession session, final SchemaTreeProvider schemaTreeProvider) {
    return new SchemaInfoProvider() {
      private final ViewExpansionContext viewExpansionContext = new ViewExpansionContext(this, schemaTreeProvider, session.getCredentials().getUserName());

      @Override
      public ViewExpansionContext getViewExpansionContext() {
        return viewExpansionContext;
      }

      @Override
      public OptionValue getOption(String optionKey) {
        return session.getOptions().getOption(optionKey);
      }
    };
  }

  /**
   * Helper method to create {@link DremioPBError} for client response message.
   * @param failedFunction Brief description of the failed function.
   * @param ex Exception thrown
   * @return
   */
  public static DremioPBError createPBError(final String failedFunction, final Throwable ex) {
    final String errorId = UUID.randomUUID().toString();
    logger.error("Failed to {}. ErrorId: {}", failedFunction, errorId, ex);

    final DremioPBError.Builder builder = DremioPBError.newBuilder();
    builder.setErrorType(ErrorType.SYSTEM); // Metadata requests shouldn't cause any user errors
    builder.setErrorId(errorId);
    if (ex.getMessage() != null) {
      builder.setMessage(ex.getMessage());
    }

    builder.setException(ErrorHelper.getWrapper(ex));

    return builder.build();
  }
}
