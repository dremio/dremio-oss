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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.SearchTypes.SearchQuery;
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
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.SchemaMetadata;
import com.dremio.exec.proto.UserProtos.TableMetadata;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.ischema.tables.CatalogsTable.Catalog;
import com.dremio.exec.store.ischema.tables.ColumnsTable.Column;
import com.dremio.exec.store.ischema.tables.InfoSchemaTable;
import com.dremio.exec.store.ischema.tables.SchemataTable.Schema;
import com.dremio.exec.store.ischema.tables.TablesTable.Table;
import com.dremio.exec.work.protector.ResponseSenderHandler;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;

/**
 * Contains worker {@link Runnable} classes for providing the metadata and related helper methods.
 */
public class MetadataProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataProvider.class);

  /**
   * Super class for all metadata provider runnable classes.
   */
  public abstract static class MetadataCommand<R> implements CommandRunner<R> {
    protected final UserSession session;
    protected final SabotContext dContext;
    protected final NamespaceService namespace;
    protected final String catalogName;
    protected final InfoSchemaTable table;
    protected final MetadataProviderConditionBuilder builder;

    protected MetadataCommand(
        final UserSession session,
        final SabotContext dContext,
        final InfoSchemaTable table) {
      this.session = Preconditions.checkNotNull(session);
      this.dContext = Preconditions.checkNotNull(dContext);
      this.namespace = dContext.getNamespaceService(session.getCredentials().getUserName());
      this.catalogName = session.getCatalogName();
      this.table = table;
      this.builder = new MetadataProviderConditionBuilder();
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
      super(session, dContext, InfoSchemaTable.CATALOGS);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetCatalogsResp execute() throws Exception {
      final GetCatalogsResp.Builder respBuilder = GetCatalogsResp.newBuilder();

      final Predicate<String> catalogNamePred = req.hasCatalogNameFilter() ? builder.getLikePredicate(req.getCatalogNameFilter()) : Predicates.<String>alwaysTrue();
      final Iterable<Catalog> records = FluentIterable.<Catalog>from(table.<Catalog>asIterable(catalogName, namespace, null)).filter(new Predicate<Catalog>() {
        @Override
        public boolean apply(Catalog input) {
          return catalogNamePred.apply(input.CATALOG_NAME);
        }});

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
      super(session, dContext, InfoSchemaTable.SCHEMATA);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetSchemasResp execute() throws Exception {
      final GetSchemasResp.Builder respBuilder = GetSchemasResp.newBuilder();

      final SearchQuery filter = builder.createFilter(req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null, null);

      final Predicate<String> catalogNamePred = req.hasCatalogNameFilter() ? builder.getLikePredicate(req.getCatalogNameFilter()) : Predicates.<String>alwaysTrue();
      final Iterable<Schema> records = FluentIterable.<Schema>from(table.<Schema>asIterable(catalogName, namespace, filter)).filter(new Predicate<Schema>() {
        @Override
        public boolean apply(Schema input) {
          return catalogNamePred.apply(input.CATALOG_NAME);
        }});

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
      super(session, dContext, InfoSchemaTable.TABLES);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetTablesResp execute() throws Exception {
      final GetTablesResp.Builder respBuilder = GetTablesResp.newBuilder();

      final Predicate<String> catalogNamePred = req.hasCatalogNameFilter() ? builder.getLikePredicate(req.getCatalogNameFilter()) : Predicates.<String>alwaysTrue();
      final Predicate<String> tableTypeFilter = req.getTableTypeFilterCount() > 0 ? builder.getTableTypePredicate(req.getTableTypeFilterList()) : Predicates.<String>alwaysTrue();
      final SearchQuery filter = builder.createFilter(req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null, req.hasTableNameFilter() ? req.getTableNameFilter() : null);

      final Iterable<Table> records = FluentIterable.<Table>from(table.<Table>asIterable(catalogName, namespace, filter)).filter(new Predicate<Table>() {
        @Override
        public boolean apply(Table input) {
          return catalogNamePred.apply(input.TABLE_CATALOG) && tableTypeFilter.apply(input.TABLE_TYPE);
        }});

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
      super(session, dContext, InfoSchemaTable.COLUMNS);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = queryId;
    }

    @Override
    public GetColumnsResp execute() throws Exception {
      final GetColumnsResp.Builder respBuilder = GetColumnsResp.newBuilder();

      final Predicate<String> catalogNamePred = req.hasCatalogNameFilter() ? builder.getLikePredicate(req.getCatalogNameFilter()) : Predicates.<String>alwaysTrue();
      final Predicate<String> columnNameFilter = req.hasColumnNameFilter() ? builder.getLikePredicate(req.getColumnNameFilter()) : Predicates.<String>alwaysTrue();
      final SearchQuery filter = builder.createFilter(req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null, req.hasTableNameFilter() ? req.getTableNameFilter() : null);

      final Iterable<Column> records = FluentIterable.<Column>from(table.<Column>asIterable(catalogName, namespace, filter)).filter(new Predicate<Column>() {
        @Override
        public boolean apply(Column input) {
          return catalogNamePred.apply(input.TABLE_CATALOG) && columnNameFilter.apply(input.COLUMN_NAME);
        }});

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
