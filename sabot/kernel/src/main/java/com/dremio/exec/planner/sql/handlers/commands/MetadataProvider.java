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
package com.dremio.exec.planner.sql.handlers.commands;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.MetadataRequestOptions;
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
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.work.protector.ResponseSenderHandler;
import com.dremio.service.catalog.Catalog;
import com.dremio.service.catalog.InformationSchemaServiceGrpc.InformationSchemaServiceBlockingStub;
import com.dremio.service.catalog.ListCatalogsRequest;
import com.dremio.service.catalog.ListSchemataRequest;
import com.dremio.service.catalog.ListTableSchemataRequest;
import com.dremio.service.catalog.ListTablesRequest;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

/**
 * Contains worker {@link Runnable} classes for providing the metadata and related helper methods.
 */
public class MetadataProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataProvider.class);

  /**
   * Super class for all metadata provider runnable classes.
   */
  private abstract static class MetadataCommand<R> implements CommandRunner<R> {

    protected final InformationSchemaServiceBlockingStub catalogStub;
    protected final MetadataCommandParameters parameters;

    protected MetadataCommand(
      InformationSchemaServiceBlockingStub catalogStub,
      MetadataCommandParameters parameters
    ) {
      this.catalogStub = Preconditions.checkNotNull(catalogStub);
      this.parameters = Preconditions.checkNotNull(parameters);
    }

    @Override
    public double plan() {
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

    @Override
    public void close() {
      // no-op
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
      InformationSchemaServiceBlockingStub catalogStub,
      MetadataCommandParameters parameters,
      GetCatalogsReq req
    ) {
      super(catalogStub, parameters);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = parameters.getQueryId();
    }

    @Override
    public GetCatalogsResp execute() throws Exception {
      final Predicate<String> catalogNamePred = MetadataProviderConditions
        .getCatalogNamePredicate(req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null);

      final ListCatalogsRequest catalogsRequest = ListCatalogsRequest.newBuilder()
        .setUsername(parameters.getUsername())
        .build();

      final Iterator<Catalog> catalogs = catalogStub.listCatalogs(catalogsRequest);

      final List<CatalogMetadata> catalogMetadata =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(catalogs, Spliterator.ORDERED), false)
          .map(catalog -> MetadataProviderUtils.toCatalogMetadata(catalog, parameters.getCatalogName()))
          .filter(catalog -> catalogNamePred.test(catalog.getCatalogName()))
          .sorted(CATALOGS_ORDERING) // reorder results according to JDBC spec
          .collect(Collectors.toList());

      final GetCatalogsResp.Builder respBuilder = GetCatalogsResp.newBuilder();
      respBuilder.setQueryId(queryId);
      respBuilder.addAllCatalogs(catalogMetadata);
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
      }
    };

    private final GetSchemasReq req;
    private final QueryId queryId;

    public SchemasProvider(
      InformationSchemaServiceBlockingStub catalogStub,
      MetadataCommandParameters parameters,
      GetSchemasReq req
    ) {
      super(catalogStub, parameters);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = parameters.getQueryId();
    }

    @Override
    public GetSchemasResp execute() throws Exception {
      final Predicate<String> catalogNamePred = MetadataProviderConditions
        .getCatalogNamePredicate(req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null);

      final Optional<SearchQuery> searchQuery = MetadataProviderConditions
        .createConjunctiveQuery(req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null, null);

      final ListSchemataRequest.Builder requestBuilder = ListSchemataRequest.newBuilder()
        .setUsername(parameters.getUsername());
      searchQuery.ifPresent(requestBuilder::setQuery);

      final Iterator<Schema> schemata = catalogStub.listSchemata(requestBuilder.build());

      final List<SchemaMetadata> schemaMetadata =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(schemata, Spliterator.ORDERED), false)
          .map(schema -> MetadataProviderUtils.toSchemaMetadata(schema, parameters.getCatalogName()))
          .filter(schema -> catalogNamePred.test(schema.getCatalogName()))
          .sorted(SCHEMAS_ORDERING) // reorder results according to JDBC spec
          .collect(Collectors.toList());

      final GetSchemasResp.Builder respBuilder = GetSchemasResp.newBuilder();
      respBuilder.setQueryId(queryId);
      respBuilder.addAllSchemas(schemaMetadata);
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

    private final GetTablesReq req;
    private final QueryId queryId;

    public TablesProvider(
      InformationSchemaServiceBlockingStub catalogStub,
      MetadataCommandParameters parameters,
      GetTablesReq req
    ) {
      super(catalogStub, parameters);
      this.req = Preconditions.checkNotNull(req);
      this.queryId = parameters.getQueryId();
    }

    @Override
    public GetTablesResp execute() throws Exception {
      final Predicate<String> catalogNamePred = MetadataProviderConditions
        .getCatalogNamePredicate(req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null);
      final java.util.function.Predicate<String> tableTypePred = MetadataProviderConditions
        .getTableTypePredicate(req.getTableTypeFilterList());

      final Optional<SearchQuery> searchQuery = MetadataProviderConditions
        .createConjunctiveQuery(
          req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null,
          req.hasTableNameFilter() ? req.getTableNameFilter() : null);

      final ListTablesRequest.Builder requestBuilder = ListTablesRequest.newBuilder()
        .setUsername(parameters.getUsername());
      searchQuery.ifPresent(requestBuilder::setQuery);

      final Iterator<Table> tables = catalogStub.listTables(requestBuilder.build());

      Stream<TableMetadata> tableStream =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(tables, Spliterator.ORDERED), false)
          .map(table -> MetadataProviderUtils.toTableMetadata(table, parameters.getCatalogName()))
          .filter(table -> catalogNamePred.test(table.getCatalogName()) &&
            tableTypePred.test(table.getType()));

      if (parameters.getMaxMetadataCount() > 0) {
        tableStream = tableStream.limit(parameters.getMaxMetadataCount());
      }

      final List<TableMetadata> tableMetadata =
        tableStream.sorted(TABLES_ORDERING)  // reorder results according to JDBC/ODBC spec
          .collect(Collectors.toList());

      final GetTablesResp.Builder respBuilder = GetTablesResp.newBuilder();
      respBuilder.setQueryId(queryId);
      respBuilder.addAllTables(tableMetadata);
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

    private final CatalogService catalogService;
    private final GetColumnsReq req;
    private final QueryId queryId;

    public ColumnsProvider(
      InformationSchemaServiceBlockingStub catalogStub,
      MetadataCommandParameters parameters,
      CatalogService catalogService,
      GetColumnsReq req
    ) {
      super(catalogStub, parameters);
      this.catalogService = catalogService;
      this.req = Preconditions.checkNotNull(req);
      this.queryId = parameters.getQueryId();
    }

    @Override
    public GetColumnsResp execute() throws Exception {
      final Predicate<String> catalogNamePred = MetadataProviderConditions
        .getCatalogNamePredicate(req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null);
      final Predicate<String> columnNamePred = MetadataProviderConditions
        .getCatalogNamePredicate(req.hasColumnNameFilter() ? req.getColumnNameFilter() : null);

      final Optional<SearchQuery> searchQuery = MetadataProviderConditions
        .createConjunctiveQuery(
          req.hasSchemaNameFilter() ? req.getSchemaNameFilter() : null,
          req.hasTableNameFilter() ? req.getTableNameFilter() : null);

      final ListTableSchemataRequest.Builder requestBuilder = ListTableSchemataRequest.newBuilder()
        .setUsername(parameters.getUsername());
      searchQuery.ifPresent(requestBuilder::setQuery);

      Iterator<TableSchema> schemata = catalogStub.listTableSchemata(requestBuilder.build());
      final boolean hasUnfilteredResponses = schemata.hasNext();

      // if there are no responses and if the request is a point lookup, refresh inline and request again
      if (!hasUnfilteredResponses && req.hasSchemaNameFilter() && req.hasTableNameFilter()) {
        final NamespaceKey tableKey = getTableKeyFromFilter(req.getSchemaNameFilter(), req.getTableNameFilter());

        if (tableKey != null) {
          catalogService.getCatalog(MetadataRequestOptions.of(
            SchemaConfig.newBuilder(parameters.getUsername()).build()))
            .getTable(tableKey);

          schemata = catalogStub.listTableSchemata(requestBuilder.build());
        }
      }

      final List<ColumnMetadata> columnMetadata =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(schemata, Spliterator.ORDERED), false)
          .flatMap((Function<TableSchema, Stream<ColumnMetadata>>) tableSchema ->
            MetadataProviderUtils.toColumnMetadata(tableSchema, parameters.getCatalogName(), false))
          .filter(column ->
            catalogNamePred.test(column.getCatalogName()) &&
              columnNamePred.test(column.getColumnName()))
          .sorted(COLUMNS_ORDERING) // reorder results according to JDBC/ODBC spec
          .collect(Collectors.toList());

      final GetColumnsResp.Builder respBuilder = GetColumnsResp.newBuilder();
      respBuilder.setQueryId(queryId);
      respBuilder.addAllColumns(columnMetadata);
      respBuilder.setStatus(RequestStatus.OK);
      return respBuilder.build();
    }
  }

  /**
   * Helper method to create {@link DremioPBError} for client response message.
   *
   * @param failedFunction Brief description of the failed function.
   * @param ex             Exception thrown
   * @return pb error
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

  private static final String SQL_LIKE_SPECIALS = "_%";

  /**
   * Helper method to convert schema and table filters into NamespaceKey.
   * <p>
   * This method creates NamespaceKey only if there are no any wildcard characters.
   * Needed to be able to get metadata for a particular table if metadata is being queried.
   *
   * @param schemaFilter schema filter
   * @param tableFilter  table filter
   * @return NamespaceKey (can return null in case there are wildcard characters in any of the filters)
   */
  @VisibleForTesting
  static NamespaceKey getTableKeyFromFilter(LikeFilter schemaFilter, LikeFilter tableFilter) {
    final StringBuilder sb = new StringBuilder();
    final List<String> paths = Lists.newArrayList();
    final List<LikeFilter> filters = ImmutableList.of(schemaFilter, tableFilter);

    for (LikeFilter filter : filters) {
      final String pattern = filter.getPattern();
      if (pattern.isEmpty()) {
        // should have both schema and table filter
        return null;
      }
      final String escape = filter.getEscape();

      final char e = escape == null ? '\\' : escape.charAt(0);
      boolean escaped = false;

      for (int i = 0; i < pattern.length(); i++) {
        char c = pattern.charAt(i);

        if (escaped) {
          sb.append(c);
          escaped = false;
          continue;
        }

        if (c == e) {
          escaped = true;
          continue;
        }

        if (SQL_LIKE_SPECIALS.indexOf(c) >= 0) {
          // unable to deal with likecards
          return null;
        }
        sb.append(c);
      }
      paths.add(sb.toString());
      sb.setLength(0);
    }
    if (!paths.isEmpty()) {
      return new NamespaceKey(paths);
    }
    return null;
  }
}
