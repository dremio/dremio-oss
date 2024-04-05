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

package com.dremio.exec.catalog;

import com.dremio.datastore.indexed.IndexKey;
import com.dremio.exec.planner.sql.handlers.commands.MetadataProvider;
import com.dremio.exec.planner.sql.handlers.commands.MetadataProviderUtils;
import com.dremio.exec.proto.SearchProtos;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserProtos.ColumnMetadata;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.util.AdaptingServerCallStreamObserver;
import com.dremio.service.catalog.Catalog;
import com.dremio.service.catalog.InformationSchemaServiceGrpc;
import com.dremio.service.catalog.ListCatalogsRequest;
import com.dremio.service.catalog.ListSchemataRequest;
import com.dremio.service.catalog.ListSysCatalogsRequest;
import com.dremio.service.catalog.ListSysColumnsRequest;
import com.dremio.service.catalog.ListSysSchemasRequest;
import com.dremio.service.catalog.ListTableSchemataRequest;
import com.dremio.service.catalog.ListTablesRequest;
import com.dremio.service.catalog.ListViewsRequest;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.View;
import com.dremio.service.grpc.OnReadyHandler;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableMap;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of information schema service which adapts the RPC requests and responses to
 * Catalog API.
 */
public class InformationSchemaServiceImpl
    extends InformationSchemaServiceGrpc.InformationSchemaServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(InformationSchemaServiceImpl.class);

  private final Provider<Executor> executor;
  private final Provider<CatalogService> catalogService;

  public InformationSchemaServiceImpl(
      Provider<CatalogService> catalogService, Provider<Executor> executor) {
    this.catalogService = catalogService;
    this.executor = executor;
  }

  private static MetadataRequestOptions createRequestOptions(String username) {
    return MetadataRequestOptions.of(SchemaConfig.newBuilder(CatalogUser.from(username)).build());
  }

  @Override
  public void listCatalogs(ListCatalogsRequest request, StreamObserver<Catalog> responseObserver) {
    final ServerCallStreamObserver<Catalog> streamObserver =
        (ServerCallStreamObserver<Catalog>) responseObserver;

    final Iterator<Catalog> catalogs =
        catalogService
            .get()
            .getCatalog(createRequestOptions(request.getUsername()))
            .listCatalogs(request.hasQuery() ? request.getQuery() : null);

    final class ListCatalogs extends OnReadyHandler<Catalog> {
      ListCatalogs() {
        super(
            "list-catalogs",
            InformationSchemaServiceImpl.this.executor.get(),
            streamObserver,
            catalogs);
      }
    }

    final ListCatalogs listCatalogs = new ListCatalogs();
    streamObserver.setOnReadyHandler(listCatalogs);
    streamObserver.setOnCancelHandler(listCatalogs::cancel);
  }

  @Override
  public void listSchemata(ListSchemataRequest request, StreamObserver<Schema> responseObserver) {
    final ServerCallStreamObserver<Schema> streamObserver =
        (ServerCallStreamObserver<Schema>) responseObserver;

    final Iterator<Schema> schemata =
        catalogService
            .get()
            .getCatalog(createRequestOptions(request.getUsername()))
            .listSchemata(request.hasQuery() ? request.getQuery() : null);

    final class ListSchemata extends OnReadyHandler<Schema> {
      ListSchemata() {
        super(
            "list-schemata",
            InformationSchemaServiceImpl.this.executor.get(),
            streamObserver,
            schemata);
      }
    }

    final ListSchemata listSchemata = new ListSchemata();
    streamObserver.setOnReadyHandler(listSchemata);
    streamObserver.setOnCancelHandler(listSchemata::cancel);
  }

  @Override
  public void listTables(ListTablesRequest request, StreamObserver<Table> responseObserver) {
    final ServerCallStreamObserver<Table> streamObserver =
        (ServerCallStreamObserver<Table>) responseObserver;

    final Iterator<Table> tables =
        catalogService
            .get()
            .getCatalog(createRequestOptions(request.getUsername()))
            .listTables(request.hasQuery() ? request.getQuery() : null);

    final class ListTables extends OnReadyHandler<Table> {
      ListTables() {
        super(
            "list-tables",
            InformationSchemaServiceImpl.this.executor.get(),
            streamObserver,
            tables);
      }
    }

    final ListTables listTables = new ListTables();
    streamObserver.setOnReadyHandler(listTables);
    streamObserver.setOnCancelHandler(listTables::cancel);
  }

  @Override
  public void listViews(ListViewsRequest request, StreamObserver<View> responseObserver) {
    final ServerCallStreamObserver<View> streamObserver =
        (ServerCallStreamObserver<View>) responseObserver;

    final Iterator<View> views =
        catalogService
            .get()
            .getCatalog(createRequestOptions(request.getUsername()))
            .listViews(request.hasQuery() ? request.getQuery() : null);

    final class ListViews extends OnReadyHandler<View> {
      ListViews() {
        super(
            "list-views", InformationSchemaServiceImpl.this.executor.get(), streamObserver, views);
      }
    }

    final ListViews listViews = new ListViews();
    streamObserver.setOnReadyHandler(listViews);
    streamObserver.setOnCancelHandler(listViews::cancel);
  }

  @Override
  public void listTableSchemata(
      ListTableSchemataRequest request, StreamObserver<TableSchema> responseObserver) {
    final ServerCallStreamObserver<TableSchema> streamObserver =
        (ServerCallStreamObserver<TableSchema>) responseObserver;

    final Iterator<TableSchema> tableSchemata =
        catalogService
            .get()
            .getCatalog(createRequestOptions(request.getUsername()))
            .listTableSchemata(request.hasQuery() ? request.getQuery() : null);

    final class ListTableSchemata extends OnReadyHandler<TableSchema> {
      ListTableSchemata() {
        super(
            "list-table-schemata",
            InformationSchemaServiceImpl.this.executor.get(),
            streamObserver,
            tableSchemata);
      }
    }

    final ListTableSchemata listTableSchemata = new ListTableSchemata();
    streamObserver.setOnReadyHandler(listTableSchemata);
    streamObserver.setOnCancelHandler(listTableSchemata::cancel);
  }

  @Override
  public void listSysCatalogs(
      ListSysCatalogsRequest request, StreamObserver<UserProtos.CatalogMetadata> responseObserver) {
    logger.info("Received listSysCatalogs request {}", request);
    Function<Catalog, UserProtos.CatalogMetadata> toCatalogMetadataConverter =
        catalog -> MetadataProviderUtils.toCatalogMetadata(catalog, "");
    SearchQuery searchQuery = toSearchQuery(request.getQuery());
    ListCatalogsRequest.Builder builder =
        ListCatalogsRequest.newBuilder().setUsername(request.getUsername());
    if (searchQuery != null) {
      builder.setQuery(searchQuery);
    }
    AdaptingServerCallStreamObserver<Catalog, UserProtos.CatalogMetadata> catalogsSCSO =
        new AdaptingServerCallStreamObserver<>(
            (ServerCallStreamObserver<UserProtos.CatalogMetadata>) responseObserver,
            toCatalogMetadataConverter);
    listCatalogs(builder.build(), catalogsSCSO);
  }

  @Override
  public void listSysSchemas(
      ListSysSchemasRequest request, StreamObserver<UserProtos.SchemaMetadata> responseObserver) {
    logger.info("Received listSysSchemas request {}", request);
    Function<Schema, UserProtos.SchemaMetadata> toSchemaMetadataConverter =
        schema -> MetadataProviderUtils.toSchemaMetadata(schema, "");
    SearchQuery searchQuery = toSearchQuery(request.getQuery());
    ListSchemataRequest.Builder builder =
        ListSchemataRequest.newBuilder().setUsername(request.getUsername());
    if (searchQuery != null) {
      builder.setQuery(searchQuery);
    }
    AdaptingServerCallStreamObserver<Schema, UserProtos.SchemaMetadata> schemaSCSO =
        new AdaptingServerCallStreamObserver<>(
            (ServerCallStreamObserver<UserProtos.SchemaMetadata>) responseObserver,
            toSchemaMetadataConverter);
    listSchemata(builder.build(), schemaSCSO);
  }

  @Override
  public void listSysColumns(
      ListSysColumnsRequest request, StreamObserver<ColumnMetadata> responseObserver) {
    logger.info("Received listSysColumns request {}", request);
    SearchQuery searchQuery = toSearchQuery(request.getQuery());
    listTableSchemata(searchQuery, request.getUsername(), responseObserver)
        .exceptionally(
            ex -> {
              if (ex instanceof NoSuchElementException) {
                // if there are no responses and if the request is a point lookup, refresh inline
                // and request again
                SearchQuery schemaNameQuery =
                    getFieldFilter(
                        DatasetIndexKeys.UNQUOTED_SCHEMA.getIndexFieldName(), searchQuery);
                SearchQuery tableNameQuery =
                    getFieldFilter(DatasetIndexKeys.UNQUOTED_NAME.getIndexFieldName(), searchQuery);
                if (schemaNameQuery != null && tableNameQuery != null) {
                  final NamespaceKey tableKey =
                      MetadataProvider.getTableKeyFromFilter(schemaNameQuery, tableNameQuery);
                  if (tableKey != null) {
                    catalogService
                        .get()
                        .getCatalog(
                            MetadataRequestOptions.of(
                                SchemaConfig.newBuilder(CatalogUser.from(request.getUsername()))
                                    .build()))
                        .getTable(tableKey);
                    listTableSchemata(searchQuery, request.getUsername(), responseObserver)
                        .exceptionally(
                            th -> {
                              if (th instanceof NoSuchElementException) {
                                responseObserver.onCompleted();
                              }
                              return null;
                            });
                  } else {
                    responseObserver.onCompleted();
                  }
                } else {
                  responseObserver.onCompleted();
                }
              }
              return null;
            });
  }

  private SearchQuery getFieldFilter(String fieldName, SearchQuery searchQuery) {
    if (searchQuery == null) {
      return null;
    }
    if ((searchQuery.hasLike() && searchQuery.getLike().getField().equals(fieldName))
        || (searchQuery.hasEquals() && searchQuery.getEquals().getField().equals(fieldName))) {
      return searchQuery;
    }
    if (searchQuery.hasAnd()) {
      for (SearchQuery query : searchQuery.getAnd().getClausesList()) {
        if ((query.hasLike() && query.getLike().getField().equals(fieldName))
            || (query.hasEquals() && query.getEquals().getField().equals(fieldName))) {
          return query;
        }
      }
    }
    return null;
  }

  /**
   * Helper method for listSysColumns api, calls listTableSchemata api internally and sends a stream
   * of ColumnMetadata in responseObserver on each table schema found. sends {@link
   * NoSuchElementException} in completable future if no table schema found
   */
  private CompletableFuture<Void> listTableSchemata(
      SearchQuery searchQuery, String userName, StreamObserver<ColumnMetadata> responseObserver) {
    final ListTableSchemataRequest.Builder builder =
        ListTableSchemataRequest.newBuilder().setUsername(userName);
    if (searchQuery != null) {
      builder.setQuery(searchQuery);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    final class ColumnsServerCallStreamObserver
        extends AdaptingServerCallStreamObserver<TableSchema, ColumnMetadata> {
      private boolean tableSchemaFound = false;

      public ColumnsServerCallStreamObserver(
          ServerCallStreamObserver<ColumnMetadata> serverCallStreamObserver) {
        super(serverCallStreamObserver, null);
      }

      @Override
      public void onNext(TableSchema value) {
        tableSchemaFound = true;
        ServerCallStreamObserver<ColumnMetadata> serverCallStreamObserver =
            getDelegateServerCallStreamObserver();
        (MetadataProviderUtils.toColumnMetadata(value, null, false))
            .forEach(
                (columnMetadata) -> {
                  synchronized (serverCallStreamObserver) {
                    serverCallStreamObserver.onNext(columnMetadata);
                  }
                });
      }

      @Override
      public void onError(Throwable th) {
        logger.error("Exception in listTableSchemata call", th);
        getDelegateServerCallStreamObserver().onError(th);
        future.completeExceptionally(th);
      }

      @Override
      public void onCompleted() {
        if (!tableSchemaFound) {
          future.completeExceptionally(new NoSuchElementException());
          return;
        }
        getDelegateServerCallStreamObserver().onCompleted();
        future.complete(null);
      }
    }
    listTableSchemata(
        builder.build(),
        new ColumnsServerCallStreamObserver(
            (ServerCallStreamObserver<ColumnMetadata>) responseObserver));
    return future;
  }

  /**
   * Convert SearchProtos.SearchQuery(received in system table requests) to Indexed Catalog
   * SearchQuery. UnIndexed fields are ignored
   */
  private static SearchQuery toSearchQuery(SearchProtos.SearchQuery query) {
    SearchQuery catalogSearchQuery = null;
    try {
      if (query.hasEquals()) {
        IndexKey indexKey = FIELDS.get(query.getEquals().getField());
        if (indexKey == null) {
          return null;
        }
        SearchQuery.Equals.Builder builder =
            SearchQuery.Equals.newBuilder().setField(indexKey.getIndexFieldName());
        if (query.getEquals().hasStringValue()) {
          builder.setStringValue(query.getEquals().getStringValue());
        } else {
          builder.setIntValue(query.getEquals().getIntValue());
        }
        catalogSearchQuery = SearchQuery.newBuilder().setEquals(builder.build()).build();
      } else if (query.hasLike()) {
        IndexKey indexKey = FIELDS.get(query.getLike().getField());
        if (indexKey == null) {
          return null;
        }
        SearchQuery.Like.Builder builder =
            SearchQuery.Like.newBuilder()
                .setField(indexKey.getIndexFieldName())
                .setPattern(query.getLike().getPattern())
                .setEscape(query.getLike().getEscape())
                .setCaseInsensitive(query.getLike().getCaseInsensitive());
        catalogSearchQuery = SearchQuery.newBuilder().setLike(builder.build()).build();
      } else if (query.hasAnd()) {
        SearchQuery.And.Builder builder = SearchQuery.And.newBuilder();
        query
            .getAnd()
            .getClausesList()
            .forEach(
                (currQuery) -> {
                  SearchQuery currCatalogQuery = toSearchQuery(currQuery);
                  if (currCatalogQuery != null) {
                    builder.addClauses(currCatalogQuery);
                  }
                });
        SearchQuery.And searchQueryList = builder.build();
        return searchQueryList.getClausesList().size() != 0
            ? SearchQuery.newBuilder().setAnd(searchQueryList).build()
            : null;
      } else if (query.hasOr()) {
        SearchQuery.Or.Builder builder = SearchQuery.Or.newBuilder();
        for (SearchProtos.SearchQuery currQuery : query.getOr().getClausesList()) {
          SearchQuery currSearchQuery = toSearchQuery(currQuery);
          if (currSearchQuery == null) {
            return null;
          }
          builder.addClauses(currSearchQuery);
        }
        SearchQuery.Or or = builder.build();
        return or.getClausesList().size() != 0 ? SearchQuery.newBuilder().setOr(or).build() : null;
      }
    } catch (Exception e) {
      logger.error("Exception in search query conversion for query {}", query, e);
    }
    return catalogSearchQuery;
  }

  /** Mapping of information schema system table fields to indexed kvstore fields */
  private static final ImmutableMap<String, IndexKey> FIELDS =
      ImmutableMap.of(
          "TABLE_SCHEMA".toLowerCase(Locale.ROOT), DatasetIndexKeys.UNQUOTED_SCHEMA,
          "TABLE_NAME".toLowerCase(Locale.ROOT), DatasetIndexKeys.UNQUOTED_NAME,
          "SCHEMA_NAME".toLowerCase(Locale.ROOT), DatasetIndexKeys.UNQUOTED_SCHEMA);
}
