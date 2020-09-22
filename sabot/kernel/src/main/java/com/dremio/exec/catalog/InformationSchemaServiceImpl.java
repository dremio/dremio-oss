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

import java.util.Iterator;
import java.util.concurrent.Executor;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.SerializedExecutor;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.catalog.Catalog;
import com.dremio.service.catalog.InformationSchemaServiceGrpc;
import com.dremio.service.catalog.ListCatalogsRequest;
import com.dremio.service.catalog.ListSchemataRequest;
import com.dremio.service.catalog.ListTableSchemataRequest;
import com.dremio.service.catalog.ListTablesRequest;
import com.dremio.service.catalog.ListViewsRequest;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.View;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of information schema service which adapts the RPC requests and responses to Catalog API.
 */
public class InformationSchemaServiceImpl extends InformationSchemaServiceGrpc.InformationSchemaServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(InformationSchemaServiceImpl.class);

  private final Provider<Executor> executor;
  private final Provider<CatalogService> catalogService;

  public InformationSchemaServiceImpl(Provider<CatalogService> catalogService, Provider<Executor> executor) {
    this.catalogService = catalogService;
    this.executor = executor;
  }

  private static MetadataRequestOptions createRequestOptions(String username) {
    return MetadataRequestOptions.of(SchemaConfig.newBuilder(username).build());
  }

  @Override
  public void listCatalogs(ListCatalogsRequest request, StreamObserver<Catalog> responseObserver) {
    final ServerCallStreamObserver<Catalog> streamObserver = (ServerCallStreamObserver<Catalog>) responseObserver;

    final Iterator<Catalog> catalogs = catalogService.get()
      .getCatalog(createRequestOptions(request.getUsername()))
      .listCatalogs(request.hasQuery() ? request.getQuery() : null);

    final class ListCatalogs extends OnReadyHandler<Catalog> {
      ListCatalogs() {
        super("list-catalogs", InformationSchemaServiceImpl.this.executor.get(), streamObserver, catalogs);
      }
    }

    final ListCatalogs listCatalogs = new ListCatalogs();
    streamObserver.setOnReadyHandler(listCatalogs);
    streamObserver.setOnCancelHandler(listCatalogs::cancel);
  }

  @Override
  public void listSchemata(ListSchemataRequest request, StreamObserver<Schema> responseObserver) {
    final ServerCallStreamObserver<Schema> streamObserver = (ServerCallStreamObserver<Schema>) responseObserver;

    final Iterator<Schema> schemata = catalogService.get()
      .getCatalog(createRequestOptions(request.getUsername()))
      .listSchemata(request.hasQuery() ? request.getQuery() : null);

    final class ListSchemata extends OnReadyHandler<Schema> {
      ListSchemata() {
        super("list-schemata", InformationSchemaServiceImpl.this.executor.get(), streamObserver, schemata);
      }
    }

    final ListSchemata listSchemata = new ListSchemata();
    streamObserver.setOnReadyHandler(listSchemata);
    streamObserver.setOnCancelHandler(listSchemata::cancel);
  }

  @Override
  public void listTables(ListTablesRequest request, StreamObserver<Table> responseObserver) {
    final ServerCallStreamObserver<Table> streamObserver = (ServerCallStreamObserver<Table>) responseObserver;

    final Iterator<Table> tables = catalogService.get()
      .getCatalog(createRequestOptions(request.getUsername()))
      .listTables(request.hasQuery() ? request.getQuery() : null);

    final class ListTables extends OnReadyHandler<Table> {
      ListTables() {
        super("list-tables", InformationSchemaServiceImpl.this.executor.get(), streamObserver, tables);
      }
    }

    final ListTables listTables = new ListTables();
    streamObserver.setOnReadyHandler(listTables);
    streamObserver.setOnCancelHandler(listTables::cancel);
  }

  @Override
  public void listViews(ListViewsRequest request, StreamObserver<View> responseObserver) {
    final ServerCallStreamObserver<View> streamObserver = (ServerCallStreamObserver<View>) responseObserver;

    final Iterator<View> views = catalogService.get()
      .getCatalog(createRequestOptions(request.getUsername()))
      .listViews(request.hasQuery() ? request.getQuery() : null);

    final class ListViews extends OnReadyHandler<View> {
      ListViews() {
        super("list-views", InformationSchemaServiceImpl.this.executor.get(), streamObserver, views);
      }
    }

    final ListViews listViews = new ListViews();
    streamObserver.setOnReadyHandler(listViews);
    streamObserver.setOnCancelHandler(listViews::cancel);
  }

  @Override
  public void listTableSchemata(ListTableSchemataRequest request, StreamObserver<TableSchema> responseObserver) {
    final ServerCallStreamObserver<TableSchema> streamObserver =
      (ServerCallStreamObserver<TableSchema>) responseObserver;

    final Iterator<TableSchema> tableSchemata = catalogService.get()
      .getCatalog(createRequestOptions(request.getUsername()))
      .listTableSchemata(request.hasQuery() ? request.getQuery() : null);

    final class ListTableSchemata extends OnReadyHandler<TableSchema> {
      ListTableSchemata() {
        super("list-table-schemata", InformationSchemaServiceImpl.this.executor.get(), streamObserver, tableSchemata);
      }
    }

    final ListTableSchemata listTableSchemata = new ListTableSchemata();
    streamObserver.setOnReadyHandler(listTableSchemata);
    streamObserver.setOnCancelHandler(listTableSchemata::cancel);
  }

  /**
   * Abstract handler which is invoked every time the peer is ready to receive more messages.
   *
   * @param <V> response type
   */
  private static abstract class OnReadyHandler<V> implements Runnable {
    private final OnReadyEventExecutor executor;
    private final ServerCallStreamObserver<V> responseObserver;

    private volatile Iterator<V> iterator;

    private OnReadyHandler(
      String requestType,
      Executor executor,
      ServerCallStreamObserver<V> streamObserver,
      Iterator<V> iterator
    ) {
      this.executor = new OnReadyEventExecutor(requestType, executor);
      this.responseObserver = streamObserver;

      this.iterator = iterator;
    }

    void cancel() {
      // clear state
      iterator = null;
    }

    @Override
    public void run() {
      if (iterator == null) {
        logger.debug("Callback was invoked even though last message was sent");
        return;
      }

      if (!responseObserver.isReady()) {
        // see CallStreamObserver#setOnReadyHandler
        // handle spurious notifications: although handled in handleStreamReady, this avoids volatile reads
        return;
      }

      executor.execute(this::handleStreamReady);
    }

    private void handleStreamReady() {
      // Every run try to send as many responses as the client is willing to receive. This also handles
      // cancellation and depleting enqueued requests in SerializedExecutor#queuedRunnables.

      Iterator<V> iterator = this.iterator;
      int numSent = 0;

      while (responseObserver.isReady() && iterator != null && iterator.hasNext()) {
        try {
          responseObserver.onNext(iterator.next());
        } catch (Exception e) {
          responseObserver.onError(e);
          this.iterator = null;
          return;
        }

        numSent++;
        if (numSent == 500) { // refresh iterator
          iterator = this.iterator;
          numSent = 0;
        }
      }

      iterator = this.iterator;
      if (iterator != null && !iterator.hasNext()) {
        responseObserver.onCompleted();
        this.iterator = null;
      }
    }

    /**
     * Serializes execution of {@code #onReady} events, and offloads request handling.
     * <p>
     * This ensures there is no write contention (including errors).
     */
    private final class OnReadyEventExecutor extends SerializedExecutor<Runnable> {

      private OnReadyEventExecutor(String requestType, Executor underlyingExecutor) {
        super(requestType, underlyingExecutor, false);
      }

      @Override
      protected void runException(Runnable command, Throwable t) {
        responseObserver.onError(t);
        iterator = null;
      }
    }
  }
}
