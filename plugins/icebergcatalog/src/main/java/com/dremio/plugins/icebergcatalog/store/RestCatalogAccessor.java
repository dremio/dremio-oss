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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_ALLOWED_NS_SEPARATOR;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.exec.store.iceberg.model.DremioBaseTable;
import com.dremio.options.OptionManager;
import com.dremio.service.users.SystemUser;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.DremioRESTTableOperations;
import org.apache.iceberg.rest.RESTCatalog;

public class RestCatalogAccessor implements CatalogAccessor {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RestCatalogAccessor.class);

  private final Configuration config;
  private final OptionManager optionsManager;
  private final LoadingCache<TableIdentifier, Table> tableCache;
  private final ExpiringCatalogCache catalogCache;
  private final Set<Namespace> allowedNamespaces;
  private final boolean isRecursiveAllowedNamespaces;

  public RestCatalogAccessor(
      Supplier<Catalog> catalogSupplier,
      Configuration config,
      OptionManager optionsManager,
      List<String> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces) {
    this.catalogCache =
        new ExpiringCatalogCache(
            Suppliers.compose(this::validateCatalogInstance, catalogSupplier),
            optionsManager.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS),
            TimeUnit.SECONDS);
    this.config = config;
    this.optionsManager = optionsManager;
    this.tableCache =
        Caffeine.newBuilder()
            .maximumSize(optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS))
            .expireAfterWrite(
                optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS),
                TimeUnit.SECONDS)
            .build(tableIdentifier -> catalogCache.get().loadTable(tableIdentifier));
    if (allowedNamespaces != null) {
      String separator = optionsManager.getOption(RESTCATALOG_ALLOWED_NS_SEPARATOR);
      this.allowedNamespaces =
          allowedNamespaces.stream()
              .filter(ns -> !ns.isEmpty())
              .map(s -> Namespace.of(s.split(separator)))
              .collect(Collectors.toSet());
      this.isRecursiveAllowedNamespaces = isRecursiveAllowedNamespaces;
    } else {
      this.allowedNamespaces = Sets.newHashSet(Namespace.empty());
      this.isRecursiveAllowedNamespaces = true;
    }
  }

  // Based on com.google.common.base.Suppliers.ExpiringMemoizingSupplier
  static class ExpiringCatalogCache implements Supplier<Catalog>, AutoCloseable {
    private final Object lock = new Object();
    private final Supplier<Catalog> catalogSupplier;
    private final long durationNanos;
    private volatile Catalog catalog;
    private volatile long expirationNanos;

    ExpiringCatalogCache(Supplier<Catalog> catalogSupplier, long duration, TimeUnit unit) {
      this(catalogSupplier, unit.toNanos(duration));
    }

    ExpiringCatalogCache(Supplier<Catalog> catalogSupplier, long durationNanos) {
      this.catalogSupplier = catalogSupplier;
      this.durationNanos = durationNanos;
      this.catalog = null;
      this.expirationNanos = 0;
    }

    public void invalidate() {
      expirationNanos = 0;
      if (catalog != null) {
        try {
          ((Closeable) catalog).close();
        } catch (Exception e) {
          logger.warn("Encountered exception during closing the catalog.");
        }
        catalog = null;
      }
    }

    public Supplier<Catalog> getCatalogSupplier() {
      return catalogSupplier;
    }

    @Override
    public Catalog get() {
      // double-checked locking
      long nanos = expirationNanos;
      long now = System.nanoTime();
      if (expirationNanos == 0 || now - expirationNanos >= 0) {
        synchronized (lock) {
          if (nanos == expirationNanos) {
            invalidate();
            catalog = catalogSupplier.get();
            expirationNanos = now + durationNanos + 1;
          }
        }
      }
      return catalog;
    }

    @Override
    public void close() {
      invalidate();
    }
  }

  protected Catalog validateCatalogInstance(Catalog catalogInstance) {
    Preconditions.checkArgument(
        catalogInstance instanceof RESTCatalog, "RESTCatalog instance expected");
    return catalogInstance;
  }

  private Table loadTable(TableIdentifier tableIdentifier) {
    if (optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)) {
      return tableCache.get(tableIdentifier);
    } else {
      return catalogCache.get().loadTable(tableIdentifier);
    }
  }

  @Override
  public void checkState() throws Exception {
    try (AutoCloseable ignore = (Closeable) catalogCache.getCatalogSupplier().get()) {
      return;
    } catch (Exception e) {
      catalogCache.invalidate();
      tableCache.invalidateAll();
      throw e;
    }
  }

  @Override
  public DatasetHandleListing listDatasetHandles(
      String rootName, SupportsIcebergRootPointer plugin) {
    return () ->
        streamTables(catalogCache.get(), allowedNamespaces, isRecursiveAllowedNamespaces)
            .map(
                tableIdentifier -> {
                  List<String> dataset = new ArrayList<>();
                  Collections.addAll(dataset, rootName);
                  Collections.addAll(dataset, tableIdentifier.namespace().levels());
                  Collections.addAll(dataset, tableIdentifier.name());
                  return getDatasetHandleInternal(dataset, tableIdentifier, plugin);
                })
            .iterator();
  }

  @VisibleForTesting
  static Stream<TableIdentifier> streamTables(
      Catalog catalog, Set<Namespace> allowedNamespaces, boolean isRecursiveAllowedNamespaces) {
    if (!isRecursiveAllowedNamespaces) {
      // we basically have a fixed set of namespaces to look for tables in
      return allowedNamespaces.stream().flatMap(ns -> streamCatalogTables(catalog, ns));
    } else {
      // we have a set of namespaces to start a recursive NS discovery from
      return allowedNamespaces.stream().flatMap(ns -> streamTablesRecursive(catalog, ns));
    }
  }

  private static Stream<TableIdentifier> streamTablesRecursive(
      Catalog catalogInstance, Namespace namespace) {
    return Stream.concat(
        streamCatalogNamespaces(catalogInstance, namespace)
            .flatMap(ns -> streamTablesRecursive(catalogInstance, ns)),
        streamCatalogTables(catalogInstance, namespace));
  }

  private static Stream<Namespace> streamCatalogNamespaces(
      Catalog catalogInstance, Namespace root) {
    try {
      return ((SupportsNamespaces) catalogInstance).listNamespaces(root).stream();
    } catch (Exception ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error listing namespace {}", root.toString(), ex);
      }
    }
    return Stream.empty();
  }

  private static Stream<TableIdentifier> streamCatalogTables(
      Catalog catalogInstance, Namespace namespace) {
    try {
      return catalogInstance.listTables(namespace).stream();
    } catch (Exception ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error listing tables in namespace {}", namespace.toString(), ex);
      }
    }
    return Stream.empty();
  }

  @Override
  public DatasetHandle getDatasetHandle(
      List<String> dataset, SupportsIcebergRootPointer plugin, GetDatasetOption... options) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    return getDatasetHandleInternal(dataset, tableIdentifier, plugin, options);
  }

  public DatasetHandle getDatasetHandleInternal(
      List<String> dataset,
      TableIdentifier tableIdentifier,
      SupportsIcebergRootPointer plugin,
      GetDatasetOption... options) {
    return new IcebergCatalogTableProvider(
        new EntityPath(dataset),
        () -> {
          Table baseTable = loadTable(tableIdentifier);
          // RESTSessionCatalog will provide a BaseTable for us with RESTTableOperations and
          // org.apache.iceberg.io.ResolvingFileIO as IO. We replace this with DremioFileIO
          // in order to provide our own FS constructs.
          try {
            DremioFileIO fileIO =
                (DremioFileIO)
                    plugin.createIcebergFileIO(
                        plugin.createFS(baseTable.location(), SystemUser.SYSTEM_USERNAME, null),
                        null,
                        dataset,
                        null,
                        null);
            return new DremioBaseTable(
                new DremioRESTTableOperations(
                    fileIO, ((HasTableOperations) baseTable).operations()),
                baseTable.name());
          } catch (IOException e) {
            throw UserException.ioExceptionError(e)
                .message("Error while trying to create DremioIO for %s", dataset)
                .buildSilently();
          } finally {
            baseTable.io().close();
          }
        },
        config,
        getSnapshotProvider(dataset, options),
        null, // TODO: forward 'this' once it becomes an IcebergMutablePlugin impl
        getSchemaProvider(options),
        optionsManager);
  }

  private TableSnapshotProvider getSnapshotProvider(
      List<String> dataset, GetDatasetOption... options) {
    TimeTravelOption timeTravelOption = TimeTravelOption.getTimeTravelOption(options);
    if (timeTravelOption != null
        && timeTravelOption.getTimeTravelRequest() instanceof TimeTravelOption.SnapshotIdRequest) {
      TimeTravelOption.SnapshotIdRequest snapshotIdRequest =
          (TimeTravelOption.SnapshotIdRequest) timeTravelOption.getTimeTravelRequest();
      final long snapshotId = Long.parseLong(snapshotIdRequest.getSnapshotId());
      return t -> {
        final Snapshot snapshot = t.snapshot(snapshotId);
        if (snapshot == null) {
          throw UserException.validationError()
              .message(
                  "For table '%s', the provided snapshot ID '%d' is invalid",
                  String.join(".", dataset), snapshotId)
              .buildSilently();
        }
        return snapshot;
      };
    } else {
      return Table::currentSnapshot;
    }
  }

  private TableSchemaProvider getSchemaProvider(GetDatasetOption... options) {
    TimeTravelOption timeTravelOption = TimeTravelOption.getTimeTravelOption(options);
    if (timeTravelOption != null
        && timeTravelOption.getTimeTravelRequest() instanceof TimeTravelOption.SnapshotIdRequest) {
      return (table, snapshot) -> {
        Integer schemaId = snapshot.schemaId();
        return schemaId != null ? table.schemas().get(schemaId) : table.schema();
      };
    } else {
      return (table, snapshot) -> table.schema();
    }
  }

  private static Namespace namespaceFromDataset(List<String> dataset, boolean hasTable) {
    Preconditions.checkNotNull(dataset);
    Preconditions.checkState(!dataset.isEmpty());
    int size = dataset.size();
    Preconditions.checkState(
        !hasTable || size >= 2,
        "with hasTable specified, expecting at least a source name and a table name");
    return Namespace.of(dataset.subList(1, hasTable ? size - 1 : size).toArray(new String[] {}));
  }

  private static TableIdentifier tableIdentifierFromDataset(List<String> dataset) {
    Namespace ns = namespaceFromDataset(dataset, true);
    return TableIdentifier.of(ns, dataset.get(dataset.size() - 1));
  }

  @Override
  public boolean datasetExists(List<String> dataset) {
    return catalogCache.get().tableExists(tableIdentifierFromDataset(dataset));
  }

  @Override
  public TableMetadata getTableMetadata(List<String> dataset) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    Table table = loadTable(tableIdentifier);
    Preconditions.checkState(table instanceof HasTableOperations);
    return ((HasTableOperations) table).operations().current();
  }

  @Override
  public void close() throws Exception {
    catalogCache.close();
    tableCache.invalidateAll();
    tableCache.cleanUp();
  }
}
