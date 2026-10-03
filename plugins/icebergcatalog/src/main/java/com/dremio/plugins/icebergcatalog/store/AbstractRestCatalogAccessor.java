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

import static com.dremio.exec.catalog.CatalogOptions.RESTCATALOG_VIEWS_SUPPORTED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_ALLOWED_NS_SEPARATOR;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.options.ForceUpdateOption;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.SupportsFsCreation;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.TimeTravelProcessors;
import com.dremio.exec.store.iceberg.model.DremioBaseTable;
import com.dremio.options.OptionManager;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.DremioRESTTableOperations;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;

public abstract class AbstractRestCatalogAccessor implements CatalogAccessor {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AbstractRestCatalogAccessor.class);

  private final OptionManager optionsManager;
  private final LoadingCache<CatalogAccessorTableCacheKey, Table> tableCache;
  private final LoadingCache<CatalogAccessorTableCacheKey, View> viewCache;
  private final Supplier<Catalog> icebergCatalogSupplier;
  private final Set<Namespace> allowedNamespaces;

  /**
   * The allowed namespaces that discovery starts from: with recursive discovery, an entry whose
   * ancestor is also allowed is dropped, since the ancestor's walk already covers it (otherwise its
   * datasets would be listed, and loaded during metadata refresh, twice).
   */
  private final Set<Namespace> discoveryRoots;

  private final boolean isRecursiveAllowedNamespaces;

  /**
   * Masks the source's secret values in text that the catalog server provided (error messages are
   * copied into user errors, job profiles and logs).
   */
  private final UnaryOperator<String> redactor;

  public static final String DEFAULT_BASE_LOCATION = "default-base-location";

  /**
   * Schema of the staged creation used by {@link #loadTableStorageProperties} for a table that does
   * not exist yet. Any valid schema does: the staged table is never committed.
   */
  private static final Schema STAGED_CREATE_SCHEMA =
      new Schema(Types.NestedField.optional(1, "placeholder", Types.StringType.get()));

  public AbstractRestCatalogAccessor(
      Supplier<Catalog> catalogSupplier,
      OptionManager optionsManager,
      @Nullable List<String> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces) {
    this(
        catalogSupplier,
        optionsManager,
        allowedNamespaces,
        isRecursiveAllowedNamespaces,
        UnaryOperator.identity());
  }

  public AbstractRestCatalogAccessor(
      Supplier<Catalog> catalogSupplier,
      OptionManager optionsManager,
      @Nullable List<String> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces,
      UnaryOperator<String> redactor) {
    this.optionsManager = optionsManager;
    this.redactor = Preconditions.checkNotNull(redactor);
    this.icebergCatalogSupplier = catalogSupplier;
    this.tableCache =
        Caffeine.newBuilder()
            .maximumSize(optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS))
            .expireAfterWrite(
                optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS),
                TimeUnit.SECONDS)
            .build(
                catalogAccessorTableCacheKey -> {
                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Catalog table cache: cache miss for user {} on table {}",
                        catalogAccessorTableCacheKey.userId(),
                        catalogAccessorTableCacheKey.tableIdentifier());
                  }
                  return getCatalog().loadTable(catalogAccessorTableCacheKey.tableIdentifier());
                });
    this.viewCache =
        Caffeine.newBuilder()
            .maximumSize(optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS))
            .expireAfterWrite(
                optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS),
                TimeUnit.SECONDS)
            .build(
                userViewIdentifier -> {
                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Catalog view cache: cache miss for user {} on view {}",
                        userViewIdentifier.userId(),
                        userViewIdentifier.tableIdentifier());
                  }
                  return ((ViewCatalog) getCatalog())
                      .loadView(userViewIdentifier.tableIdentifier());
                });
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
    this.discoveryRoots =
        this.isRecursiveAllowedNamespaces
            ? dropCoveredNamespaces(this.allowedNamespaces)
            : this.allowedNamespaces;
  }

  /** Keeps only the namespaces that no other namespace of the set is a proper ancestor of. */
  @VisibleForTesting
  static Set<Namespace> dropCoveredNamespaces(Set<Namespace> namespaces) {
    Set<Namespace> roots = new HashSet<>();
    for (Namespace ns : namespaces) {
      boolean covered = false;
      for (Namespace other : namespaces) {
        if (isProperAncestor(other, ns)) {
          covered = true;
          break;
        }
      }
      if (!covered) {
        roots.add(ns);
      }
    }
    return roots;
  }

  private static boolean isProperAncestor(Namespace ancestor, Namespace ns) {
    return ancestor.length() < ns.length()
        && Arrays.equals(ancestor.levels(), Arrays.copyOf(ns.levels(), ancestor.length()));
  }

  protected Catalog getCatalog() {
    return icebergCatalogSupplier.get();
  }

  @VisibleForTesting
  Table loadTable(TableIdentifier tableIdentifier, GetDatasetOption... options) {
    try {
      return loadTableInternal(tableIdentifier, options);
    } catch (ForbiddenException e) {
      throw forbidden(e, "load table", bracket(tableIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "load table", bracket(tableIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "load table", bracket(tableIdentifier));
    }
  }

  private Table loadTableInternal(TableIdentifier tableIdentifier, GetDatasetOption... options) {
    if (optionsManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)) {
      UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);
      if (userContext == null) {
        logger.warn("Missing user context when loading table");
      }
      String userId = userContext == null ? "" : userContext.getUserId();
      CatalogAccessorTableCacheKey catalogAccessorTableCacheKey =
          new CatalogAccessorTableCacheKey(userId, tableIdentifier);
      if (ForceUpdateOption.isForceUpdate(options)) {
        invalidateTableCacheForAllUsers(tableIdentifier);
      }
      return tableCache.get(catalogAccessorTableCacheKey);
    } else {
      return getCatalog().loadTable(tableIdentifier);
    }
  }

  @VisibleForTesting
  @Override
  public View loadView(TableIdentifier tableIdentifier, GetDatasetOption... options) {
    if (!viewsEnabled()) {
      throw UserException.unsupportedError()
          .message("Views are not supported in this catalog.")
          .buildSilently();
    }
    try {
      return loadViewInternal(tableIdentifier, options);
    } catch (ForbiddenException e) {
      throw forbidden(e, "load view", bracket(tableIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "load view", bracket(tableIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "load view", bracket(tableIdentifier));
    }
  }

  private View loadViewInternal(TableIdentifier tableIdentifier, GetDatasetOption... options) {
    if (optionsManager.getOption(RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED)) {
      UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);
      if (userContext == null) {
        logger.warn("Missing user context when loading view");
      }
      String userId = userContext == null ? "" : userContext.getUserId();
      CatalogAccessorTableCacheKey catalogAccessorTableCacheKey =
          new CatalogAccessorTableCacheKey(userId, tableIdentifier);
      if (ForceUpdateOption.isForceUpdate(options)) {
        invalidateViewCacheForAllUsers(tableIdentifier);
      }
      return viewCache.get(catalogAccessorTableCacheKey);
    } else {
      return ((ViewCatalog) getCatalog()).loadView(tableIdentifier);
    }
  }

  @Override
  public void checkState() throws Exception {
    try {
      checkStateInternal();
    } catch (Exception e) {
      if (invalidatesCachesOnFailure(e)) {
        invalidateCachedTablesAndViews();
      }
      throw e;
    }
  }

  /**
   * Whether a failed {@link #checkState()} drops the cached tables and views. True by default;
   * subclasses return false for failures that do not affect them (e.g. a denied listing of the
   * catalog root, which the source may report as healthy and which then occurs on every check).
   */
  protected boolean invalidatesCachesOnFailure(Exception failure) {
    return true;
  }

  /**
   * Drops the cached tables and views. Required whenever the catalog client is replaced: their
   * table and view operations are bound to the client that loaded them.
   */
  protected void invalidateCachedTablesAndViews() {
    tableCache.invalidateAll();
    viewCache.invalidateAll();
  }

  /** Masks the source's secret values in server-provided text. */
  protected String redact(@Nullable String text) {
    return text == null ? null : redactor.apply(text);
  }

  private UserException forbidden(ForbiddenException e, String action, Object entity) {
    return RestCatalogExceptionMapper.forbidden(e, action, entity, redactor);
  }

  private UserException notAuthorized(NotAuthorizedException e, String action, Object entity) {
    return RestCatalogExceptionMapper.notAuthorized(e, action, entity, redactor);
  }

  private Throwable redactedForLogging(Throwable e) {
    return RestCatalogExceptionMapper.redactedForLogging(e, redactor);
  }

  private UserException requestFailed(RESTException e, String action, Object entity) {
    return RestCatalogExceptionMapper.requestFailed(e, action, entity, redactor);
  }

  @Override
  public Set<TableIdentifier> listDatasetIdentifiers(List<String> pathWithRootName) {
    if (pathWithRootName.isEmpty()) {
      logger.error("Received empty path to list");
      throw new IllegalArgumentException("Path to list must not be empty");
    }
    Namespace namespaceToListFrom = namespaceFromPath(pathWithRootName);
    if (allowedNamespaces.equals(ImmutableSet.of(Namespace.empty()))
        || allowedNamespaces.contains(namespaceToListFrom)) {
      return Stream.concat(
              streamTables(
                  getCatalog(), ImmutableSet.of(namespaceToListFrom), isRecursiveAllowedNamespaces),
              streamViews(
                  getCatalog(), ImmutableSet.of(namespaceToListFrom), isRecursiveAllowedNamespaces))
          .collect(Collectors.toSet());
    }
    return ImmutableSet.of();
  }

  @Override
  public DatasetHandleListing listDatasetHandles(
      String rootName, SupportsIcebergRootPointer plugin) {
    Stream<DatasetHandle> tableStream =
        streamTables(getCatalog(), discoveryRoots, isRecursiveAllowedNamespaces)
            .map(
                tableIdentifier -> {
                  List<String> dataset = new ArrayList<>();
                  Collections.addAll(dataset, rootName);
                  Collections.addAll(dataset, tableIdentifier.namespace().levels());
                  Collections.addAll(dataset, tableIdentifier.name());
                  return getTableHandleInternal(dataset, tableIdentifier, plugin);
                });
    Stream<DatasetHandle> viewStream =
        viewsEnabled()
            ? streamViews(getCatalog(), discoveryRoots, isRecursiveAllowedNamespaces)
                .map(
                    viewIdentifier -> {
                      List<String> dataset = new ArrayList<>();
                      Collections.addAll(dataset, rootName);
                      Collections.addAll(dataset, viewIdentifier.namespace().levels());
                      Collections.addAll(dataset, viewIdentifier.name());
                      return getViewHandleInternal(dataset, viewIdentifier, plugin);
                    })
            : Stream.empty();

    return Stream.concat(tableStream, viewStream)::iterator;
  }

  @VisibleForTesting
  Stream<TableIdentifier> streamTables(
      Catalog catalog, Set<Namespace> allowedNamespaces, boolean isRecursiveAllowedNamespaces) {
    if (!isRecursiveAllowedNamespaces) {
      // we basically have a fixed set of namespaces to look for tables in
      return allowedNamespaces.stream().flatMap(ns -> streamCatalogTables(catalog, ns));
    } else {
      // we have a set of namespaces to start a recursive NS discovery from
      return allowedNamespaces.stream().flatMap(ns -> streamTablesRecursive(catalog, ns));
    }
  }

  private Stream<TableIdentifier> streamTablesRecursive(
      Catalog catalogInstance, Namespace namespace) {
    return Stream.concat(
        streamCatalogNamespaces(catalogInstance, namespace)
            .flatMap(ns -> streamTablesRecursive(catalogInstance, ns)),
        streamCatalogTables(catalogInstance, namespace));
  }

  private Stream<Namespace> streamCatalogNamespaces(Catalog catalogInstance, Namespace root) {
    try {
      return ((SupportsNamespaces) catalogInstance).listNamespaces(root).stream();
    } catch (NoSuchNamespaceException | ForbiddenException ex) {
      // Expected for an allowed namespace that does not exist (any more) or that the principal may
      // not list; logged on every metadata refresh, so without a stack trace. The message comes
      // from the catalog server: redacted and abbreviated.
      logger.warn(
          "Skipping namespace {} while listing: {}",
          root,
          RestCatalogExceptionMapper.redactedServerMessage(ex, redactor));
    } catch (Exception ex) {
      // Logged on every metadata refresh: one line with the redacted server message, the stack
      // trace at DEBUG only.
      logger.warn(
          "Error listing namespace {}: {}",
          root,
          RestCatalogExceptionMapper.redactedServerMessage(ex, redactor));
      logger.debug("Error listing namespace {}", root, redactedForLogging(ex));
    }
    return Stream.empty();
  }

  protected Stream<TableIdentifier> streamCatalogTables(
      Catalog catalogInstance, Namespace namespace) {
    try {
      return catalogInstance.listTables(namespace).stream();
    } catch (Exception ex) {
      logger.debug("Error listing tables in namespace {}", namespace, redactedForLogging(ex));
    }
    return Stream.empty();
  }

  @VisibleForTesting
  Stream<TableIdentifier> streamViews(
      Catalog catalog, Set<Namespace> allowedNamespaces, boolean isRecursiveAllowedNamespaces) {
    if (!isRecursiveAllowedNamespaces) {
      // we basically have a fixed set of namespaces to look for tables in
      return allowedNamespaces.stream().flatMap(ns -> streamCatalogViews(catalog, ns));
    } else {
      // we have a set of namespaces to start a recursive NS discovery from
      return allowedNamespaces.stream().flatMap(ns -> streamViewsRecursive(catalog, ns));
    }
  }

  private Stream<TableIdentifier> streamViewsRecursive(
      Catalog catalogInstance, Namespace namespace) {
    return Stream.concat(
        streamCatalogNamespaces(catalogInstance, namespace)
            .flatMap(ns -> streamViewsRecursive(catalogInstance, ns)),
        streamCatalogViews(catalogInstance, namespace));
  }

  protected Stream<TableIdentifier> streamCatalogViews(
      Catalog catalogInstance, Namespace namespace) {
    try {
      return ((ViewCatalog) catalogInstance).listViews(namespace).stream();
    } catch (Exception ex) {
      logger.debug("Error listing views in namespace {}", namespace, redactedForLogging(ex));
    }
    return Stream.empty();
  }

  @VisibleForTesting
  DatasetHandle getViewHandleInternal(
      List<String> viewPath,
      TableIdentifier viewIdentifier,
      SupportsIcebergRootPointer plugin,
      GetDatasetOption... options) {
    return new IcebergCatalogViewProvider(
        new EntityPath(viewPath), () -> plugin.loadViewMetadata(viewIdentifier, options));
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(
      List<String> dataset, SupportsIcebergRootPointer plugin, GetDatasetOption... options) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    try {
      if (getCatalog().tableExists(tableIdentifier)) {
        return Optional.of(getTableHandleInternal(dataset, tableIdentifier, plugin, options));
      }

      if (viewsEnabled() && ((ViewCatalog) getCatalog()).viewExists(tableIdentifier)) {
        return Optional.of(getViewHandleInternal(dataset, tableIdentifier, plugin, options));
      }
    } catch (ForbiddenException e) {
      throw forbidden(e, "look up", bracket(tableIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "look up", bracket(tableIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "look up", bracket(tableIdentifier));
    }

    logger.warn("DatasetHandle '{}' not found - table or view not found.", dataset);
    return Optional.empty();
  }

  @VisibleForTesting
  public DatasetHandle getTableHandleInternal(
      List<String> dataset,
      TableIdentifier tableIdentifier,
      SupportsIcebergRootPointer plugin,
      GetDatasetOption... options) {
    TimeTravelOption.TimeTravelRequest timeTravelRequest =
        Optional.ofNullable(TimeTravelOption.getTimeTravelOption(options))
            .map(TimeTravelOption::getTimeTravelRequest)
            .orElse(null);
    return new IcebergCatalogTableProvider(
        new EntityPath(dataset),
        () -> {
          Table baseTable = loadTable(tableIdentifier, options);
          // RESTSessionCatalog will provide a BaseTable for us with RESTTableOperations and
          // org.apache.iceberg.io.ResolvingFileIO as IO. We replace this with DremioFileIO
          // in order to provide our own FS constructs.
          try {
            DremioFileIO fileIO =
                (DremioFileIO)
                    plugin.createIcebergFileIO(
                        plugin.createFS(
                            SupportsFsCreation.builder()
                                .filePath(baseTable.location())
                                .withSystemUserName()
                                .withSystemUserId()
                                .dataset(dataset)),
                        null,
                        dataset,
                        null,
                        null);
            return new DremioBaseTable(
                new DremioRESTTableOperations(
                    fileIO, ((HasTableOperations) baseTable).operations()),
                baseTable.name());
          } catch (Exception e) {
            throw UserException.ioExceptionError(e)
                .message("Error while trying to access file system.")
                .buildSilently();
          } finally {
            baseTable.io().close();
          }
        },
        TimeTravelProcessors.getTableSnapshotProvider(dataset, timeTravelRequest),
        TimeTravelProcessors.getTableSchemaProvider(timeTravelRequest),
        optionsManager);
  }

  private static Namespace namespaceFromDataset(List<String> dataset) {
    Preconditions.checkNotNull(dataset);
    Preconditions.checkState(!dataset.isEmpty());
    int size = dataset.size();
    Preconditions.checkState(size >= 3, "A dataset must only be created underneath of a folder.");
    return Namespace.of(dataset.subList(1, size - 1).toArray(new String[] {}));
  }

  protected static TableIdentifier tableIdentifierFromDataset(List<String> dataset) {
    Namespace ns = namespaceFromDataset(dataset);
    return TableIdentifier.of(ns, dataset.get(dataset.size() - 1));
  }

  @Override
  public boolean datasetExists(List<String> dataset) {
    try {
      return getCatalog().tableExists(tableIdentifierFromDataset(dataset))
          || (viewsEnabled()
              && ((ViewCatalog) getCatalog()).viewExists(tableIdentifierFromDataset(dataset)));
    } catch (IllegalStateException e) {
      // This happens if the table identifier created is incorrect.
      return false;
    } catch (BadRequestException e) {
      // if the path to the dataset is wrong we get a BadRequestException containing "Invalid
      // request path" message. If this is the case, just ignore it and return false.
      if (e.getMessage().contains("Invalid request path")) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public boolean namespaceExists(List<String> namespace) {
    return ((SupportsNamespaces) getCatalog()).namespaceExists(namespaceFromPath(namespace));
  }

  @Override
  public TableMetadata getTableMetadata(List<String> dataset) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    Table table = loadTable(tableIdentifier);
    Preconditions.checkState(table instanceof HasTableOperations);
    return ((HasTableOperations) table).operations().current();
  }

  @Override
  public ViewMetadata getViewMetadata(List<String> dataset) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    BaseView baseView = (BaseView) loadView(tableIdentifier);
    return baseView.operations().current();
  }

  /**
   * Loads the table directly from the catalog (not through the per-user table cache: this runs
   * wherever a file system for the table is created, also without a user context) and returns the
   * properties of the FileIO that the Iceberg REST client built for it from the catalog's response.
   */
  @Override
  public Map<String, String> loadTableStorageProperties(
      TableIdentifier tableIdentifier, boolean stageNewTable) {
    Catalog catalog = getCatalog();
    try {
      try {
        return ioProperties(catalog.loadTable(tableIdentifier).io());
      } catch (NoSuchTableException e) {
        if (!stageNewTable) {
          return Collections.emptyMap();
        }
        // CTAS writes the data files of a new table before it creates the table in the catalog
        // (staged create, then commit). A staged creation at the table's default location, which
        // CTAS uses unless it is given a LOCATION, returns the credentials for that location. It
        // is discarded: nothing is committed, so the catalog does not create the table.
        try {
          return ioProperties(
              catalog
                  .newCreateTableTransaction(tableIdentifier, STAGED_CREATE_SCHEMA)
                  .table()
                  .io());
        } catch (NoSuchNamespaceException missingNamespace) {
          return Collections.emptyMap();
        } catch (AlreadyExistsException createdMeanwhile) {
          return ioProperties(catalog.loadTable(tableIdentifier).io());
        }
      }
    } catch (ForbiddenException e) {
      throw forbidden(e, "load the storage credentials of table", bracket(tableIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "load the storage credentials of table", bracket(tableIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "load the storage credentials of table", bracket(tableIdentifier));
    }
  }

  private static Map<String, String> ioProperties(FileIO io) {
    try {
      Map<String, String> properties = io.properties();
      return properties == null ? Collections.emptyMap() : properties;
    } catch (UnsupportedOperationException e) {
      // A custom io-impl that does not expose its configuration.
      return Collections.emptyMap();
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(
      IcebergCatalogTableProvider icebergTableProvider, ListPartitionChunkOption[] options) {
    return icebergTableProvider.listPartitionChunks(options);
  }

  @Override
  public DatasetMetadata getTableMetadata(
      IcebergCatalogTableProvider icebergTableProvider, GetMetadataOption[] options) {
    return icebergTableProvider.getDatasetMetadata(options);
  }

  @Override
  public DatasetMetadata getViewMetadata(DatasetHandle viewHandle) {
    return viewHandle.unwrap(IcebergCatalogViewProvider.class);
  }

  boolean viewsEnabled() {
    return optionsManager.getOption(RESTCATALOG_VIEWS_SUPPORTED);
  }

  /**
   * Invalidates all table cache entries for the specified TableIdentifier, regardless of the
   * userId. This is useful when you need to invalidate a table for all users (e.g., when table
   * metadata changes).
   *
   * @param tableIdentifier the table identifier to invalidate for all users
   */
  public void invalidateTableCacheForAllUsers(TableIdentifier tableIdentifier) {
    if (tableIdentifier == null) {
      throw new IllegalArgumentException("TableIdentifier cannot be null");
    }

    // Get all cache keys and filter for matching table identifiers
    Set<CatalogAccessorTableCacheKey> keysToInvalidate =
        tableCache.asMap().keySet().stream()
            .filter(key -> key.tableIdentifier().equals(tableIdentifier))
            .collect(Collectors.toSet());

    // Invalidate all matching keys
    tableCache.invalidateAll(keysToInvalidate);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Invalidated {} table cache entries for table: {}",
          keysToInvalidate.size(),
          tableIdentifier);
    }
  }

  /**
   * Invalidates all view cache entries for the specified TableIdentifier, regardless of the userId.
   * This is useful when you need to invalidate a view for all users (e.g., when view metadata
   * changes).
   *
   * @param tableIdentifier the table identifier to invalidate for all users
   */
  public void invalidateViewCacheForAllUsers(TableIdentifier tableIdentifier) {
    if (tableIdentifier == null) {
      throw new IllegalArgumentException("TableIdentifier cannot be null");
    }

    // Get all cache keys and filter for matching table identifiers
    Set<CatalogAccessorTableCacheKey> keysToInvalidate =
        viewCache.asMap().keySet().stream()
            .filter(key -> key.tableIdentifier().equals(tableIdentifier))
            .collect(Collectors.toSet());

    // Invalidate all matching keys
    viewCache.invalidateAll(keysToInvalidate);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Invalidated {} view cache entries for view: {}",
          keysToInvalidate.size(),
          tableIdentifier);
    }
  }

  @Override
  public void close() throws Exception {
    if (icebergCatalogSupplier instanceof Closeable) {
      ((Closeable) icebergCatalogSupplier).close();
    }
    tableCache.invalidateAll();
    tableCache.cleanUp();
  }

  protected abstract void checkStateInternal() throws Exception;

  @Override
  public Stream<IcebergNamespaceWithProperties> getFolderStream() {
    return streamNamespaceWithPropertiesWithRoot(
        (SupportsNamespaces) getCatalog(), discoveryRoots, isRecursiveAllowedNamespaces);
  }

  /**
   * Lists the folders of the source: the allowed namespaces, the namespaces discovered below them
   * and every ancestor of a listed namespace, each namespace once and every parent before its
   * children.
   *
   * <p>Both properties matter to Dremio's names refresh ({@code
   * SourceMetadataManager#handleFolderListing}): it deletes every known folder that the listing
   * does not contain, together with its content, so an allowed namespace {@code a.b} needs its
   * parent {@code a} in the listing too; and it only recognizes a listed folder as existing once
   * its parent was listed, otherwise it deletes the folder as "no longer found". Ancestors are
   * listed without properties (no request, and no privilege on them needed) unless they are
   * discovered anyway; so is an allowed namespace whose properties could not be loaded but whose
   * children were listed. The whole-catalog listing is already in that order and stays lazy.
   */
  private Stream<IcebergNamespaceWithProperties> streamNamespaceWithPropertiesWithRoot(
      SupportsNamespaces catalog,
      Set<Namespace> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces) {
    if (allowedNamespaces.size() == 1 && allowedNamespaces.contains(Namespace.empty())) {
      return streamNamespaceWithProperties(
          catalog, allowedNamespaces, isRecursiveAllowedNamespaces);
    }
    Map<Namespace, IcebergNamespaceWithProperties> folders = new LinkedHashMap<>();
    getNamespacesWithProperties(catalog, allowedNamespaces)
        .forEach(folder -> folders.putIfAbsent(folder.getNamespace(), folder));
    streamNamespaceWithProperties(catalog, allowedNamespaces, isRecursiveAllowedNamespaces)
        .forEach(folder -> folders.putIfAbsent(folder.getNamespace(), folder));
    // Every proper ancestor of a listed folder that is not listed itself: not allowed, or allowed
    // but its properties could not be loaded (e.g. the principal may not read them).
    for (Namespace ancestor : missingAncestorsOf(folders.keySet())) {
      folders.put(ancestor, new IcebergNamespaceWithProperties(ancestor, Collections.emptyMap()));
    }
    // Stable sort: parents (shorter namespaces) first, discovery order otherwise.
    return folders.values().stream()
        .sorted(Comparator.comparingInt(folder -> folder.getNamespace().length()));
  }

  private static Set<Namespace> missingAncestorsOf(Set<Namespace> listed) {
    Set<Namespace> missing = new LinkedHashSet<>();
    for (Namespace namespace : listed) {
      String[] levels = namespace.levels();
      for (int i = 1; i < levels.length; i++) {
        Namespace ancestor = Namespace.of(Arrays.copyOf(levels, i));
        if (!listed.contains(ancestor)) {
          missing.add(ancestor);
        }
      }
    }
    return missing;
  }

  private Stream<IcebergNamespaceWithProperties> streamNamespaceWithProperties(
      SupportsNamespaces catalog,
      Set<Namespace> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces) {
    if (!isRecursiveAllowedNamespaces) {
      return allowedNamespaces.stream()
          .flatMap(ns -> getNamespacesWithProperties(catalog, getSubNamespaces(catalog, ns)));
    }
    return allowedNamespaces.stream()
        .flatMap(ns -> streamNamespaceWithPropertiesRecursive(catalog, ns));
  }

  private Stream<IcebergNamespaceWithProperties> streamNamespaceWithPropertiesRecursive(
      SupportsNamespaces catalog, Namespace namespace) throws NoSuchNamespaceException {
    List<Namespace> namespaces = getSubNamespaces(catalog, namespace);
    if (namespaces.isEmpty()) {
      return Stream.empty();
    }
    return Stream.concat(
        getNamespacesWithProperties(catalog, namespaces),
        namespaces.stream().flatMap(ns -> streamNamespaceWithPropertiesRecursive(catalog, ns)));
  }

  private List<Namespace> getSubNamespaces(SupportsNamespaces catalog, Namespace namespace) {
    try {
      return catalog.listNamespaces(namespace);
    } catch (NoSuchNamespaceException e) {
      logMissingNamespace(namespace);
      return Collections.emptyList();
    } catch (Exception e) {
      logNamespaceListingError(namespace, e);
      return Collections.emptyList();
    }
  }

  /**
   * An allowed namespace that does not exist, or one deleted while refreshing metadata. Logged on
   * every metadata refresh, so without a stack trace.
   */
  private static void logMissingNamespace(Namespace namespace) {
    logger.warn(
        "Namespace {} does not exist (it is configured as an allowed namespace but missing in"
            + " the catalog, or it was deleted while refreshing metadata).",
        namespace);
  }

  private void logNamespaceListingError(Namespace namespace, Exception e) {
    logger.debug(
        "Error listing namespace {}. This could occur if we are not authorized to access it.",
        namespace,
        redactedForLogging(e));
  }

  private Stream<IcebergNamespaceWithProperties> getNamespacesWithProperties(
      SupportsNamespaces catalog, Collection<Namespace> namespaces) {
    return namespaces.stream()
        .map(ns -> getNamespaceWithProperties(catalog, ns).orElse(null))
        .filter(Objects::nonNull);
  }

  private Optional<IcebergNamespaceWithProperties> getNamespaceWithProperties(
      SupportsNamespaces catalog, Namespace namespace) throws NoSuchNamespaceException {
    try {
      return Optional.of(
          new IcebergNamespaceWithProperties(namespace, catalog.loadNamespaceMetadata(namespace)));
    } catch (NoSuchNamespaceException e) {
      logMissingNamespace(namespace);
      return Optional.empty();
    } catch (Exception e) {
      logNamespaceListingError(namespace, e);
      return Optional.empty();
    }
  }

  // SupportsIcebergDatasetCUD Methods - START

  @Override
  public Table createTable(
      List<String> tablePathComponents,
      Schema schema,
      PartitionSpec partitionSpec,
      SortOrder sortOrder,
      @Nullable String location,
      Map<String, String> tableProperties) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(tablePathComponents);

    try {
      return getCatalog()
          .buildTable(tableIdentifier, schema)
          .withPartitionSpec(partitionSpec)
          .withSortOrder(sortOrder)
          .withLocation(location)
          .withProperties(tableProperties)
          .create();
    } catch (ForbiddenException e) {
      throw forbidden(e, "create table", bracket(tableIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "create table", bracket(tableIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "create table", bracket(tableIdentifier));
    } catch (NoSuchNamespaceException e) {
      throw RestCatalogExceptionMapper.parentNamespaceNotFound(e, tableIdentifier);
    } catch (AlreadyExistsException e) {
      throw RestCatalogExceptionMapper.alreadyExists(e, tableIdentifier);
    }
  }

  @Override
  public View createView(
      List<String> viewPathComponents,
      @Nullable String location,
      List<String> workspaceSchemaPath,
      Schema schema,
      String sql)
      throws AlreadyExistsException, NoSuchNamespaceException {
    TableIdentifier viewIdentifier = tableIdentifierFromDataset(viewPathComponents);
    Preconditions.checkArgument(!viewPathComponents.isEmpty(), "View path cannot be empty.");
    try {
      if (((ViewCatalog) getCatalog()).viewExists(viewIdentifier)) {
        throw new AlreadyExistsException(
            "View [%s] already exists with the source.", viewIdentifier);
      }
      return getViewBuilder(
              viewIdentifier, schema, location, workspaceSchemaPath, viewPathComponents.get(0), sql)
          .create();
    } catch (ForbiddenException e) {
      throw forbidden(e, "create view", bracket(viewIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "create view", bracket(viewIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "create view", bracket(viewIdentifier));
    }
  }

  @Override
  public void dropView(List<String> viewPathComponents) throws NoSuchViewException {
    TableIdentifier viewIdentifier = tableIdentifierFromDataset(viewPathComponents);
    try {
      ((ViewCatalog) getCatalog()).dropView(viewIdentifier);
    } catch (ForbiddenException e) {
      throw forbidden(e, "drop view", bracket(viewIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "drop view", bracket(viewIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "drop view", bracket(viewIdentifier));
    }
  }

  @Override
  public View updateView(
      List<String> viewPathComponents,
      @Nullable String location,
      List<String> workSchemaPath,
      Schema schema,
      String sql)
      throws NoSuchViewException {
    TableIdentifier viewIdentifier = tableIdentifierFromDataset(viewPathComponents);
    // TODO(DX-99998) Add retryer
    try {
      if (!((ViewCatalog) getCatalog()).viewExists(viewIdentifier)) {
        throw new NoSuchViewException("Cannot find View [%s] in the source.", viewIdentifier);
      }
      return getViewBuilder(
              viewIdentifier, schema, location, workSchemaPath, viewPathComponents.get(0), sql)
          .replace();
    } catch (ForbiddenException e) {
      throw forbidden(e, "replace view", bracket(viewIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "replace view", bracket(viewIdentifier));
    } catch (RESTException e) {
      throw requestFailed(e, "replace view", bracket(viewIdentifier));
    }
  }

  private String getRootOfWorkspaceSchemaPath(List<String> workspaceSchemaPath) {
    if (workspaceSchemaPath != null && !workspaceSchemaPath.isEmpty()) {
      return workspaceSchemaPath.get(0);
    }
    return null;
  }

  private String deriveDefaultCatalog(String parentCatalogName, List<String> workspaceSchemaPath) {
    String root = getRootOfWorkspaceSchemaPath(workspaceSchemaPath);
    if (root != null && !root.equals(parentCatalogName)) {
      return root;
    }
    return null;
  }

  private Namespace getNamespaceFromSchemaPath(List<String> workspaceSchemaPath) {
    if (workspaceSchemaPath.isEmpty()) {
      return Namespace.empty();
    }
    List<String> schemaPath = workspaceSchemaPath.subList(1, workspaceSchemaPath.size());
    return Namespace.of(schemaPath.toArray(new String[schemaPath.size()]));
  }

  private ViewBuilder getViewBuilder(
      TableIdentifier identifier,
      Schema schema,
      @Nullable String location,
      List<String> workspaceSchemaPath,
      String catalog,
      String sql) {
    String resolvedLocation = getViewLocation(location, identifier);

    return ((ViewCatalog) getCatalog())
        .buildView(identifier)
        .withSchema(schema)
        .withLocation(resolvedLocation)
        .withDefaultNamespace(getNamespaceFromSchemaPath(workspaceSchemaPath))
        .withDefaultCatalog(deriveDefaultCatalog(catalog, workspaceSchemaPath))
        .withQuery(IcebergViewMetadata.SupportedViewDialectsForRead.DREMIOSQL.toString(), sql);
  }

  @VisibleForTesting
  String getViewLocation(@Nullable String location, TableIdentifier identifier) {
    if (location != null) {
      return location;
    }

    ViewCatalog viewCatalog = (ViewCatalog) getCatalog();
    if (viewCatalog.viewExists(identifier)) {
      return viewCatalog.loadView(identifier).location();
    }

    SupportsNamespaces namespaceCatalog = (SupportsNamespaces) getCatalog();
    return getDatasetLocationFromExistingNamespaceLocationUri(identifier, namespaceCatalog);
  }

  /**
   * Drops the table from the catalog without purging its data (what Spark does by default for
   * external tables).
   *
   * @throws NoSuchTableException when the catalog has no such table (or no such namespace): the
   *     Iceberg REST client reports an HTTP 404 for the table by returning false
   */
  @Override
  public void dropTable(List<String> dataset) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    boolean dropped;
    try {
      dropped = getCatalog().dropTable(tableIdentifier, false);
    } catch (NoSuchNamespaceException e) {
      dropped = false;
    } catch (ForbiddenException e) {
      throw forbidden(e, "drop table", bracket(tableIdentifier));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "drop table", bracket(tableIdentifier));
    } catch (RESTException e) {
      // Not requestFailed: an HTTP 400/422 must not become a validation error, which DROP TABLE IF
      // EXISTS would report as "not found" while the table still exists.
      throw RestCatalogExceptionMapper.dropFailed(
          e, "drop table", bracket(tableIdentifier), redactor);
    }
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
    }
  }

  @Override
  public TableOperations createIcebergTableOperations(
      FileIO fileIO, List<String> dataset, @Nullable String userName, @Nullable String userId) {
    return new ForbiddenMappingTableOperations(
        (DremioFileIO) fileIO, tableOperationsHelper(dataset), dataset, redactor);
  }

  protected TableOperations tableOperationsHelper(List<String> dataset) {
    final TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    final Table table = loadTable(tableIdentifier);
    return ((HasTableOperations) table).operations();
  }

  @Override
  public TableOperations createIcebergTableOperationsForCtas(
      FileIO fileIO,
      List<String> dataset,
      Schema schema,
      @Nullable String userName,
      @Nullable String userId) {
    final TableOperations stagedCreate;
    try {
      // Stages the table creation in the catalog (e.g. Apache Polaris CREATE_TABLE_STAGED).
      stagedCreate = tableOperationsHelperForCtas(dataset, schema);
    } catch (ForbiddenException e) {
      throw forbidden(e, "create table", bracket(tableIdentifierFromDataset(dataset)));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "create table", bracket(tableIdentifierFromDataset(dataset)));
    } catch (RESTException e) {
      throw requestFailed(e, "create table", bracket(tableIdentifierFromDataset(dataset)));
    }
    final TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    return new ForbiddenMappingTableOperations(
        (DremioFileIO) fileIO,
        stagedCreate,
        dataset,
        redactor,
        metadata -> stagedCreateOperations(tableIdentifier, metadata));
  }

  /**
   * Stages the creation of a table with the partition spec, sort order, location and properties of
   * the metadata that CTAS commits.
   *
   * <p>CTAS first stages the table with only its schema and then commits fresh table metadata
   * ({@code IcebergBaseCommand#beginCreateTableTransaction}). The Iceberg REST client sends the
   * changes of the staged table (unpartitioned spec 0, set as default) followed by the changes of
   * the committed metadata. The latter add the partition spec but do not set it as the default,
   * because a new table's metadata builder already starts with default spec id 0. The catalog then
   * creates the table with the partition spec as spec 1 and keeps the unpartitioned spec 0 as the
   * default (seen with Apache Polaris). Staging with the committed spec avoids that: spec 0 is the
   * partition spec, and the committed changes reuse it.
   */
  protected TableOperations stagedCreateOperations(
      TableIdentifier tableIdentifier, TableMetadata metadata) {
    final Transaction transaction =
        getCatalog()
            .buildTable(tableIdentifier, metadata.schema())
            .withPartitionSpec(metadata.spec())
            .withSortOrder(metadata.sortOrder())
            .withLocation(metadata.location())
            .withProperties(metadata.properties())
            .createTransaction();
    Preconditions.checkState(
        transaction instanceof BaseTransaction, "Error - Plugin does not support this operation.");
    return ((BaseTransaction) transaction).underlyingOps();
  }

  protected TableOperations tableOperationsHelperForCtas(List<String> dataset, Schema schema) {
    final TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    final Transaction transaction = createTableTransactionForNewTable(tableIdentifier, schema);

    Preconditions.checkState(
        transaction instanceof BaseTransaction, "Error - Plugin does not support this operation.");
    return ((BaseTransaction) transaction).underlyingOps();
  }

  @Override
  public @Nullable String getDatasetLocationFromExistingNamespaceLocationUri(List<String> dataset) {
    TableIdentifier tableIdentifier = tableIdentifierFromDataset(dataset);
    SupportsNamespaces namespaceCatalog = (SupportsNamespaces) getCatalog();

    return getDatasetLocationFromExistingNamespaceLocationUri(tableIdentifier, namespaceCatalog);
  }

  private static @Nullable String getDatasetLocationFromExistingNamespaceLocationUri(
      TableIdentifier tableIdentifier, SupportsNamespaces namespaceCatalog) {
    String locationUri;
    try {
      locationUri =
          namespaceCatalog.loadNamespaceMetadata(tableIdentifier.namespace()).get("location");
    } catch (NoSuchNamespaceException e) {
      logger.warn("There was no namespace at {}.", tableIdentifier.namespace().toString(), e);
      return null;
    }
    if (locationUri != null && !locationUri.isBlank()) {
      return PathUtils.removeTrailingSlash(locationUri) + '/' + tableIdentifier.name();
    }

    // The location can be null if there is no location in the namespace metadata, however we do not
    // throw an error because the Iceberg spec will accept a null location.
    return null;
  }

  // SupportsIcebergDatasetCUD Methods - END

  // SupportsIcebergFolderCUD Methods - START

  @Override
  public Map<String, String> createFolder(
      List<String> folderPathWithSourceName, Map<String, String> properties)
      throws AlreadyExistsException {
    Namespace namespace = namespaceFromPath(folderPathWithSourceName);
    SupportsNamespaces supportsNamespaces = (SupportsNamespaces) getCatalog();
    try {
      supportsNamespaces.createNamespace(namespace, properties);
      return supportsNamespaces.loadNamespaceMetadata(namespace);
    } catch (ForbiddenException e) {
      throw forbidden(e, "create folder", bracket(namespace));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "create folder", bracket(namespace));
    } catch (RESTException e) {
      throw requestFailed(e, "create folder", bracket(namespace));
    } catch (NoSuchNamespaceException e) {
      throw RestCatalogExceptionMapper.parentNamespaceNotFound(e, namespace);
    }
  }

  @Override
  public Map<String, String> updateFolder(
      List<String> folderPathWithSourceName,
      Map<String, String> propertiesToUpdate,
      Set<String> propertiesToRemove)
      throws NoSuchNamespaceException {
    Namespace namespace = namespaceFromPath(folderPathWithSourceName);
    SupportsNamespaces supportsNamespaces = (SupportsNamespaces) getCatalog();
    if (!propertiesToUpdate.isEmpty()) {
      supportsNamespaces.setProperties(namespace, propertiesToUpdate);
    }
    if (!propertiesToRemove.isEmpty()) {
      supportsNamespaces.removeProperties(namespace, propertiesToRemove);
    }
    return supportsNamespaces.loadNamespaceMetadata(namespace);
  }

  @Override
  public boolean dropFolder(List<String> folderPathWithSourceName)
      throws NamespaceNotEmptyException {
    Namespace namespace = namespaceFromPath(folderPathWithSourceName);
    SupportsNamespaces supportsNamespaces = (SupportsNamespaces) getCatalog();
    final boolean dropped;
    try {
      dropped = supportsNamespaces.dropNamespace(namespace);
    } catch (BadRequestException e) {
      // HTTP 409 (NamespaceNotEmptyException, as defined by the Iceberg REST spec) is passed
      // through. Some catalogs (e.g. Apache Polaris 1.x) answer HTTP 400 "Namespace ... is not
      // empty" instead: reported the same way, so callers handle one exception type.
      if (RestCatalogExceptionMapper.isNamespaceNotEmpty(e)) {
        throw new NamespaceNotEmptyException(e, "Namespace %s is not empty", namespace);
      }
      throw requestFailed(e, "drop folder", bracket(namespace));
    } catch (ForbiddenException e) {
      throw forbidden(e, "drop folder", bracket(namespace));
    } catch (NotAuthorizedException e) {
      throw notAuthorized(e, "drop folder", bracket(namespace));
    } catch (RESTException e) {
      throw requestFailed(e, "drop folder", bracket(namespace));
    }
    if (!dropped) {
      // RESTSessionCatalog#dropNamespace returns false only when the server answers 404.
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    return true;
  }

  // SupportsIcebergFolderCUD Methods - END

  private static String bracket(Object entity) {
    return "[" + entity + "]";
  }

  /**
   * Table operations that report an HTTP 403 or 401 on commit as a permission error instead of a
   * system error (see {@link CommitForbiddenException} and {@link CommitNotAuthorizedException}).
   * Other commit failures (e.g. {@code CommitFailedException} on HTTP 409) are left to the Iceberg
   * commit path, which already reports them as concurrent modification errors.
   */
  @VisibleForTesting
  static final class ForbiddenMappingTableOperations extends DremioRESTTableOperations {
    private final List<String> dataset;
    private final UnaryOperator<String> redactor;
    // CTAS only: stages the creation again with the metadata to commit (see
    // stagedCreateOperations).
    @Nullable private final Function<TableMetadata, TableOperations> restagedCreate;
    @Nullable private volatile TableOperations committedCreate;

    ForbiddenMappingTableOperations(
        DremioFileIO dremioFileIO,
        TableOperations delegate,
        List<String> dataset,
        UnaryOperator<String> redactor) {
      this(dremioFileIO, delegate, dataset, redactor, null);
    }

    ForbiddenMappingTableOperations(
        DremioFileIO dremioFileIO,
        TableOperations delegate,
        List<String> dataset,
        UnaryOperator<String> redactor,
        @Nullable Function<TableMetadata, TableOperations> restagedCreate) {
      super(dremioFileIO, delegate);
      this.dataset = dataset;
      this.redactor = redactor;
      this.restagedCreate = restagedCreate;
    }

    @Override
    public TableMetadata current() {
      final TableOperations created = committedCreate;
      return created != null ? created.current() : super.current();
    }

    @Override
    public TableMetadata refresh() {
      final TableOperations created = committedCreate;
      return created != null ? created.refresh() : super.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      try {
        if (base == null && restagedCreate != null && metadata.spec().isPartitioned()) {
          // A partitioned CTAS: commit through a creation staged with its partition spec, or the
          // catalog keeps the staged unpartitioned spec as the default.
          final TableOperations created = restagedCreate.apply(metadata);
          created.commit(null, metadata);
          committedCreate = created;
        } else {
          super.commit(base, metadata);
        }
      } catch (ForbiddenException e) {
        throw new CommitForbiddenException(
            RestCatalogExceptionMapper.forbidden(
                e, "commit to table", bracket(PathUtils.constructFullPath(dataset)), redactor));
      } catch (NotAuthorizedException e) {
        throw new CommitNotAuthorizedException(
            RestCatalogExceptionMapper.notAuthorized(
                e, "commit to table", bracket(PathUtils.constructFullPath(dataset)), redactor));
      }
    }
  }

  /**
   * An HTTP 403 on commit, carrying the permission error to report.
   *
   * <p>It must stay a {@link ForbiddenException}, which is an Iceberg {@code CleanableFailure}:
   * Iceberg's commit path ({@code SnapshotProducer#commit}, {@code
   * BaseTransaction#commitTransaction}) deletes the manifests and manifest lists it wrote for the
   * rejected commit only for such failures (table operations require strict cleanup). Dremio still
   * reports the permission error: it is the cause, and {@code UserException} builders return a
   * {@code UserException} found in the cause chain of the exception they wrap.
   */
  @VisibleForTesting
  static final class CommitForbiddenException extends ForbiddenException {
    CommitForbiddenException(UserException permissionError) {
      super(permissionError, "%s", permissionError.getOriginalMessage());
    }
  }

  /**
   * An HTTP 401 on commit (the catalog rejected the session token), carrying the permission error
   * to report. A {@link NotAuthorizedException}, which is a {@code CleanableFailure}, for the same
   * reason as {@link CommitForbiddenException}.
   */
  @VisibleForTesting
  static final class CommitNotAuthorizedException extends NotAuthorizedException {
    CommitNotAuthorizedException(UserException permissionError) {
      super(permissionError, "%s", permissionError.getOriginalMessage());
    }
  }

  private static Namespace namespaceFromPath(List<String> folderPathWithSourceName) {
    return Namespace.of(
        folderPathWithSourceName
            .subList(1, folderPathWithSourceName.size())
            .toArray(new String[] {}));
  }
}
