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

import static com.dremio.exec.catalog.CatalogUtil.permittedNessieKey;

import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.concurrent.ContextAwareCompletableFuture;
import com.dremio.common.concurrent.bulk.BulkFunction;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.concurrent.bulk.ValueTransformer;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.impersonation.extensions.SupportsImpersonation;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.exec.catalog.CatalogImpl.IdentityResolver;
import com.dremio.exec.catalog.ManagedStoragePlugin.MetadataAccessType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.Views;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.ImpersonationConf;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceOptions;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workhorse of Catalog, responsible for retrieving datasets and interacting with sources as
 * necessary to complete a query.
 *
 * <p>This operates entirely in the context of the user's namespace with the exception of the
 * DatasetSaver, which hides access to the SystemUser namespace solely for purposes of retrieving
 * datasets.
 */
class DatasetManager {

  private static final Logger logger = LoggerFactory.getLogger(DatasetManager.class);

  private final PluginRetriever plugins;
  private final NamespaceService userNamespaceService;
  private final OptionManager optionManager;
  private final String userName;
  private final IdentityResolver identityProvider;
  private final VersionContextResolver versionContextResolver;
  private final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory;
  private final MetadataIOPool metadataIOPool;

  public DatasetManager(
      PluginRetriever plugins,
      NamespaceService userNamespaceService,
      OptionManager optionManager,
      String userName,
      IdentityResolver identityProvider,
      VersionContextResolver versionContextResolver,
      VersionedDatasetAdapterFactory versionedDatasetAdapterFactory,
      MetadataIOPool metadataIOPool) {
    this.userNamespaceService = userNamespaceService;
    this.plugins = plugins;
    this.optionManager = optionManager;
    this.userName = userName;
    this.identityProvider = identityProvider;
    this.versionContextResolver = versionContextResolver;
    this.versionedDatasetAdapterFactory = versionedDatasetAdapterFactory;
    this.metadataIOPool = metadataIOPool;
  }

  /**
   * A path is ambiguous if it has two parts and the root contains a period. In this case, we don't
   * know whether the root should be considered a single part or multiple parts and need to do a
   * search for an unescaped path rather than a lookup.
   *
   * <p>This is because JDBC & ODBC tools use a two part naming scheme and thus we also present
   * Dremio datasets using this two part scheme where all parts of the path except the leaf are
   * presented as part of the schema of the table.
   *
   * @param key Key to test
   * @return Whether path is ambiguous.
   */
  private boolean isAmbiguousKey(NamespaceKey key) {
    if (key.size() != 2) {
      return false;
    }

    return key.getRoot().contains(".");
  }

  private DatasetConfig getConfig(final NamespaceKey key) {
    if (!isAmbiguousKey(key)) {
      try {
        return userNamespaceService.getDataset(key);
      } catch (NamespaceNotFoundException ex) {
        return null;
      } catch (NamespaceException ex) {
        throw Throwables.propagate(ex);
      }
    }

    /**
     * If we have an ambiguous key, let's search for possible matches in the namespace.
     *
     * <p>From there we will: - only consider keys that have the same leaf value (since ambiguity
     * isn't allowed there) - return the first key by ordering the keys by segment (cis then cs).
     */
    final FindByCondition condition =
        new ImmutableFindByCondition.Builder()
            .setCondition(
                SearchQueryUtils.newTermQuery(
                    NamespaceIndexKeys.UNQUOTED_LC_PATH, key.toUnescapedString().toLowerCase()))
            .build();
    // do a case insensitive order first.
    // do a case sensitive order second.
    return StreamSupport.stream(userNamespaceService.find(condition).spliterator(), false)
        .filter(entry -> entry.getKey().getLeaf().equalsIgnoreCase(key.getLeaf()))
        .map(input -> input.getValue().getDataset())
        .min(
            (o1, o2) -> {
              List<String> p1 = o1.getFullPathList();
              List<String> p2 = o2.getFullPathList();

              final int size = Math.max(p1.size(), p2.size());

              for (int i = 0; i < size; i++) {

                // do a case insensitive order first.
                int cmp = p1.get(i).toLowerCase().compareTo(p2.get(i).toLowerCase());
                if (cmp != 0) {
                  return cmp;
                }

                // do a case sensitive order second.
                cmp = p1.get(i).compareTo(p2.get(i));
                if (cmp != 0) {
                  return cmp;
                }
              }

              Preconditions.checkArgument(
                  p1.size() == p2.size(),
                  "Two keys were indexed the same but had different lengths. %s, %s",
                  p1,
                  p2);
              return 0;
            })
        .orElse(null);
  }

  private Optional<DatasetConfig> getConfig(EntityId datasetId) {
    return userNamespaceService.getDatasetById(datasetId);
  }

  private NamespaceKey getCanonicalKey(NamespaceKey key) {
    Preconditions.checkArgument(isAmbiguousKey(key), "%s should be ambiguous", key.getSchemaPath());
    List<String> pathComponents = key.getPathComponents();
    List<String> entries = new ArrayList<>();
    for (String component : pathComponents) {
      entries.addAll(Arrays.asList(component.split("\\.(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")));
    }
    return new NamespaceKey(entries);
  }

  @WithSpan
  public DremioTable getTable(
      NamespaceKey key, MetadataRequestOptions options, boolean ignoreColumnCount) {
    final DatasetConfig config = getConfig(key);

    if (config != null) {
      // canonicalize the path.
      key = new NamespaceKey(config.getFullPathList());
    }

    if (isAmbiguousKey(key)) {
      key = getCanonicalKey(key);
    }

    NamespaceKeyWithConfig namespaceKeyWithConfig =
        new ImmutableNamespaceKeyWithConfig.Builder().setKey(key).setDatasetConfig(config).build();

    String pluginName = key.getRoot();
    final ManagedStoragePlugin plugin = plugins.getPlugin(pluginName, false);

    if (config == null) {
      logger.debug("Got a null config");
    } else {
      logger.debug("Got config id {}", config.getId());
    }

    if (plugin != null) {

      // if we have a plugin and the info isn't a vds (this happens in home, where VDS are
      // intermingled with plugin datasets).
      if (config == null || config.getType() != DatasetType.VIRTUAL_DATASET) {
        return getTableFromPlugin(namespaceKeyWithConfig, plugin, options, ignoreColumnCount);
      }
    }

    if (config == null) {
      return null;
    }

    // at this point, we should only be looking at virtual datasets.
    if (config.getType() != DatasetType.VIRTUAL_DATASET) {
      // if we're not looking at a virtual dataset, it must mean that we hit a race condition where
      // the source has been removed but the dataset was retrieved just before.
      return null;
    }

    return createTableFromVirtualDataset(config, options);
  }

  @WithSpan
  public BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTables(
      BulkRequest<NamespaceKey> keys, MetadataRequestOptions options, boolean ignoreColumnCount) {

    // Map to DatasetConfig and canonicalize the key
    Function<NamespaceKey, NamespaceKeyWithConfig> keyTransformer =
        key -> {
          DatasetConfig config = getConfig(key);

          NamespaceKey canonicalKey = key;
          if (config != null) {
            // canonicalize the path.
            canonicalKey = new NamespaceKey(config.getFullPathList());
          }

          if (isAmbiguousKey(canonicalKey)) {
            canonicalKey = getCanonicalKey(canonicalKey);
          }

          return new ImmutableNamespaceKeyWithConfig.Builder()
              .setKey(canonicalKey)
              .setDatasetConfig(config)
              .build();
        };

    return keys.bulkTransformAndHandleRequests(
        keyConfigs -> bulkGetTablesByCanonicalKey(keyConfigs, options, ignoreColumnCount),
        keyTransformer);
  }

  private BulkResponse<NamespaceKeyWithConfig, Optional<DremioTable>> bulkGetTablesByCanonicalKey(
      BulkRequest<NamespaceKeyWithConfig> keysWithConfig,
      MetadataRequestOptions options,
      boolean ignoreColumnCount) {

    // group into partitions:
    // - for any key without a DatasetConfig, or for keys that aren't for VIRTUAL_DATASET,
    //   partition by the ManagedStoragePlugin instance
    // - put all other keys into a separate partition
    Function<NamespaceKeyWithConfig, Optional<ManagedStoragePlugin>> partitionByPlugin =
        keyWithConfig -> {
          if (keyWithConfig.datasetConfig() == null
              || keyWithConfig.datasetConfig().getType() != DatasetType.VIRTUAL_DATASET) {
            return Optional.ofNullable(plugins.getPlugin(keyWithConfig.key().getRoot(), false));
          } else {
            return Optional.empty();
          }
        };

    // define the bulk functions to handle each partition of keys
    Function<
            Optional<ManagedStoragePlugin>,
            BulkFunction<NamespaceKeyWithConfig, Optional<DremioTable>>>
        bulkFunctions =
            optPlugin ->
                optPlugin.isPresent()
                    // for keys with a plugin instance, issue a bulkGetTablesFromPlugin call,
                    // one call for each unique plugin
                    ? requests ->
                        bulkGetTablesFromPlugin(
                            requests, optPlugin.get(), options, ignoreColumnCount)
                    // for other keys, handle the response on the calling thread.  if it's a
                    // VIRTUAL_DATASET, create the table for it synchronously.  for keys where we
                    // don't have a DatasetConfig return Optional.empty()
                    : requests ->
                        requests.handleRequests(
                            keyWithConfig ->
                                CompletableFuture.completedFuture(
                                    keyWithConfig.datasetConfig() == null
                                            || keyWithConfig.datasetConfig().getType()
                                                != DatasetType.VIRTUAL_DATASET
                                        ? Optional.empty()
                                        : Optional.of(
                                            createTableFromVirtualDataset(
                                                keyWithConfig.datasetConfig(), options))));

    return keysWithConfig.bulkPartitionAndHandleRequests(
        partitionByPlugin, bulkFunctions, Function.identity(), ValueTransformer.identity());
  }

  public DremioTable getTable(String datasetId, MetadataRequestOptions options) {
    Optional<DatasetConfig> config = getConfig(new EntityId(datasetId));

    if (config.isEmpty()) {
      VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(datasetId);
      if (versionedDatasetId != null) {
        Span.current().setAttribute("dremio.catalog.getTable.isVersionedDatasetId", true);
        // try lookup in external catalog
        return getTableFromNessieCatalog(versionedDatasetId, options);
      } else {
        return null;
      }
    }
    Span.current().setAttribute("dremio.catalog.getTable.isVersionedDatasetId", false);

    NamespaceKey key = new NamespaceKey(config.get().getFullPathList());

    return getTable(key, options, false);
  }

  @WithSpan
  private DremioTable getTableFromNessieCatalog(
      VersionedDatasetId versionedDatasetId, MetadataRequestOptions options) {
    List<String> tableKey = versionedDatasetId.getTableKey();
    TableVersionContext versionContext = versionedDatasetId.getVersionContext();
    String pluginName = tableKey.get(0);
    final ManagedStoragePlugin plugin = plugins.getPlugin(pluginName, false);
    if (plugin == null) {
      return null;
    }
    MetadataRequestOptions optionsWithVersion =
        options.cloneWith(tableKey.get(0), versionContext.asVersionContext());
    DremioTable table =
        getTableFromNessieCatalog(new NamespaceKey(tableKey), plugin, optionsWithVersion);
    if (table != null) {
      // check for ContentId . If someone has dropped and recreated the table with the same key
      // in the same VersionContext , the ContentId will be different
      VersionedDatasetId returnedVersionedDatasetId =
          VersionedDatasetId.tryParse(table.getDatasetConfig().getId().getId());
      if (returnedVersionedDatasetId == null) {
        logger.debug(
            "Could not parse VersionedDatasetId from string : {}",
            table.getDatasetConfig().getId().getId());
        return null;
      }
      if (!returnedVersionedDatasetId.getContentId().equals(versionedDatasetId.getContentId())) {
        logger.debug(
            "ContentId mismatch. VersionedDatasetId in : {} : VersionedDatasetId out : {}",
            versionedDatasetId.asString(),
            returnedVersionedDatasetId.asString());
        return null;
      }
    }
    return table;
  }

  private NamespaceTable getTableFromNamespace(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      ManagedStoragePlugin plugin,
      MetadataRequestOptions options,
      boolean savePrimaryKeyInKvStore) {
    final String accessUserName = getAccessUserName(plugin, options.getSchemaConfig());

    Span.current().setAttribute("dremio.namespace.key.schemapath", key.getSchemaPath());
    plugin.checkAccess(key, datasetConfig, accessUserName, options);

    if (plugin.getPlugin() instanceof FileSystemPlugin) {
      // DX-84516: Need to verify here and not just when accessing the path from FS plugin because
      // the user may have
      // already created/promoted a table they should not have access to - so we need to block the
      // direct NS access too
      List<String> pathComponents = key.getPathComponents();
      PathUtils.verifyNoDirectoryTraversal(
          pathComponents,
          () ->
              UserException.permissionError()
                  .message("Not allowed to perform directory traversal")
                  .addContext("Path", pathComponents.toString())
                  .buildSilently());
    }

    final TableMetadata tableMetadata =
        new TableMetadataImpl(
            plugin.getId(),
            datasetConfig,
            accessUserName,
            DatasetSplitsPointer.of(userNamespaceService, datasetConfig),
            getPrimaryKey(
                plugin.getPlugin(),
                datasetConfig,
                options.getSchemaConfig(),
                key,
                savePrimaryKeyInKvStore));
    return new NamespaceTable(tableMetadata, plugin.getDatasetMetadataState(datasetConfig), true);
  }

  private List<String> getPrimaryKey(
      StoragePlugin plugin,
      DatasetConfig datasetConfig,
      SchemaConfig config,
      NamespaceKey key,
      boolean saveInKvStore) {
    List<String> primaryKey = null;
    if (plugin instanceof MutablePlugin) {
      MutablePlugin mutablePlugin = (MutablePlugin) plugin;
      try {
        primaryKey = mutablePlugin.getPrimaryKey(key, datasetConfig, config, null, saveInKvStore);
      } catch (Exception ex) {
        logger.debug("Failed to get primary key", ex);
      }
    }
    return primaryKey;
  }

  /** Retrieves a source table, checking that things are up to date. */
  private DremioTable getTableFromPlugin(
      NamespaceKeyWithConfig namespaceKeyWithConfig,
      ManagedStoragePlugin plugin,
      MetadataRequestOptions options,
      boolean ignoreColumnCount) {

    NamespaceKey key = namespaceKeyWithConfig.key();
    DatasetConfig datasetConfig = namespaceKeyWithConfig.datasetConfig();

    // Special case 1: versioned plug-in.
    final StoragePlugin underlyingPlugin = plugin.getPlugin();
    if (underlyingPlugin != null && underlyingPlugin.isWrapperFor(VersionedPlugin.class)) {
      return getTableFromNessieCatalog(key, plugin, options);
    }

    // Special case 2: key is for a view.
    // TODO: move views to namespace and out of filesystem.
    if (datasetConfig == null) {
      try {
        ViewTable view = plugin.getView(key, options);
        if (view != null) {
          return view;
        }
      } catch (UserException ex) {
        throw ex;
      } catch (Exception ex) {
        logger.warn("Exception while trying to read view.", ex);
        return null;
      }
    }

    // Check completeness and validity of table metadata and get it.
    final Stopwatch stopwatch = Stopwatch.createStarted();
    if (plugin.checkValidity(namespaceKeyWithConfig, options)) {
      return getExistingTableFromNamespace(namespaceKeyWithConfig, plugin, options, stopwatch);
    }

    return getInvalidTableFromPlugin(
        namespaceKeyWithConfig, plugin, options, stopwatch, ignoreColumnCount);
  }

  private DremioTable getInvalidTableFromPlugin(
      NamespaceKeyWithConfig namespaceKeyWithConfig,
      ManagedStoragePlugin plugin,
      MetadataRequestOptions options,
      Stopwatch stopwatch,
      boolean ignoreColumnCount) {

    NamespaceKey key = namespaceKeyWithConfig.key();
    DatasetConfig datasetConfig = namespaceKeyWithConfig.datasetConfig();

    // If only the cached version is needed, check and return when no entry is found
    if (options.neverPromote()) {
      return null;
    }

    // Get dataset handle from the plugin.
    if (datasetConfig != null) {
      // canonicalize key if we can.
      key = new NamespaceKey(datasetConfig.getFullPathList());
    }
    final Optional<DatasetHandle> handle;
    try {
      handle =
          plugin.getDatasetHandle(
              key, datasetConfig, getDatasetRetrievalOptions(plugin, options, ignoreColumnCount));
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", key)
          .build(logger);
    }
    if (handle.isEmpty()) {
      return null;
    }

    final NamespaceKey canonicalKey =
        MetadataObjectsUtils.toNamespaceKey(handle.get().getDatasetPath());
    if (datasetConfig == null && !canonicalKey.equals(key)) {
      // before we do anything with this accessor, we should reprobe namespace as it is possible
      // that the request the user made was not the canonical key and therefore we missed when
      // trying to retrieve data from the namespace.

      try {
        datasetConfig = userNamespaceService.getDataset(canonicalKey);
        NamespaceKeyWithConfig canonicalKeyWithConfig =
            new ImmutableNamespaceKeyWithConfig.Builder()
                .setKey(canonicalKey)
                .setDatasetConfig(datasetConfig)
                .build();
        if (plugin.checkValidity(canonicalKeyWithConfig, options)) {
          return getExistingTableFromNamespace(canonicalKeyWithConfig, plugin, options, stopwatch);
        }
      } catch (NamespaceException e) {
        // ignore, we'll fall through.
      }
    }

    // arriving here means that the metadata for the table is either incomplete, missing or out of
    // date. We need to save it and return the updated data.
    return refreshMetadataAndGetTable(
        stopwatch, canonicalKey, datasetConfig, handle.get(), plugin, options, ignoreColumnCount);
  }

  private DremioTable getExistingTableFromNamespace(
      NamespaceKeyWithConfig namespaceKeyWithConfig,
      ManagedStoragePlugin plugin,
      MetadataRequestOptions options,
      Stopwatch stopwatch) {
    NamespaceKey key = namespaceKeyWithConfig.key();
    DatasetConfig datasetConfig = namespaceKeyWithConfig.datasetConfig();
    NamespaceTable table = getTableFromNamespace(key, datasetConfig, plugin, options, true);
    options
        .getStatsCollector()
        .addDatasetStat(
            new NamespaceKey(datasetConfig.getFullPathList()).getSchemaPath(),
            MetadataAccessType.CACHED_METADATA.name(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS));
    return table;
  }

  private BulkResponse<NamespaceKeyWithConfig, Optional<DremioTable>> bulkGetTablesFromPlugin(
      BulkRequest<NamespaceKeyWithConfig> keys,
      ManagedStoragePlugin plugin,
      MetadataRequestOptions options,
      boolean ignoreColumnCount) {

    // handle the VersionedPlugin case - this bypasses any interaction with NamespaceService
    final StoragePlugin underlyingPlugin = plugin.getPlugin();
    if (underlyingPlugin != null && underlyingPlugin.isWrapperFor(VersionedPlugin.class)) {
      return bulkGetTablesFromVersionedPlugin(keys, plugin, options);
    }

    BulkResponse.Builder<NamespaceKeyWithConfig, Optional<DremioTable>> responseBuilder =
        BulkResponse.builder();

    // filter out filesystem views
    // TODO: move views to namespace and out of filesystem.
    BulkRequest<NamespaceKeyWithConfig> filteredKeys;
    filteredKeys =
        keys.partition(
                key -> {
                  if (key.datasetConfig() == null) {
                    try {
                      ViewTable view = plugin.getView(key.key(), options);
                      if (view != null) {
                        responseBuilder.add(key, Optional.of(view));
                        return false;
                      }
                    } catch (Exception ex) {
                      logger.warn("Exception while trying to read view.", ex);
                      responseBuilder.add(key, CompletableFuture.failedFuture(ex));
                      return false;
                    }
                  }
                  return true;
                })
            .get(true);

    if (filteredKeys == null) {
      return responseBuilder.build();
    }

    Stopwatch stopwatch = Stopwatch.createStarted();

    BulkResponse<NamespaceKeyWithConfig, Optional<DremioTable>> tables =
        filteredKeys.bulkTransformAndHandleRequestsAsync(
            datasets -> plugin.bulkCheckValidity(datasets, options),
            Function.identity(),
            (originalKey, transformedKey, isValid) -> {
              if (isValid) {
                // If metadata is valid, get the metadata from Namespace on current thread.
                // This reduces load on the metadataIOPool when validityCheck=false.
                return ContextAwareCompletableFuture.of(
                    Optional.of(
                        getExistingTableFromNamespace(originalKey, plugin, options, stopwatch)));
              }
              // If we have to get metadata from source plugin,
              // then make sure we execute async using dedicated pool.
              return ContextAwareCompletableFuture.createFrom(
                  metadataIOPool.execute(
                      new MetadataIOPool.MetadataTask<>(
                          "get_table_from_plugin_async",
                          new EntityPath(originalKey.key().getPathComponents()),
                          () ->
                              Optional.ofNullable(
                                  getInvalidTableFromPlugin(
                                      originalKey,
                                      plugin,
                                      options,
                                      stopwatch,
                                      ignoreColumnCount)))));
            });

    return responseBuilder.addAll(tables).build();
  }

  /**
   * The method reads metadata (schema and other) from source via {@link ManagedStoragePlugin} and
   * creates and instance of {@link DremioTable} from result.
   *
   * <p>This could be expensive as it runs SQL command, which may spin up new executors.
   */
  @WithSpan
  private DremioTable refreshMetadataAndGetTable(
      Stopwatch stopwatch,
      NamespaceKey key,
      DatasetConfig datasetConfig,
      DatasetHandle handle,
      ManagedStoragePlugin plugin,
      MetadataRequestOptions options,
      boolean ignoreColumnCount) {

    final NamespaceKey canonicalKey = MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath());

    boolean opportunisticSave = (datasetConfig == null);
    if (opportunisticSave) {
      datasetConfig = MetadataObjectsUtils.newShallowConfig(handle);
    }
    logger.debug("Attempting inline refresh for  key : {} , canonicalKey : {} ", key, canonicalKey);
    boolean successfulSave = false;
    boolean cachedMetadata = false;
    boolean readConfigAfterSave = false;
    int retries = 1;
    do {
      try {
        plugin
            .getSaver()
            .save(
                datasetConfig,
                handle,
                plugin.unwrap(StoragePlugin.class),
                opportunisticSave,
                getDatasetRetrievalOptions(plugin, options, ignoreColumnCount),
                userName);
        successfulSave = true;
      } catch (ManagedStoragePlugin.StoragePluginChanging e) {
        // The source was changed in the KV store between:
        //    1. The getSaver() call where the e-tag is copied.
        //    2. And, the call to read/write from/to namespace during save call.
        // Retry once if so by re-creating the saver that uses the latest source e-tag.
        logger.warn(
            "Failed to refresh dataset's {} metadata as source was updated, retries left = {}",
            datasetConfig.getFullPathList(),
            retries,
            e);
        if (retries == 0) {
          throw e;
        }
      } catch (ConcurrentModificationException cme) {
        // Some other query, or perhaps the metadata refresh, must have already created this
        // dataset. Re-obtain it from the namespace.
        assert opportunisticSave : "Non-opportunistic saves should have already handled a CME";
        try {
          datasetConfig = userNamespaceService.getDataset(canonicalKey);
        } catch (NamespaceException e) {
          // We got a concurrent modification exception because a dataset existed. It shouldn't be
          // the case that it no longer exists. In the very rare case of this code racing with both
          // another update *and* a dataset deletion we should act as if the delete won.
          logger.warn(
              "Unable to obtain dataset {}. Likely race with dataset deletion", canonicalKey);
          return null;
        }

        // Check if dataset config is valid.
        if (datasetConfig.getReadDefinition() != null) {
          // Got metadata saved by a concurrent process.
          successfulSave = true;
          cachedMetadata = true;
        } else {
          // Getting here means the config was updated concurrently with null read definition, the
          // stack for the modification is logged in namespace service.
          logger.warn(
              "Read definition is null for: path = {} retries left = {}",
              datasetConfig.getFullPathList(),
              retries);
          if (retries == 0) {
            throw cme;
          }
          readConfigAfterSave = true;
        }
      } catch (AccessControlException ignored) {
        return null;
      }
    } while (!successfulSave && retries-- > 0);

    // Read config after retries.
    if (readConfigAfterSave) {
      try {
        datasetConfig = userNamespaceService.getDataset(canonicalKey);
      } catch (NamespaceException e) {
        logger.warn("Unable to obtain dataset {} after retries.", canonicalKey);
        return null;
      }
    }

    /* When not cached, the metadata for the table is either incomplete, missing or out of date.
    Storing in namespace can cause issues if the metadata is missing. Don't save here.
    */
    boolean savePrimaryKeyInKvStore = cachedMetadata;
    NamespaceTable namespaceTable =
        getTableFromNamespace(
            canonicalKey, datasetConfig, plugin, options, savePrimaryKeyInKvStore);
    options
        .getStatsCollector()
        .addDatasetStat(
            canonicalKey.getSchemaPath(),
            cachedMetadata
                ? MetadataAccessType.CACHED_METADATA.name()
                : MetadataAccessType.PARTIAL_METADATA.name(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS));
    return namespaceTable;
  }

  private static DatasetRetrievalOptions getDatasetRetrievalOptions(
      ManagedStoragePlugin plugin, MetadataRequestOptions options, boolean ignoreColumnCount) {
    return plugin.getDefaultRetrievalOptions().toBuilder()
        .setIgnoreAuthzErrors(options.getSchemaConfig().getIgnoreAuthErrors())
        .setMaxMetadataLeafColumns(
            (ignoreColumnCount)
                ? Integer.MAX_VALUE
                : plugin.getDefaultRetrievalOptions().maxMetadataLeafColumns())
        .setMaxNestedLevel(plugin.getDefaultRetrievalOptions().maxNestedLevel())
        .build();
  }

  /** Return whether or not saves are allowed */
  protected boolean checkCanSave(NamespaceKey key) {
    return true;
  }

  // Figure out the user we want to access the source with.  If the source supports impersonation we
  // allow it to
  // override the delegated username.
  private String getAccessUserName(ManagedStoragePlugin plugin, SchemaConfig schemaConfig) {
    final String accessUserName;

    ConnectionConf<?, ?> conf = plugin.getConnectionConf();
    if (conf instanceof ImpersonationConf) {
      if (plugin.getPlugin() instanceof SupportsImpersonation
          && !(schemaConfig.getAuthContext().getSubject() instanceof CatalogUser)) {
        if (((SupportsImpersonation) plugin.getPlugin()).isImpersonationEnabled()) {
          throw UserException.unsupportedError(
                  new InvalidImpersonationTargetException(
                      "Only users can be used to connect to impersonation enabled sources."))
              .buildSilently();
        }
      }

      String queryUser = null;
      if (schemaConfig.getViewExpansionContext() != null) {
        queryUser = schemaConfig.getViewExpansionContext().getQueryUser().getName();
      }
      accessUserName =
          ((ImpersonationConf) conf).getAccessUserName(schemaConfig.getUserName(), queryUser);
    } else {
      accessUserName = schemaConfig.getUserName();
    }

    return accessUserName;
  }

  @WithSpan("create-table-from-view")
  private ViewTable createTableFromVirtualDataset(
      DatasetConfig datasetConfig, MetadataRequestOptions options) {
    try {
      // 1.4.0 and earlier didn't correctly save virtual dataset schema information.
      BatchSchema schema =
          DatasetHelper.getSchemaBytes(datasetConfig) != null
              ? CalciteArrowHelper.fromDataset(datasetConfig)
              : null;

      View view =
          Views.fieldTypesToView(
              Iterables.getLast(datasetConfig.getFullPathList()),
              datasetConfig.getVirtualDataset().getSql(),
              ViewFieldsHelper.getViewFields(datasetConfig),
              datasetConfig.getVirtualDataset().getContextList(),
              options.getSchemaConfig().getOptions() != null ? schema : null);

      return new ViewTable(
          new NamespaceKey(datasetConfig.getFullPathList()),
          view,
          identityProvider.getOwner(datasetConfig.getFullPathList()),
          datasetConfig,
          schema);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failure while constructing the ViewTable from datasetConfig for key %s with datasetId %s",
              String.join(".", datasetConfig.getFullPathList()), datasetConfig.getId().getId()),
          e);
    }
  }

  @WithSpan
  private DremioTable getTableFromNessieCatalog(
      NamespaceKey key, ManagedStoragePlugin plugin, MetadataRequestOptions options) {
    final String accessUserName = options.getSchemaConfig().getUserName();
    final StoragePlugin underlyingPlugin = plugin.getPlugin();
    VersionedDatasetAdapter versionedDatasetAdapter;
    if (!permittedNessieKey(key)) {
      return null;
    }
    // If the key is the root, we do an early return - nothing to lookup in Nessie
    if (key.size() == 1 && plugin.getName().equals(key)) {
      return null;
    }

    try {
      versionedDatasetAdapter =
          versionedDatasetAdapterFactory.newInstance(
              key.getPathComponents(),
              versionContextResolver.resolveVersionContext(
                  plugin.getName().getRoot(),
                  options.getVersionForSource(plugin.getName().getRoot(), key)),
              underlyingPlugin,
              plugin.getId(),
              optionManager);
    } catch (VersionNotFoundInNessieException e) {
      logger.debug("Unable to retrieve table metadata for {} ", key, e);
      // Any error in resolving the version context returns a NessieReferenceException with a
      // detailed error.
      // We log that and return null to indicate that the table cannot be found.
      return null;
    }
    if (versionedDatasetAdapter == null) {
      return null;
    }
    return versionedDatasetAdapter.getTable(accessUserName);
  }

  private BulkResponse<NamespaceKeyWithConfig, Optional<DremioTable>>
      bulkGetTablesFromVersionedPlugin(
          BulkRequest<NamespaceKeyWithConfig> keys,
          ManagedStoragePlugin plugin,
          MetadataRequestOptions options) {

    // partition the requests into two subsets - permitted keys and invalid keys
    Function<NamespaceKeyWithConfig, Boolean> partitioner = key -> permittedNessieKey(key.key());

    // define the mapping to bulk handlers for each subset of keys
    Function<Boolean, BulkFunction<VersionedTableKey, Optional<VersionedDatasetAdapter>>>
        partitionBulkFunction =
            permitted ->
                permitted
                    // if this is a permitted key, forward a bulk request to
                    // versionedDatasetAdapterFactory
                    ? requests ->
                        versionedDatasetAdapterFactory.bulkCreateInstances(
                            requests, plugin.getPlugin(), plugin.getId(), optionManager, options)
                    // otherwise, return an empty value
                    : requests -> requests.handleRequests(Optional.empty());

    // define the key transformation from a NamespaceKeyWithConfig to a VersionedTableKey
    Function<NamespaceKeyWithConfig, VersionedTableKey> keyTransformer =
        key ->
            new ImmutableVersionedTableKey.Builder()
                .setVersionedTableKey(key.key().getPathComponents())
                .setVersionContext(
                    versionContextResolver.resolveVersionContext(
                        plugin.getName().getRoot(),
                        options.getVersionForSource(plugin.getName().getRoot(), key.key())))
                .build();

    // define the value transformation from a VersionedDatasetAdapter to a DremioTable
    String accessUserName = options.getSchemaConfig().getUserName();
    ValueTransformer<
            VersionedTableKey,
            Optional<VersionedDatasetAdapter>,
            NamespaceKeyWithConfig,
            Optional<DremioTable>>
        valueTransformer =
            (versionedTableKey, namespaceKeyWithConfig, optDataset) ->
                optDataset.map(
                    versionedDatasetAdapter -> versionedDatasetAdapter.getTable(accessUserName));

    return keys.bulkPartitionAndHandleRequests(
        partitioner, partitionBulkFunction, keyTransformer, valueTransformer);
  }

  private static boolean isFSBasedDataset(DatasetConfig datasetConfig) {
    return datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE
        || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FILE
        || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER
        || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  public boolean createOrUpdateDataset(
      ManagedStoragePlugin plugin,
      NamespaceKey datasetPath,
      DatasetConfig newConfig,
      NamespaceAttribute... attributes)
      throws NamespaceException {

    if (!isFSBasedDataset(newConfig)) {
      return userNamespaceService.tryCreatePhysicalDataset(datasetPath, newConfig, attributes);
    }

    DatasetConfig currentConfig = null;
    try {
      currentConfig = userNamespaceService.getDataset(datasetPath);
    } catch (NamespaceNotFoundException nfe) {
      // ignore
    }

    // if format settings did not change fall back to namespace based update
    if (currentConfig != null
        && currentConfig
            .getPhysicalDataset()
            .getFormatSettings()
            .equals(newConfig.getPhysicalDataset().getFormatSettings())) {
      return userNamespaceService.tryCreatePhysicalDataset(datasetPath, newConfig, attributes);
    }

    final DatasetRetrievalOptions retrievalOptions = plugin.getDefaultRetrievalOptions();
    try {
      // for home files dataset path is location of file not path in namespace.
      if (newConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE) {
        final NamespaceKey pathInHome =
            new NamespaceKey(
                ImmutableList.<String>builder()
                    .add("__home") // TODO (AH) hack.
                    .addAll(
                        PathUtils.toPathComponents(
                            newConfig.getPhysicalDataset().getFormatSettings().getLocation()))
                    .build());

        final Optional<DatasetHandle> handle =
            getHandle(plugin, pathInHome, newConfig, retrievalOptions);
        if (!handle.isPresent()) {
          // setting format setting on empty folders?
          return userNamespaceService.tryCreatePhysicalDataset(datasetPath, newConfig, attributes);
        }

        saveInHomeSpace(
            userNamespaceService,
            plugin.unwrap(StoragePlugin.class),
            handle.get(),
            retrievalOptions,
            newConfig);
        return true;
      }

      final Optional<DatasetHandle> handle =
          getHandle(plugin, datasetPath, newConfig, retrievalOptions);
      if (!handle.isPresent()) {
        // setting format setting on empty folders?
        return userNamespaceService.tryCreatePhysicalDataset(datasetPath, newConfig, attributes);
      }

      NamespaceUtils.copyFromOldConfig(currentConfig, newConfig);
      plugin
          .getSaver()
          .save(
              newConfig,
              handle.get(),
              plugin.unwrap(StoragePlugin.class),
              false,
              retrievalOptions,
              userName,
              attributes);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, NamespaceException.class);
      throw new RuntimeException("Failed to get new dataset ", e);
    }
    return true;
  }

  private static Optional<DatasetHandle> getHandle(
      ManagedStoragePlugin plugin,
      NamespaceKey key,
      DatasetConfig datasetConfig,
      DatasetRetrievalOptions retrievalOptions) {
    try {
      return plugin.getDatasetHandle(key, datasetConfig, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", key)
          .build(logger);
    }
  }

  public void createDataset(
      NamespaceKey key,
      ManagedStoragePlugin plugin,
      Function<DatasetConfig, DatasetConfig> datasetMutator) {
    DatasetConfig config;
    try {
      config = userNamespaceService.getDataset(key);

      if (config != null) {
        throw UserException.validationError()
            .message("Table already exists %s", key.getRoot())
            .build(logger);
      }
    } catch (NamespaceException ex) {
      logger.debug(
          "Failure while trying to retrieve dataset for key {}. Exception: {}",
          key,
          ex.getLocalizedMessage());
    }

    final DatasetRetrievalOptions retrievalOptions = plugin.getDefaultRetrievalOptions();
    final Optional<DatasetHandle> handle;
    try {
      handle = plugin.getDatasetHandle(key, null, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", key)
          .build(logger);
    }

    if (!handle.isPresent()) {
      throw UserException.validationError()
          .message("Unable to find requested dataset %s.", key)
          .build(logger);
    }

    config = MetadataObjectsUtils.newShallowConfig(handle.get());

    plugin
        .getSaver()
        .save(
            config,
            handle.get(),
            plugin.unwrap(StoragePlugin.class),
            false,
            retrievalOptions,
            datasetMutator::apply);
  }

  private void saveInHomeSpace(
      NamespaceService userNamespace,
      SourceMetadata sourceMetadata,
      DatasetHandle handle,
      DatasetRetrievalOptions options,
      DatasetConfig nsConfig) {
    Preconditions.checkNotNull(nsConfig);
    final NamespaceKey key = new NamespaceKey(nsConfig.getFullPathList());

    if (nsConfig.getId() == null) {
      nsConfig.setId(new EntityId(UUID.randomUUID().toString()));
    }

    NamespaceService.SplitCompression splitCompression =
        NamespaceService.SplitCompression.valueOf(
            optionManager.getOption(CatalogOptions.SPLIT_COMPRESSION_TYPE).toUpperCase());
    try (DatasetMetadataSaver saver =
        userNamespace.newDatasetMetadataSaver(
            key,
            nsConfig.getId(),
            splitCompression,
            optionManager.getOption(CatalogOptions.SINGLE_SPLIT_PARTITION_MAX),
            optionManager.getOption(NamespaceOptions.DATASET_METADATA_CONSISTENCY_VALIDATE))) {
      final PartitionChunkListing chunkListing =
          sourceMetadata.listPartitionChunks(handle, options.asListPartitionChunkOptions(nsConfig));

      final long recordCountFromSplits =
          saver == null || chunkListing == null
              ? 0
              : CatalogUtil.savePartitionChunksInSplitsStores(saver, chunkListing);
      final DatasetMetadata datasetMetadata =
          sourceMetadata.getDatasetMetadata(
              handle, chunkListing, options.asGetMetadataOptions(nsConfig));
      MetadataObjectsUtils.overrideExtended(
          nsConfig,
          datasetMetadata,
          Optional.empty(),
          recordCountFromSplits,
          options.maxMetadataLeafColumns());
      saver.saveDataset(nsConfig, false);
    } catch (DatasetMetadataTooLargeException e) {
      nsConfig.setRecordSchema(null);
      nsConfig.setReadDefinition(null);
      try {
        userNamespaceService.addOrUpdateDataset(key, nsConfig);
      } catch (NamespaceException ignored) {
      }
      throw UserException.validationError(e).build(logger);
    } catch (Exception e) {
      logger.warn("Failure while retrieving and saving dataset {}.", key, e);
    }
  }
}
