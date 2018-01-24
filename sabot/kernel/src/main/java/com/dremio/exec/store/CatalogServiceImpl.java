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
package com.dremio.exec.store;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.dremio.datastore.VersionExtractor;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin.CheckResult;
import com.dremio.exec.store.StoragePlugin.UpdateStatus;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.service.BindingCreator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Implementation of catalog service.
 */
public class CatalogServiceImpl implements CatalogService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogServiceImpl.class);

  public static final String CATALOG_SOURCE_DATA_NAMESPACE = "catalog-source-data";

  private final Plugin2Map pluginMap = new Plugin2Map();
  private final ConcurrentMap<NamespaceKey, Cancellable> updateTasks = Maps.newConcurrentMap();

  private final BindingCreator bindingCreator;
  private final Provider<SabotContext> context;
  private final Provider<SchedulerService> scheduler;
  private final Provider<KVStoreProvider> kvStoreProvider;
  private final boolean isMaster;
  private final boolean isCoordinator;
  private final Provider<SystemTablePluginConfigProvider> sysPluginProvider;

  private NamespaceService systemUserNamespaceService;
  private StoragePluginRegistryImpl registry;

  private KVStore<NamespaceKey, SourceInternalData> sourceDataStore;

  private boolean isClosing = false; // Set to 'true' upon close

  public CatalogServiceImpl(Provider<SabotContext> context,
                            Provider<SchedulerService> scheduler,
                            Provider<KVStoreProvider> kvStoreProvider,
                            BindingCreator bindingCreator,
                            boolean isMaster,
                            boolean isCoordinator,
                            Provider<SystemTablePluginConfigProvider> sysPluginProvider) {
    this.context = context;
    this.scheduler = scheduler;
    this.kvStoreProvider = kvStoreProvider;
    this.isMaster = isMaster;
    this.isCoordinator = isCoordinator;
    this.bindingCreator = bindingCreator;
    this.sysPluginProvider = sysPluginProvider;
  }

  @Override
  public StoragePlugin getStoragePlugin(String name){
    return pluginMap.get(name);
  }

  private boolean isFSBasedDataset(DatasetConfig datasetConfig) {
    return datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE ||
      datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FILE ||
      datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER ||
      datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  @Override
  public void start() throws Exception {
    SabotContext context = this.context.get();
    this.systemUserNamespaceService = context.getNamespaceService(SYSTEM_USERNAME);
    this.sourceDataStore = kvStoreProvider.get().getStore(CatalogSourceDataCreator.class);
    this.registry = new StoragePluginRegistryImpl(context, this, context.getStoreProvider(), sysPluginProvider.get());
    registry.init();
    bindingCreator.bind(StoragePluginRegistry.class, registry);
  }

  @Override
  public boolean createOrUpdateDataset(NamespaceService userNamespaceService, NamespaceKey source, final NamespaceKey datasetPath, final DatasetConfig datasetConfig) throws NamespaceException {
    final StoragePlugin sourceRegistry = pluginMap.get(source);

    if (!isFSBasedDataset(datasetConfig) || sourceRegistry == null) {
      return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig);
    }
    DatasetConfig oldDatasetConfig = null;
    try {
      oldDatasetConfig = userNamespaceService.getDataset(datasetPath);
    } catch (NamespaceNotFoundException nfe) {
      // ignore
    }

    // if format settings did not change fall back to namespace based update
    if (oldDatasetConfig != null && oldDatasetConfig.getPhysicalDataset().getFormatSettings().equals(datasetConfig.getPhysicalDataset().getFormatSettings())) {
      return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig);
    }

    try {
      // for home files dataset path is location of file not path in namespace.
      if (datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER) {
        final SourceTableDefinition datasetAccessor = sourceRegistry.getDataset(
          new NamespaceKey(ImmutableList.<String>builder()
            .add("__home") // TODO (AH) hack.
            .addAll(PathUtils.toPathComponents(datasetConfig.getPhysicalDataset().getFormatSettings().getLocation())).build()),
          datasetConfig, false);
        if (datasetAccessor == null) {
          return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig);
        }
        saveInHomeSpace(userNamespaceService, datasetAccessor, datasetConfig);
      } else {
        final SourceTableDefinition datasetAccessor = sourceRegistry.getDataset(datasetPath, datasetConfig, false);
        if (datasetAccessor == null) {
          // setting format setting on empty folders?
          return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig);
        }
        completeSave(userNamespaceService, datasetAccessor, oldDatasetConfig);
      }
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, NamespaceException.class);
      throw new RuntimeException("Failed to get new dataset " + e);
    }
    return true;
  }

  @Override
  public void registerSource(NamespaceKey source, StoragePlugin plugin2) {
    if(plugin2 != null){
      pluginMap.put(source, plugin2);
    }
    if (!sourceDataStore.contains(source)) {
      sourceDataStore.put(source, new SourceInternalData());
    }
  }

  @Override
  public void scheduleMetadataRefresh(NamespaceKey source, SourceConfig sourceConfig) {
    if (!isCoordinator || !isMaster) {
      // metadata collection is a master-only proposition.
      return;
    }
    MetadataPolicy policy = sourceConfig.getMetadataPolicy();
    if (policy == null) {
      policy = NEVER_REFRESH_POLICY;
    }
    if (updateTasks.get(source) != null) {
      Cancellable c = updateTasks.remove(source);
      if (c != null) {
        c.cancel(false);
      }
    }

    long lastNameRefresh = 0;
    long lastFullRefresh = 0;
    SourceInternalData srcData = sourceDataStore.get(source);
    // NB: srcData could become null under a race between an unregisterSource() and this background metadata update task
    if (srcData != null) {
      lastNameRefresh = (srcData.getLastNameRefreshDateMs() == null) ? 0 : srcData.getLastNameRefreshDateMs();
      lastFullRefresh = (srcData.getLastFullRefreshDateMs() == null) ? 0 : srcData.getLastFullRefreshDateMs();
    }
    final long nextNameRefresh = lastNameRefresh + policy.getNamesRefreshMs();
    final long nextDatasetRefresh = lastFullRefresh + policy.getDatasetDefinitionRefreshAfterMs();
    // Next scheduled time to run is whichever comes first (name or dataset refresh), but no earlier than 'now'
    // Please note: upon the initial addition of a source, the caller (storage plugin registry) explicitly calls
    // refreshSourceNames(), which then sets the last name refresh date. However, the last dataset refresh remains
    // null, which triggers the Math.max() below, and causes the next refresh time to be exactly 'now'
    final long nextRefresh = Math.max(Math.min(nextNameRefresh, nextDatasetRefresh), System.currentTimeMillis());

    // Default refresh time only used when there are issues in the per-source namespace update
    final long defaultRefreshMs = Math.min(policy.getNamesRefreshMs(), policy.getDatasetDefinitionRefreshAfterMs());
    PerSourceNamespaceUpdateTask ut = new PerSourceNamespaceUpdateTask(source, source.getRoot(), defaultRefreshMs);

    scheduleUpdate(ut, nextRefresh);
  }


  /**
   * Schedule update task 'task' to run at 'nextStartTimeMs' milliseconds
   * @param nextStartTimeMs absolute time at which the next update should run. In the same units of measure as System.currentTimeMillis()
   */
  private void scheduleUpdate(PerSourceNamespaceUpdateTask task, long nextStartTimeMs) {
    Cancellable c = scheduler.get()
      .schedule(ScheduleUtils.scheduleForRunningOnceAt(nextStartTimeMs), task);
    if (updateTasks.putIfAbsent(task.getSourceKey(), c) != null) {
      // Someone else already scheduled this source. This attempt turns into a no-op
      c.cancel(false);
    }
  }

  /**
   * Update to 'sourceKey' started running
   */
  private void updateStartedRunning(NamespaceKey sourceKey) {
    updateTasks.remove(sourceKey);
  }


  @Override
  public void unregisterSource(NamespaceKey source) {
    // Note: V1 plugins not stored in the sourceRegistryMap, so the removal for them is a no-op
    pluginMap.remove(source);
    Cancellable c = updateTasks.remove(source);
    if (c != null) {
      c.cancel(false);
    }
    sourceDataStore.delete(source);
  }

  @Override
  public void close() throws Exception {
    isClosing = true;
    AutoCloseables.close(registry);
    for(Cancellable c : updateTasks.values()) {
      c.cancel(false);
    }
  }

  /**
   * version extractor for namespace container.
   */
  public static class SourceInternalDataVersionExtractor implements VersionExtractor<SourceInternalData> {
    @Override
    public Long getVersion(SourceInternalData value) {
      return value.getVersion();
    }
    @Override
    public void setVersion(SourceInternalData value, Long version) {
      value.setVersion(version);
    }
    @Override
    public Long incrementVersion(SourceInternalData value) {
      Long version = getVersion(value);
      setVersion(value, version == null ? 0 : version + 1);
      return version;
    }
  }

  /**
   * Creator for catalog source data kvstore
   */
  public static class CatalogSourceDataCreator implements StoreCreationFunction<KVStore<NamespaceKey, SourceInternalData>> {
    @Override
    public KVStore<NamespaceKey, SourceInternalData> build(StoreBuildingFactory factory) {
      return factory.<NamespaceKey, SourceInternalData>newStore()
        .name(CATALOG_SOURCE_DATA_NAMESPACE)
        .keySerializer(NamespaceKeySerializer.class)
        .valueSerializer(SourceInternalDataSerializer.class)
        .versionExtractor(SourceInternalDataVersionExtractor.class)
        .build();
    }
  }

  /**
  * A serializer for namespace keys
  */
  public static class NamespaceKeySerializer extends Serializer<NamespaceKey> {
    @Override
    public String toJson(NamespaceKey v) throws IOException {
      return StringSerializer.INSTANCE.toJson(v.toString());
    }

    @Override
    public NamespaceKey fromJson(String v) throws IOException {
      return new NamespaceKey(StringSerializer.INSTANCE.fromJson(v));
    }

    @Override
    public byte[] convert(NamespaceKey v) {
      return StringSerializer.INSTANCE.convert(v.toString());
    }

    @Override
    public NamespaceKey revert(byte[] v) {
      return new NamespaceKey(StringSerializer.INSTANCE.revert(v));
    }
  }

  /**
   * Serializer for SourceInternalData.
   */
  public static class SourceInternalDataSerializer extends Serializer<SourceInternalData> {
    private final Serializer<SourceInternalData> serializer = ProtostuffSerializer.of(SourceInternalData.getSchema());

    public SourceInternalDataSerializer() {
    }

    @Override
    public String toJson(SourceInternalData v) throws IOException {
      return serializer.toJson(v);
    }

    @Override
    public SourceInternalData fromJson(String v) throws IOException {
      return serializer.fromJson(v);
    }

    @Override
    public byte[] convert(SourceInternalData v) {
      return serializer.convert(v);
    }

    @Override
    public SourceInternalData revert(byte[] v) {
      return serializer.revert(v);
    }
  }


  private class MutatedSourceTableDefinition implements SourceTableDefinition {

    private final SourceTableDefinition delegate;
    private final Function<DatasetConfig, DatasetConfig> datasetMutator;


    public MutatedSourceTableDefinition(SourceTableDefinition delegate,
        Function<DatasetConfig, DatasetConfig> datasetMutator) {
      super();
      this.delegate = delegate;
      this.datasetMutator = datasetMutator;
    }

    @Override
    public NamespaceKey getName() {
      return delegate.getName();
    }

    @Override
    public DatasetConfig getDataset() throws Exception {
      return datasetMutator.apply(delegate.getDataset());
    }

    @Override
    public List<DatasetSplit> getSplits() throws Exception {
      return delegate.getSplits();
    }

    @Override
    public boolean isSaveable() {
      return delegate.isSaveable();
    }

    @Override
    public DatasetType getType() {
      return delegate.getType();
    }

  }

  @Override
  public void createDataset(NamespaceKey key, Function<DatasetConfig, DatasetConfig> datasetMutator){
    StoragePlugin plugin = pluginMap.get(key);
    if(plugin == null){
      throw UserException.validationError().message("Unknown source %s", key.getRoot()).build(logger);
    }
    DatasetConfig config = null;
    try {
      config = systemUserNamespaceService.getDataset(key);
      if(config != null){
        throw UserException.validationError().message("Table already exists %s", key.getRoot()).build(logger);
      }
    }catch (NamespaceException ex){
      logger.debug("Failure while trying to retrieve dataset for key {}.", key, ex);
    }

    SourceTableDefinition definition = null;
    try {
      definition = plugin.getDataset(key, null, false);
    } catch (Exception ex){
      throw UserException.dataReadError(ex).message("Failure while attempting to read metadata for table %s from source.", key).build(logger);
    }

    if(definition == null){
      throw UserException.validationError().message("Unable to find requested table %s.", key).build(logger);
    }

    completeSave(systemUserNamespaceService, datasetMutator == null ? definition : new MutatedSourceTableDefinition(definition, datasetMutator), config);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key) {
    StoragePlugin plugin = pluginMap.get(key);
    if(plugin == null){
      throw UserException.validationError().message("Unknown table %s", key).build(logger);
    }
    DatasetConfig config = null;
    try {
      config = systemUserNamespaceService.getDataset(key);
    }catch (NamespaceException ex){
      logger.debug("Failure while trying to retrieve dataset for key {}.", key, ex);
    }

    SourceTableDefinition definition;
    try {
      if(config != null && config.getReadDefinition() != null){
        final CheckResult result = plugin.checkReadSignature(config.getReadDefinition().getReadSignature(), config);

        switch(result.getStatus()){
        case DELETED:
          try {
            systemUserNamespaceService.deleteDataset(key, config.getVersion());
          } catch (NamespaceNotFoundException e) {
            // Ignore...
          }
          // fall through
        case UNCHANGED:
          return result.getStatus();

        case CHANGED:
          definition = result.getDataset();
          break;
        default:
          throw new IllegalStateException();
        }

      } else {
        definition = plugin.getDataset(key, config, false);
      }
    } catch (Exception ex){
      throw UserException.dataReadError(ex).message("Failure while attempting to read metadata for table %s from source.", key).build(logger);
    }

    if(definition == null){
      throw UserException.validationError().message("Unable to find requested table %s.", key).build(logger);
    }

    completeSave(systemUserNamespaceService, definition, config);
    return UpdateStatus.CHANGED;
  }

  @Override
  public void refreshSourceNames(NamespaceKey source, MetadataPolicy policy) throws NamespaceException {
    final StoragePlugin sourceRegistry = pluginMap.get(source);
    final Set<NamespaceKey> foundKeys = Sets.newHashSet(systemUserNamespaceService.getAllDatasets(source));

    try {
      for (SourceTableDefinition accessor : sourceRegistry.getDatasets(SYSTEM_USERNAME, false)) {
        // names only added, never removed. Removal can be done by the full refresh (refreshSource())
        if (!foundKeys.contains(accessor.getName()) && accessor.isSaveable()) {
          shallowSave(accessor);
        }
      }
    } catch (Exception ex){
      logger.warn("Failure while attempting to update metadata for source {}. Terminating update of this source.", source, ex);
    }

    SourceInternalData srcData = sourceDataStore.get(source);
    srcData.setLastNameRefreshDateMs(System.currentTimeMillis());
    sourceDataStore.put(source, srcData);
  }

  @Override
  public boolean refreshSource(final NamespaceKey source, MetadataPolicy policy) throws NamespaceException {
    boolean refreshResult = false;
    /**
     * Assume everything in the namespace is deleted. As we discover the datasets from source, remove found entries
     * from these sets.
     */
    final StoragePlugin sourceRegistry = pluginMap.get(source);

    Stopwatch stopwatch = Stopwatch.createStarted();
    try{
      final Set<NamespaceKey> foundKeys = Sets.newHashSet(systemUserNamespaceService.getAllDatasets(source));

      final Set<NamespaceKey> orphanedFolders = Sets.newHashSet();
      for(NamespaceKey foundKey : foundKeys) {
        addFoldersOnPathToDeletedFolderSet(foundKey, orphanedFolders);
      }

      final Set<NamespaceKey> knownKeys = new HashSet<>();
      for(NamespaceKey foundKey : foundKeys) {
        // Refresh might take a long time. Quit if the daemon is closing, to avoid shutdown issues
        if (isClosing) {
          logger.info("Aborting update of metadata for table {} -- service is closing.", foundKey);
          return refreshResult;
        }

        Stopwatch stopwatchForDataset = Stopwatch.createStarted();
        // for each known dataset, update things.
        try {
          DatasetConfig config = systemUserNamespaceService.getDataset(foundKey);
          CheckResult result = CheckResult.UNCHANGED;

          if (sourceRegistry.datasetExists(foundKey)) {
            if (policy.getDatasetUpdateMode() == UpdateMode.PREFETCH ||
              (policy.getDatasetUpdateMode() == UpdateMode.PREFETCH_QUERIED && config.getReadDefinition() != null)) {
              if (config.getReadDefinition() == null) {
                // this is currently a name only dataset. Get the read definition.
                final SourceTableDefinition definition = sourceRegistry.getDataset(foundKey, config, false);
                result = new CheckResult() {
                  @Override
                  public UpdateStatus getStatus() {
                    return UpdateStatus.CHANGED;
                  }

                  @Override
                  public SourceTableDefinition getDataset() {
                    return definition;
                  }
                };
              } else {
                // have a read definition, need to check if it is up to date.
                result = sourceRegistry.checkReadSignature(config.getReadDefinition().getReadSignature(), config);
              }
            }
          } else {
            result = CheckResult.DELETED;
          }

          if (result.getStatus() == UpdateStatus.DELETED) {
            // TODO: handle exception
            systemUserNamespaceService.deleteDataset(foundKey, config.getVersion());
            refreshResult = true;
          } else {
            if (result.getStatus() == UpdateStatus.CHANGED) {
              completeSave(systemUserNamespaceService, result.getDataset(), config);
              refreshResult = true;
            }
            knownKeys.add(foundKey);
            removeFoldersOnPathFromOprhanSet(foundKey, orphanedFolders);
          }
        } catch(NamespaceNotFoundException nfe) {
          // Race condition: someone removed a dataset from the system namespace while we were iterating.
          // No-op
        } catch(Exception ex) {
          logger.warn("Failure while attempting to update metadata for table {}.", foundKey, ex);
        }
        finally {
          stopwatchForDataset.stop();
          if (logger.isDebugEnabled()) {
            logger.debug("Metadata refresh for dataset : {} took {} milliseconds.", foundKey, stopwatchForDataset.elapsed(TimeUnit.MILLISECONDS));
          }
        }
      }

      for(SourceTableDefinition accessor : sourceRegistry.getDatasets(SYSTEM_USERNAME, false)){
        if (isClosing) {
          return refreshResult;
        }
        if(knownKeys.add(accessor.getName())){
          if(!accessor.isSaveable()){
            continue;
          }

          // this is a new dataset, add it to namespace.
          if(policy.getDatasetUpdateMode() != UpdateMode.PREFETCH){
            shallowSave(accessor);
          } else {
            completeSave(systemUserNamespaceService, accessor, null);
          }
          refreshResult = true;
          removeFoldersOnPathFromOprhanSet(accessor.getName(), orphanedFolders);
        }
      }

      for(NamespaceKey folderKey : orphanedFolders) {
        if (isClosing) {
          return refreshResult;
        }
        try {
          final FolderConfig folderConfig = systemUserNamespaceService.getFolder(folderKey);
          systemUserNamespaceService.deleteFolder(folderKey, folderConfig.getVersion());
          refreshResult = true;
        } catch (NamespaceNotFoundException ex) {
          // no-op
        } catch (NamespaceException ex) {
          logger.warn("Failed to delete dataset from Namespace ");
        }
      }
    } catch (Exception ex){
      logger.warn("Failure while attempting to update metadata for source {}. Terminating update of this source.", source, ex);
    }
    finally {
       stopwatch.stop();
       if (logger.isDebugEnabled()) {
         logger.debug("Metadata refresh for source : {} took {} milliseconds.", source, stopwatch.elapsed(TimeUnit.MILLISECONDS));
       }
    }

    if (isClosing) {
      return refreshResult;
    }
    SourceInternalData srcData = sourceDataStore.get(source);
    long currTime = System.currentTimeMillis();
    srcData.setLastFullRefreshDateMs(currTime).setLastNameRefreshDateMs(currTime);
    sourceDataStore.put(source, srcData);

    return refreshResult;
  }

  private void shallowSave(SourceTableDefinition accessor) throws NamespaceException{
    NamespaceKey key = accessor.getName();
    DatasetConfig shallow = new DatasetConfig();
    shallow.setId(new EntityId().setId(UUID.randomUUID().toString()));
    shallow.setCreatedAt(System.currentTimeMillis());
    shallow.setName(key.getName());
    shallow.setFullPathList(key.getPathComponents());
    shallow.setType(accessor.getType());
    shallow.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
    systemUserNamespaceService.addOrUpdateDataset(key, shallow);
  }

  private void saveInHomeSpace(NamespaceService namespaceService, SourceTableDefinition accessor, DatasetConfig nsConfig) {
    Preconditions.checkNotNull(nsConfig);
    final NamespaceKey key = new NamespaceKey(nsConfig.getFullPathList());
    try{
      // use key from namespace config
      DatasetConfig srcConfig = accessor.getDataset();
      if (nsConfig.getId() == null) {
        nsConfig.setId(srcConfig.getId());
      }
      // Merge namespace config with config obtained from underlying filesystem used to store user uploaded files.
      // Set schema, read definition and state from source accessor
      nsConfig.setRecordSchema(srcConfig.getRecordSchema());
      nsConfig.setSchemaVersion(srcConfig.getSchemaVersion());
      nsConfig.setReadDefinition(srcConfig.getReadDefinition());
      // get splits from source
      List<DatasetSplit> splits = accessor.getSplits();
      namespaceService.addOrUpdateDataset(key, nsConfig, splits);
    }catch(Exception ex){
      logger.warn("Failure while retrieving and saving dataset {}.", key, ex);
    }
  }

  private void completeSave(NamespaceService namespaceService , SourceTableDefinition accessor, DatasetConfig oldConfig){
    try{
      NamespaceKey key = accessor.getName();

      DatasetConfig config = accessor.getDataset();
      if (oldConfig != null) {
        NamespaceUtils.copyFromOldConfig(oldConfig, config);
      }
      List<DatasetSplit> splits = accessor.getSplits();
      namespaceService.addOrUpdateDataset(key, config, splits);
    }catch(Exception ex){
      logger.warn("Failure while retrieving and saving dataset {}.", accessor.getName(), ex);
    }
  }

  private static void addFoldersOnPathToDeletedFolderSet(NamespaceKey dsKey, Set<NamespaceKey> existingFolderSet) {
    NamespaceKey key = dsKey.getParent();
    while(key.hasParent()) { // a folder always has a parent
      existingFolderSet.add(key);
      key = key.getParent();
    }
  }

  private static void removeFoldersOnPathFromOprhanSet(NamespaceKey dsKey, Set<NamespaceKey> existingFolderSet) {
    NamespaceKey key = dsKey;
    while(key.hasParent()) { // a folder always has a parent
      key = key.getParent();
      existingFolderSet.remove(key);
    }
  }

  /*
   * Background namespace and metadata refresher thread. One per source. Runs only on coordinator nodes
   */
  private class PerSourceNamespaceUpdateTask implements Runnable {
    private final NamespaceKey sourceKey;
    private final String sourceName;  // TODO: when all plugins moved to V2 interface, remove the sourceName (it's no longer needed)
    private long defaultRefreshMs;  // How long ago was 'this' scheduled to run. Updated on every run

    // Any invocation within this granularity is considered a match. It's sufficient for the constant below to be
    // larger than the true granularity of the underlying scheduler, but not 'big enough'. All configuration values
    // are in minutes, so 1s should be a nice middle ground
    private static final long SCHEDULER_GRANULARITY_MS = 1 * 1000;

    public PerSourceNamespaceUpdateTask(NamespaceKey sourceKey, String sourceName, long defaultRefreshMs) {
      this.sourceKey = sourceKey;
      this.sourceName = sourceName;
      this.defaultRefreshMs = defaultRefreshMs;
    }

    NamespaceKey getSourceKey() {
      return sourceKey;
    }

    @Override
    public void run() {
      long nextUpdateMs = System.currentTimeMillis() + defaultRefreshMs;
      // Task scheduled to run every min(namesRefreshMs, datasetDefinitionRefreshAfterMs)
      updateStartedRunning(sourceKey);
      try {
        StoragePlugin plugin2 = pluginMap.get(sourceKey);
        SourceState sourceState = plugin2.getState();
        if (sourceState == null || sourceState.getStatus() == SourceStatus.bad) {
          logger.info("Ignoring metadata load for source {} since it is currently in a bad state.", sourceKey);
          return;
        }
        SourceConfig config = systemUserNamespaceService.getSource(sourceKey);
        MetadataPolicy policy = config.getMetadataPolicy();
        if (policy == null) {
          policy = DEFAULT_METADATA_POLICY;
        }
        SourceInternalData srcData = sourceDataStore.get(sourceKey);
        long lastNameRefresh = 0;
        long lastFullRefresh = 0;
        // NB: srcData could become null under a race between an unregisterSource() and this background metadata update task
        if (srcData != null) {
          lastNameRefresh = (srcData.getLastNameRefreshDateMs() == null) ? 0 : srcData.getLastNameRefreshDateMs();
          lastFullRefresh = (srcData.getLastFullRefreshDateMs() == null) ? 0 : srcData.getLastFullRefreshDateMs();
        }
        long currentTimeMs = System.currentTimeMillis();
        if (currentTimeMs >= lastFullRefresh + policy.getDatasetDefinitionRefreshAfterMs() - SCHEDULER_GRANULARITY_MS) {
          logger.debug(String.format("Full update for source '%s'", sourceKey));
          lastFullRefresh = currentTimeMs;
          lastNameRefresh = currentTimeMs;
          refreshSource(sourceKey, policy);
        } else if (currentTimeMs >= lastNameRefresh + policy.getNamesRefreshMs() - SCHEDULER_GRANULARITY_MS) {
          logger.debug(String.format("Name-only update for source '%s'", sourceKey));
          lastNameRefresh = currentTimeMs;
          refreshSourceNames(sourceKey, policy);
        }
        currentTimeMs = System.currentTimeMillis(); // re-obtaining the time, because metadata updates can take a very long time
        long nextNameRefresh = lastNameRefresh + policy.getNamesRefreshMs();
        long nextFullRefresh = lastFullRefresh + policy.getDatasetDefinitionRefreshAfterMs();
        // Next update opportunity is at the earlier of the two updates, but no earlier than 'now'
        nextUpdateMs = Math.max(Math.min(nextNameRefresh, nextFullRefresh), currentTimeMs);
      } catch (Exception e) {
        // Exception while updating the metadata. Ignore, and try again later
        logger.warn(String.format("Failed to update namespace for plugin '%s'", sourceKey), e);
      }
      scheduleUpdate(this, nextUpdateMs);
    }
  }

  @Override
  public StoragePluginRegistry getOldRegistry() {
    return registry;
  }

  /**
   * Test-only interface: trim away any background metadata update tasks that point to sources that no longer exist
   */
  @VisibleForTesting
  public void testTrimBackgroundTasks() {
    List<NamespaceKey> toBeRemoved = Lists.newArrayList();
    for (NamespaceKey sourceKey : updateTasks.keySet()) {
      if (!systemUserNamespaceService.exists(sourceKey)) {
        toBeRemoved.add(sourceKey);
      }
    }
    for (NamespaceKey sourceKey : toBeRemoved) {
      unregisterSource(sourceKey);
    }
  }

  /**
   * Get the last full refresh date for a given source
   * @param sourceKey
   * @return the last full refresh date for 'sourceKey', or NULL if no such source exists
   */
  @VisibleForTesting
  Long getSourceLastFullRefreshDate(NamespaceKey sourceKey) {
    SourceInternalData srcData = sourceDataStore.get(sourceKey);
    if (srcData != null) {
      return srcData.getLastFullRefreshDateMs();
    }
    return null;
  }
}
