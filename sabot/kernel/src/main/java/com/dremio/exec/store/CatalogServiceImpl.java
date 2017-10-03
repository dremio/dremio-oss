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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.threeten.bp.Instant;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.StoragePlugin2.CheckResult;
import com.dremio.exec.store.StoragePlugin2.UpdateStatus;
import com.dremio.exec.store.sys.SystemTablePluginProvider;
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
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
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

  private final Map<NamespaceKey, StoragePlugin2> sourceRegistryMap = Maps.newConcurrentMap();
  private final Map<NamespaceKey, Cancellable> updateTasks = Maps.newConcurrentMap();

  private final BindingCreator bindingCreator;
  private final Provider<SabotContext> context;
  private final Provider<SchedulerService> scheduler;
  private final boolean isMaster;
  private final boolean isCoordinator;
  private final Provider<SystemTablePluginProvider> sysPluginProvider;

  private OptionManager options;

  private NamespaceService systemUserNamespaceService;
  private StoragePluginRegistryImpl registry;

  public CatalogServiceImpl(Provider<SabotContext> context,
                            Provider<SchedulerService> scheduler,
                            BindingCreator bindingCreator,
                            boolean isMaster,
                            boolean isCoordinator,
                            Provider<SystemTablePluginProvider> sysPluginProvider) {
    this.context = context;
    this.scheduler = scheduler;
    this.isMaster = isMaster;
    this.isCoordinator = isCoordinator;
    this.bindingCreator = bindingCreator;
    this.sysPluginProvider = sysPluginProvider;
  }

  public StoragePlugin2 getStoragePlugin(String name){
    return sourceRegistryMap.get(new NamespaceKey(name));
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
    this.options = context.getOptionManager();
    this.systemUserNamespaceService = context.getNamespaceService(SYSTEM_USERNAME);
    this.registry = new StoragePluginRegistryImpl(context, this, context.getStoreProvider(), sysPluginProvider.get());
    registry.init();
    bindingCreator.bind(StoragePluginRegistry.class, registry);
  }

  private long getRefreshRateMillis(){
    return TimeUnit.MILLISECONDS.convert(options.getOption(ExecConstants.SOURCE_METADATA_REFRESH_MIN), TimeUnit.MINUTES);
  }

  @Override
  public boolean createOrUpdateDataset(NamespaceService userNamespaceService, NamespaceKey source, final NamespaceKey datasetPath, final DatasetConfig datasetConfig) throws NamespaceException {
    final StoragePlugin2 sourceRegistry = sourceRegistryMap.get(source);

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
  public void registerSource(NamespaceKey source, StoragePlugin2 sourceRegistry) {
    if(sourceRegistry != null){
      sourceRegistryMap.put(source, sourceRegistry);
    }
    if (isCoordinator && isMaster) {  // metadata collection is a master-only proposition.
      if (updateTasks.get(source) == null) {
        PerSourceNamespaceUpdateTask ut = new PerSourceNamespaceUpdateTask(source, source.getRoot());
        Cancellable c = scheduler.get().schedule(Schedule.Builder
          .everyMillis(getRefreshRateMillis())
          .startingAt(Instant.now().plusMillis(getRefreshRateMillis()))
          .build(), ut);
        updateTasks.put(source, c);
      }
    }
  }

  @Override
  public void unregisterSource(NamespaceKey source) {
    // Note: V1 plugins not stored in the sourceRegistryMap, so the removal for them is a no-op
    sourceRegistryMap.remove(source);
    Cancellable c = updateTasks.remove(source);
    if (c != null) {
      c.cancel();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(registry);
    for(Cancellable c : updateTasks.values()) {
      c.cancel();
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

  public void createDataset(NamespaceKey key, Function<DatasetConfig, DatasetConfig> datasetMutator){
    StoragePlugin2 plugin = sourceRegistryMap.get(new NamespaceKey(key.getRoot()));
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

  public UpdateStatus refreshDataset(NamespaceKey key) {
    StoragePlugin2 plugin = sourceRegistryMap.get(new NamespaceKey(key.getRoot()));
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
          systemUserNamespaceService.deleteDataset(key, config.getVersion());
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
  public boolean refreshSource(final NamespaceKey source, MetadataPolicy policy) throws NamespaceException {
    boolean refreshResult = false;
    /**
     * Assume everything in the namespace is deleted. As we discover the datasets from source, remove found entries
     * from these sets.
     */
    final StoragePlugin2 sourceRegistry = sourceRegistryMap.get(source);

    if(sourceRegistry == null){
      // V1 plugin support
      registry.refreshSourceMetadataInNamespace(source.getRoot(), policy);
      return true;
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    try{
      final Set<NamespaceKey> foundKeys = Sets.newHashSet(systemUserNamespaceService.getAllDatasets(source));

      final Set<NamespaceKey> orphanedFolders = Sets.newHashSet();
      for(NamespaceKey foundKey : foundKeys) {
        addFoldersOnPathToDeletedFolderSet(foundKey, orphanedFolders);
      }

      final Set<NamespaceKey> knownKeys = new HashSet<>();
      for(NamespaceKey foundKey : foundKeys) {

        Stopwatch stopwatchForDataset = Stopwatch.createStarted();
        // for each known dataset, update things.
        try {
          DatasetConfig config = systemUserNamespaceService.getDataset(foundKey);
          CheckResult result = CheckResult.UNCHANGED;

          if(sourceRegistry.datasetExists(foundKey)) {
            if(policy.getDatasetUpdateMode() == UpdateMode.PREFETCH ||
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

          if(result.getStatus() == UpdateStatus.DELETED){
            // TODO: handle exception
            systemUserNamespaceService.deleteDataset(foundKey, config.getVersion());
            refreshResult = true;
          } else {
            if(result.getStatus() == UpdateStatus.CHANGED) {
              completeSave(systemUserNamespaceService, result.getDataset(), config);
              refreshResult = true;
            }
            knownKeys.add(foundKey);
            removeFoldersOnPathFromOprhanSet(foundKey, orphanedFolders);
          }
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

    // Note: only lastRefreshDate updated, and not the metadata policy -- the metadata policy might be a fake one ('update now') passed in by tests, for example
    SourceConfig sourceConfig = systemUserNamespaceService.getSource(source);
    systemUserNamespaceService.addOrUpdateSource(source, sourceConfig.setLastRefreshDate(System.currentTimeMillis()));

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

    public PerSourceNamespaceUpdateTask(NamespaceKey sourceKey, String sourceName) {
      this.sourceKey = sourceKey;
      this.sourceName = sourceName;
    }

    @Override
    public void run() {
      // Task scheduled to run every 'getRefreshRateMillis()'
      try {
        StoragePlugin2 plugin2 = sourceRegistryMap.get(sourceKey);
        if (plugin2 != null) {
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
          long lastRefresh = config.getLastRefreshDate() == null ? 0 : config.getLastRefreshDate();
          final long refreshMs = Math.min(policy.getNamesRefreshMs(), policy.getDatasetDefinitionRefreshAfterMs());
          if (System.currentTimeMillis() > (refreshMs + lastRefresh)) {
            refreshSource(sourceKey, policy);
          }
        } else {
          // V1 plugin: update always
          StoragePlugin<?> plugin = registry.getPlugin(sourceName);
          registry.refreshSourceMetadataInNamespace(sourceName, plugin, SYSTEM_USERNAME);
        }
      } catch (Exception e) {
        // Exception while updating the metadata. Ignore, and try again later
        logger.warn(String.format("Failed to update namespace for plugin '%s'", sourceKey), e);
      }
    }
  }

  @Override
  public StoragePluginRegistry getOldRegistry() {
    return registry;
  }

  @Override
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
}
