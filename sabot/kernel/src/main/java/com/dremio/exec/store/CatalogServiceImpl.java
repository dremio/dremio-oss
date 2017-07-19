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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.StoragePlugin2.CheckResult;
import com.dremio.exec.store.StoragePlugin2.UpdateStatus;
import com.dremio.service.BindingCreator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Implementation of catalog service.
 */
public class CatalogServiceImpl implements CatalogService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogServiceImpl.class);

  private final Map<NamespaceKey, StoragePlugin2> sourceRegistryMap = Maps.newConcurrentMap();
  private final Map<NamespaceKey, SourceState> sourceStateMap = Maps.newConcurrentMap();

  private final BindingCreator bindingCreator;
  private final Provider<SabotContext> context;
  private final boolean isMaster;
  private final boolean isCoordinator;

  private OptionManager options;
  private NamespaceUpdateThread updateThread;

  private NamespaceService systemUserNamespaceService;
  private StoragePluginRegistryImpl registry;

  public CatalogServiceImpl(Provider<SabotContext> context, BindingCreator bindingCreator, boolean isMaster, boolean isCoordinator) {
    this.context = context;
    this.isMaster = isMaster;
    this.isCoordinator = isCoordinator;
    this.bindingCreator = bindingCreator;
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
    this.registry = new StoragePluginRegistryImpl(context, this, context.getStoreProvider());
    registry.init();
    if(isCoordinator){
      updateThread = new NamespaceUpdateThread(isMaster, context.getOptionManager().getOption(ExecConstants.SOURCE_STATE_REFRESH_MIN) * 60 * 1000l);
      updateThread.start();
    }

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
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(registry, updateThread);
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
      if(config != null){
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
        definition = plugin.getDataset(key, null, false);
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
      registry.refreshSourceMetadataInNamespace(source.getRoot(), policy);
      return true;
    }

    try{

      final Set<NamespaceKey> foundKeys = Sets.newHashSet(systemUserNamespaceService.getAllDatasets(source));

      final Set<NamespaceKey> orphanedFolders = Sets.newHashSet();
      for(NamespaceKey foundKey : foundKeys) {
        addFoldersOnPathToDeletedFolderSet(foundKey, orphanedFolders);
      }

      final Set<NamespaceKey> knownKeys = new HashSet<>();
      for(NamespaceKey foundKey : foundKeys){

        // for each known dataset, update things.
        try {
          DatasetConfig config = systemUserNamespaceService.getDataset(foundKey);

          final CheckResult result;

          if(policy.getDatasetUpdateMode() == UpdateMode.PREFETCH || (policy.getDatasetUpdateMode() == UpdateMode.PREFETCH_QUERIED && config.getReadDefinition() != null)) {
            if(config.getReadDefinition() == null){
              // this is currently a name only dataset. Get the read definition.
              final SourceTableDefinition definition = sourceRegistry.getDataset(foundKey, config, false);
              result = new CheckResult(){
                @Override
                public UpdateStatus getStatus() {
                  return UpdateStatus.CHANGED;
                }
                @Override
                public SourceTableDefinition getDataset() {
                  return definition;
                }};
            } else {
              // have a read definition, need to check if it is up to date.
              result = sourceRegistry.checkReadSignature(config.getReadDefinition().getReadSignature(), config);
            }
          } else {
            if(sourceRegistry.datasetExists(foundKey)){
              result = CheckResult.UNCHANGED;
            }else{
              result = CheckResult.DELETED;
            }
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
      }

      for(SourceTableDefinition accessor : sourceRegistry.getDatasets(SYSTEM_USERNAME, false)){
        if(knownKeys.add(accessor.getName())){
          if(!accessor.isSaveable()){
            continue;
          }

          // this is a new datset, add it to namespace.
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
    try{
      // use key from namespace config
      NamespaceKey key = new NamespaceKey(nsConfig.getFullPathList());
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
      logger.warn("Failure while retrieving and saving dataset", ex);
    }
  }

  /**
   * Carry over few properties from old dataset config to new one
   * @param oldConfig old dataset config from namespace
   * @param newConfig new dataset config thats about to be saved in namespace
   */
  public static void copyFromOldConfig(DatasetConfig oldConfig, DatasetConfig newConfig) {
    newConfig.setId(oldConfig.getId());
    newConfig.setVersion(oldConfig.getVersion());
    newConfig.setCreatedAt(oldConfig.getCreatedAt());
    newConfig.setType(oldConfig.getType());
    newConfig.setFullPathList(oldConfig.getFullPathList());
    newConfig.setOwner(oldConfig.getOwner());
  }

  private void completeSave(NamespaceService namespaceService , SourceTableDefinition accessor, DatasetConfig oldConfig){
    try{
      NamespaceKey key = accessor.getName();

      DatasetConfig config = accessor.getDataset();
      if (oldConfig != null) {
        copyFromOldConfig(oldConfig, config);
      }
      List<DatasetSplit> splits = accessor.getSplits();
      namespaceService.addOrUpdateDataset(key, config, splits);
    }catch(Exception ex){
      logger.warn("Failure while retrieving and saving dataset", ex);
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
   * Background namespace refresher thread should only be ran coordinator nodes.
   */
  private class NamespaceUpdateThread extends Thread implements AutoCloseable {
    private final boolean isMaster;
    private final long stateRefresh;
    private long lastStateRefresh;

    public NamespaceUpdateThread(boolean isMaster, long stateRefresh) {
      super("metadata-refresh");
      setDaemon(true);
      this.isMaster = isMaster;
      this.stateRefresh = stateRefresh;
    }

    private void refreshSourceStates() {
      HashSet<String> v2pluginNames = new HashSet<>();
      for (Map.Entry<NamespaceKey, StoragePlugin2> entry : sourceRegistryMap.entrySet()) {
        final NamespaceKey source = entry.getKey();
        try {
          final StoragePlugin2 sourceRegistry = entry.getValue();
          v2pluginNames.add(source.getName());
          sourceStateMap.put(source, sourceRegistry.getState());
        } catch (Exception ex) {
          logger.warn("Failure refreshing source state for source {}.", source.getName(), ex);
        }
      }

      // Walk through the old storage plugins. Skip over the ones that have a StoragePlugin2.
      final StoragePluginRegistryImpl registry = CatalogServiceImpl.this.registry;
      Iterator<Map.Entry<String, StoragePlugin<?>>> v1Iter = registry.iterator();
      while (v1Iter.hasNext()) {
        final Map.Entry<String, StoragePlugin<?>> entry = v1Iter.next();
        final StoragePlugin<?> v1plugin = entry.getValue();
        final String v1pluginName = entry.getKey();

        // Skip over plugins that may have already been registered as StoragePlugin2 plugins.
        // Also skip over INFORMATION_SCHEMA and sys namespaces, which automatically get added.
        if (v1plugin.getStoragePlugin2() != null && v2pluginNames.contains(v1pluginName) ||
          StoragePluginRegistryImpl.isInternal(v1pluginName) ||
          v1pluginName.equals("INFORMATION_SCHEMA") ||
          v1pluginName.equals("sys")) {
          continue;
        }

        try {
          registry.refreshSourceMetadataInNamespace(v1pluginName, v1plugin, SYSTEM_USERNAME);
        } catch (NamespaceException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    @Override
    public void run() {
      while(true){

        // let's not start refreshing until the first interval completes.
        if(sleepExit()){
          return;
        }

        if(System.currentTimeMillis() > lastStateRefresh + stateRefresh){
          refreshSourceStates();
          lastStateRefresh = System.currentTimeMillis();
        }

        if(!isMaster){
          // metadata collection is a master-only proposition.
          return;
        }

        for (Map.Entry<NamespaceKey, StoragePlugin2> entry : sourceRegistryMap.entrySet()) {
          try {

            final NamespaceKey source = entry.getKey();
            SourceState state = sourceStateMap.get(source);
            if(state == null || state.getStatus() == SourceStatus.bad){
              logger.info("Ignoring metadata load for source {} since it is currently in a bad state.", source.getName());
              continue;
            }

            SourceConfig config = systemUserNamespaceService.getSource(source);
            MetadataPolicy policy = config.getMetadataPolicy();
            if (policy == null) {
              policy = DEFAULT_METADATA_POLICY;
            }
            long lastRefresh = config.getLastRefreshDate() == null ? 0 : config.getLastRefreshDate();
            final long refreshMs = Math.min(policy.getNamesRefreshMs(), policy.getDatasetDefinitionTtlMs());
            if(System.currentTimeMillis() > (refreshMs + lastRefresh)) {
              refreshSource(source, policy);
              systemUserNamespaceService.addOrUpdateSource(source, config.setLastRefreshDate(System.currentTimeMillis()));
            }

          } catch (Throwable e) {
            logger.warn(String.format("Failed to update namespace for plugin '%s'", entry.getKey()), e);
          }
        }

      }
    }

    private boolean sleepExit(){
      try{
        Thread.sleep(getRefreshRateMillis());
        return false;
      }catch(InterruptedException ex){
        logger.debug("Metadata maintenance thread exiting.");
        return true;
      }
    }
    @Override
    public void close() throws Exception {
      this.interrupt();
    }
  }

  @Override
  public StoragePluginRegistry getOldRegistry() {
    return registry;
  }
}
