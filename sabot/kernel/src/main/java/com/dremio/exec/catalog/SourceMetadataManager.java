/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.exceptions.UserException;
import com.dremio.concurrent.Runnables;
import com.dremio.datastore.KVStore;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.CatalogService.UpdateType;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePlugin.CheckResult;
import com.dremio.exec.store.StoragePlugin.UpdateStatus;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;

/**
 * Responsible for synchronizing source metadata. Schedules regular metadata updates along with
 * recording update status to the SourceInternalData store.
 */
class SourceMetadataManager implements AutoCloseable {

  private static final long MAXIMUM_CACHE_SIZE = 10_000L;

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SourceMetadataManager.class);

  private static final long SCHEDULER_GRANULARITY_MS = 1 * 1000;

  // Stores the time (in milliseconds, obtained from System.currentTimeMillis()) at which a dataset was locally updated
  private final Cache<NamespaceKey, Long> localUpdateTime =
    CacheBuilder.newBuilder()
    .maximumSize(MAXIMUM_CACHE_SIZE)
    .build();

  private final SchedulerService scheduler;
  private final DatasetSaver saver;
  private final boolean isMaster;
  private final NamespaceKey sourceKey;
  private final NamespaceService systemUserNamespaceService;
  private final KVStore<NamespaceKey, SourceInternalData> sourceDataStore;
  private final ManagedStoragePlugin msp;
  private final Provider<StoragePlugin> plugin;
  private final Runnable refreshTask;
  private final Object runLock = new Object();

  private volatile Cancellable lastTask;
  private volatile long defaultRefreshMs;
  private volatile boolean cancelWork;
  private volatile long lastRefreshTime = 0;

  public SourceMetadataManager(
      SchedulerService scheduler,
      boolean isMaster,
      NamespaceService systemUserNamespaceService,
      KVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      final ManagedStoragePlugin msp
      ) {
    this.scheduler = scheduler;
    this.isMaster = isMaster;
    this.sourceKey = msp.getName();
    this.systemUserNamespaceService = systemUserNamespaceService;
    this.sourceDataStore = sourceDataStore;
    this.msp = msp;
    this.plugin = () -> msp.unwrap(StoragePlugin.class);
    this.refreshTask = Runnables.combo(new RefreshTask());
    this.saver = new DatasetSaver(systemUserNamespaceService,
        key -> localUpdateTime.put(key, System.currentTimeMillis()));
  }

  DatasetSaver getSaver() {
    return saver;
  }

  boolean refresh(UpdateType updateType, MetadataPolicy policy) throws NamespaceException {
    switch(updateType) {
    case FULL:
      return refreshFull(policy);
    case NAMES:
      return refreshSourceNames();
    case NONE:
      return false;
    default:
      throw new IllegalArgumentException("Unknown type: " + updateType);

    }
  }

  /**
   * Initialize the automated refresh of the metadata associated with this plugin.
   */
  public void scheduleMetadataRefresh() {
    if (!isMaster) {
      // metadata refresh is a master-only proposition.
      return;
    }

    MetadataPolicy policy = msp.getMetadataPolicy();
    if (policy == null) {
      policy = CatalogService.DEFAULT_METADATA_POLICY;
    }

    final Cancellable lastTask = this.lastTask;
    if (lastTask != null) {
      lastTask.cancel(false);
    }

    long lastNameRefresh = 0;
    long lastFullRefresh = 0;
    SourceInternalData srcData = sourceDataStore.get(sourceKey);
    if(srcData == null) {
      sourceDataStore.put(sourceKey, new SourceInternalData());
    }
    // NB: srcData could become null under a race between an unregisterSource() and this background metadata update task
    if (srcData != null) {
      if (lastRefreshTime == 0 && srcData.getLastFullRefreshDateMs() != null){
        // Initialize 'lastFullRefresh' from the KV store
        lastRefreshTime = srcData.getLastFullRefreshDateMs().longValue();
      }
      lastNameRefresh = (srcData.getLastNameRefreshDateMs() == null) ? 0 : srcData.getLastNameRefreshDateMs();
      lastFullRefresh = (srcData.getLastFullRefreshDateMs() == null) ? 0 : srcData.getLastFullRefreshDateMs();
    }
    final long nextNameRefresh = lastNameRefresh + policy.getNamesRefreshMs();
    final long nextDatasetRefresh = lastFullRefresh + policy.getDatasetDefinitionRefreshAfterMs();
    // Next scheduled time to run is whichever comes first (name or dataset refresh), but no earlier than 'now'
    // Please note: upon the initial addition of a source, refresh(UpdateType.NAMES, null) will be
    // called, which then sets the last name refresh date. However, the last dataset refresh remains
    // null, which triggers the Math.max() below, and causes the next refresh time to be exactly 'now'
    final long nextRefresh = Math.max(Math.min(nextNameRefresh, nextDatasetRefresh), System.currentTimeMillis());

    // Default refresh time only used when there are issues in the per-source namespace update
    this.defaultRefreshMs = Math.min(policy.getNamesRefreshMs(), policy.getDatasetDefinitionRefreshAfterMs());
    scheduleUpdate(nextRefresh);
  }

  private void scheduleUpdate(long nextStartTimeMs) {
    synchronized(runLock) {
      cancelWork = false;
      lastTask = scheduler.schedule(ScheduleUtils.scheduleForRunningOnceAt(Instant.ofEpochMilli(nextStartTimeMs)), refreshTask);
    }
  }

  void cancelRefreshTask() {
    cancelWork = true;
    synchronized(runLock) {
      Cancellable lastTask = this.lastTask;
      if(lastTask != null && !lastTask.isCancelled()) {
        lastTask.cancel(false);
      }

      return;
    }
  }

  @Override
  public void close() {
    cancelRefreshTask();
  }

  /**
   * Checks if the entry is valid.
   *
   * @param options metadata request options
   * @param config dataset config
   * @return true iff entry is valid
   */
  boolean isStillValid(MetadataRequestOptions options, DatasetConfig config) {
    final NamespaceKey key = new NamespaceKey(config.getFullPathList());
    final Long updateTime = localUpdateTime.getIfPresent(key);
    final long currentTime = System.currentTimeMillis();
    final long expiryTime = msp.getMetadataPolicy().getDatasetDefinitionExpireAfterMs();

    // check if the entry is expired
    if (
        // request marks this expired
        options.getMaxRequestTime() < currentTime
        ||
        // dataset was locally updated too long ago (or never)
        ((updateTime == null || updateTime + expiryTime < currentTime) &&
            // AND dataset was globally updated too long ago
            lastRefreshTime + expiryTime < currentTime)
        ||
        // request marks this dataset as invalid
        !options.getSchemaConfig().getDatasetValidityChecker().apply(config)
        ) {
      return false;
    }

    return true;
  }

  private void doNextRefresh() {
    if(cancelWork) {
      return;
    }

    try(AutoCloseableLock l = msp.readLock()) {
      long nextUpdateMs = System.currentTimeMillis() + defaultRefreshMs;
      try {
        msp.refreshState();
        SourceState sourceState = msp.getState();
        if (sourceState == null || sourceState.getStatus() == SourceStatus.bad) {
          logger.info("Ignoring metadata load for source {} since it is currently in a bad state.", sourceKey);
          return;
        }
        SourceConfig config = systemUserNamespaceService.getSource(sourceKey);
        MetadataPolicy policy = config.getMetadataPolicy();
        if (policy == null) {
          policy = CatalogService.DEFAULT_METADATA_POLICY;
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
          refreshFull(null);
        } else if (currentTimeMs >= lastNameRefresh + policy.getNamesRefreshMs() - SCHEDULER_GRANULARITY_MS) {
          logger.debug(String.format("Name-only update for source '%s'", sourceKey));
          lastNameRefresh = currentTimeMs;
          refreshSourceNames();
        }
        currentTimeMs = System.currentTimeMillis(); // re-obtaining the time, because metadata updates can take a very long time
        long nextNameRefresh = lastNameRefresh + policy.getNamesRefreshMs();
        long nextFullRefresh = lastFullRefresh + policy.getDatasetDefinitionRefreshAfterMs();
        // Next update opportunity is at the earlier of the two updates, but no earlier than 'now'
        nextUpdateMs = Math.max(Math.min(nextNameRefresh, nextFullRefresh), currentTimeMs);
      } catch (Exception e) {
        // Exception while updating the metadata. Ignore, and try again later
        logger.warn(String.format("Failed to update namespace for plugin '%s'", sourceKey), e);
      } finally {
        if (!cancelWork) {
          // This tries to fetch refresh thread runLock which is already occupied by this thread, so it shouldn't
          // have any problem
          scheduleUpdate(nextUpdateMs);
        }
      }
    }
  }


  private boolean refreshSourceNames() throws NamespaceException {
    final Set<NamespaceKey> foundKeys = Sets.newHashSet(systemUserNamespaceService.getAllDatasets(sourceKey));

    boolean changed = false;

    try {
      for (SourceTableDefinition accessor : plugin.get()
          .getDatasets(SYSTEM_USERNAME, msp.getDefaultRetrievalOptions())) {
        // names only added, never removed. Removal can be done by the full refresh (refreshSource())
        if (!foundKeys.contains(accessor.getName()) && accessor.isSaveable()) {
          try {
            saver.shallowSave(accessor);
            changed = true;
          } catch (ConcurrentModificationException ex) {
            logger.debug("Concurrent update, ignoring.", ex);
          }
        }
      }

      // persist last refresh for across restarts.
      SourceInternalData srcData = sourceDataStore.get(sourceKey);

      if(srcData == null) {
        srcData = new SourceInternalData();
        srcData.setName(sourceKey.getRoot());
      }
      srcData.setLastNameRefreshDateMs(System.currentTimeMillis());
      sourceDataStore.put(sourceKey, srcData);

    } catch (Exception ex){
      logger.warn("Failure while attempting to update metadata for source {}. Terminating update of this source.", sourceKey, ex);
    }

    return changed;

  }

  private boolean refreshFull(MetadataPolicy metadataPolicy) throws NamespaceException {
    if(metadataPolicy == null) {
      metadataPolicy = msp.getMetadataPolicy();
    }

    final DatasetRetrievalOptions retrievalOptions = DatasetRetrievalOptions.fromMetadataPolicy(metadataPolicy)
        .withFallback(DatasetRetrievalOptions.DEFAULT);

    boolean refreshResult = false;
    /*
     * Assume everything in the namespace is deleted. As we discover the datasets from source, remove found entries
     * from these sets.
     */
    Stopwatch stopwatch = Stopwatch.createStarted();
    try{
      final Set<NamespaceKey> foundKeys = Sets.newHashSet(systemUserNamespaceService.getAllDatasets(sourceKey));

      final Set<NamespaceKey> orphanedFolders = Sets.newHashSet();
      for(NamespaceKey foundKey : foundKeys) {
        addFoldersOnPathToDeletedFolderSet(foundKey, orphanedFolders);
      }

      final Set<NamespaceKey> knownKeys = new HashSet<>();
      for(NamespaceKey foundKey : foundKeys) {
        // Refresh might take a long time. Quit if the daemon is closing, to avoid shutdown issues
        if (cancelWork) {
          logger.info("Aborting update of metadata for table {} -- service is closing.", foundKey);
          return refreshResult;
        }

        Stopwatch stopwatchForDataset = Stopwatch.createStarted();
        // for each known dataset, update things.
        try {
          DatasetConfig config = systemUserNamespaceService.getDataset(foundKey);
          CheckResult result = CheckResult.UNCHANGED;

          if (plugin.get().datasetExists(foundKey)) {
            if (metadataPolicy.getDatasetUpdateMode() == UpdateMode.PREFETCH ||
                (metadataPolicy.getDatasetUpdateMode() == UpdateMode.PREFETCH_QUERIED &&
                    config.getReadDefinition() != null)) {
              if (config.getReadDefinition() == null) {
                // this is currently a name only dataset. Get the read definition.
                final SourceTableDefinition definition = plugin.get().getDataset(foundKey, config, retrievalOptions);
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
                result = plugin.get()
                    .checkReadSignature(config.getReadDefinition().getReadSignature(), config, retrievalOptions);
              }
            }
          } else {
            result = CheckResult.DELETED;
          }

          if (result.getStatus() == UpdateStatus.DELETED) {
            if (!retrievalOptions.deleteUnavailableDatasets()) {
              logger.debug("Unavailable dataset '{}' will not be deleted", foundKey);
            } else {
              // TODO: handle exception
              systemUserNamespaceService.deleteDataset(foundKey, config.getTag());
              refreshResult = true;
            }
          } else {
            if (result.getStatus() == UpdateStatus.CHANGED) {
              saver.completeSave(result.getDataset(), config);
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

      for(SourceTableDefinition accessor : plugin.get().getDatasets(SYSTEM_USERNAME, retrievalOptions)) {
        if (cancelWork) {
          return refreshResult;
        }
        if(knownKeys.add(accessor.getName())){
          if(!accessor.isSaveable()){
            continue;
          }

          // this is a new dataset, add it to namespace.
          try {
            if(metadataPolicy.getDatasetUpdateMode() != UpdateMode.PREFETCH){
              saver.shallowSave(accessor);
            } else {
              saver.completeSave(accessor, null);
            }
            refreshResult = true;
          } catch (ConcurrentModificationException ex) {
            logger.debug("There was a concurrent exception while trying to refresh a dataset. This generally means another user did something that conflicted with the refresh mechnaism.", ex);
          }
          removeFoldersOnPathFromOprhanSet(accessor.getName(), orphanedFolders);
        }
      }

      for(NamespaceKey folderKey : orphanedFolders) {
        if (cancelWork) {
          return refreshResult;
        }
        try {
          final FolderConfig folderConfig = systemUserNamespaceService.getFolder(folderKey);
          systemUserNamespaceService.deleteFolder(folderKey, folderConfig.getTag());
          refreshResult = true;
        } catch (NamespaceNotFoundException ex) {
          // no-op
        } catch (NamespaceException ex) {
          logger.warn("Failed to delete dataset from Namespace ");
        }
      }
    } catch (Exception ex){
      logger.warn("Failure while attempting to update metadata for source {}. Terminating update of this source.", sourceKey, ex);
    }
    finally {
       stopwatch.stop();
       if (logger.isDebugEnabled()) {
         logger.debug("Metadata refresh for source : {} took {} milliseconds.", sourceKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));
       }
    }

    if (cancelWork) {
      return refreshResult;
    }

    SourceInternalData srcData = sourceDataStore.get(sourceKey);
    if(srcData == null) {
      srcData = new SourceInternalData();
    }
    lastRefreshTime = System.currentTimeMillis();
    srcData.setLastFullRefreshDateMs(lastRefreshTime).setLastNameRefreshDateMs(lastRefreshTime);
    sourceDataStore.put(sourceKey, srcData);

    return refreshResult;
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

  UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    retrievalOptions.withFallback(msp.getDefaultRetrievalOptions());

    DatasetConfig config = null;
    try {
      config = systemUserNamespaceService.getDataset(key);
    }catch (NamespaceException ex){
      logger.debug("Failure while trying to retrieve dataset for key {}.", key, ex);
    }

    SourceTableDefinition definition;
    try {
      if (config != null && config.getReadDefinition() != null) {
        final CheckResult result = plugin.get()
            .checkReadSignature(config.getReadDefinition().getReadSignature(), config, retrievalOptions);

        switch (result.getStatus()) {
        case DELETED:
          if (!retrievalOptions.deleteUnavailableDatasets()) {
            logger.debug("Unavailable dataset '{}' will not be deleted", key);
            return UpdateStatus.UNCHANGED;
          }

          try {
            systemUserNamespaceService.deleteDataset(key, config.getTag());
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
        definition = plugin.get().getDataset(key, config, retrievalOptions);
      }
    } catch (Exception ex) {
      throw UserException.dataReadError(ex)
          .message("Failure while attempting to read metadata for table [%s] from source", key)
          .build(logger);
    }

    if (definition == null) {

      if (config != null) {
        if (!retrievalOptions.deleteUnavailableDatasets()) {
          logger.debug("Unavailable dataset '{}' will not be deleted", key);
          return UpdateStatus.UNCHANGED;
        }

        // unable to find request table in the plugin, but somehow the entry exists in namespace, so delete it
        try {
          systemUserNamespaceService.deleteDataset(key, config.getTag());
          return UpdateStatus.DELETED;
        } catch (NamespaceNotFoundException ignored) {
        } catch (Exception e) {
          throw UserException.dataReadError(e)
              .message("Failure while attempting to delete table entry [%s] from namespace", key)
              .build(logger);
        }
      }

      throw UserException.validationError()
          .message("Unable to find requested table [%s]", key)
          .build(logger);
    }

    saver.completeSave(definition, config);
    return UpdateStatus.CHANGED;
  }

  public long getLastFullRefreshDateMs() {
    return lastRefreshTime;
  }

  private final class RefreshTask implements Runnable {

    @Override
    public void run() {
      // hold run lock so we block on replace/shutdown until the metadata refresh is stopped.
      synchronized(runLock) {
        doNextRefresh();
      }
    }

    @Override
    public String toString() {
      return "metadata-refresh-" + sourceKey.getRoot();
    }
  }

}
