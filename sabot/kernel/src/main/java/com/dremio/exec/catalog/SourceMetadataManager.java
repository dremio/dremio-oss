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

import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Provider;

import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.concurrent.Runnables;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.connector.metadata.extensions.SupportsReadSignature.MetadataValidity;
import com.dremio.datastore.KVStore;
import com.dremio.exec.catalog.Catalog.UpdateStatus;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.CatalogService.UpdateType;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

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
  private final NamespaceService systemNamespace;
  private final KVStore<NamespaceKey, SourceInternalData> sourceDataStore;
  private final ManagedStoragePlugin msp;
  private final OptionManager options;
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
      NamespaceService systemNamespace,
      KVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      final ManagedStoragePlugin msp,
      final OptionManager options
      ) {
    this.scheduler = scheduler;
    this.isMaster = isMaster;
    this.sourceKey = msp.getName();
    this.systemNamespace = systemNamespace;
    this.sourceDataStore = sourceDataStore;
    this.msp = msp;
    this.options = options;
    this.plugin = () -> msp.unwrap(StoragePlugin.class);
    this.refreshTask = Runnables.combo(new RefreshTask());
    this.saver = new DatasetSaver(systemNamespace,
        key -> localUpdateTime.put(key, System.currentTimeMillis()),
        options);
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
    final CountDownLatch wasRun = new CountDownLatch(1);
    final AtomicLong nextRefresh = new AtomicLong(0);
    final Cancellable task =
      scheduler.schedule(ScheduleUtils.scheduleToRunOnceNow(sourceKey.getRoot()), () ->  {
        try {
          MetadataPolicy policy = msp.getMetadataPolicy();
          if (policy == null) {
            policy = CatalogService.DEFAULT_METADATA_POLICY;
          }

          final Cancellable lastTaskR = lastTask;
          if (lastTaskR != null) {
            lastTaskR.cancel(false);
          }

          long lastNameRefresh = 0;
          long lastFullRefresh = 0;
          SourceInternalData srcData = sourceDataStore.get(sourceKey);
          if (srcData == null) {
            sourceDataStore.put(sourceKey, new SourceInternalData());
          }
          // NB: srcData could become null under a race between an unregisterSource() and this background metadata update task
          if (srcData != null) {
            if (lastRefreshTime == 0 && srcData.getLastFullRefreshDateMs() != null) {
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
          nextRefresh.set(Math.max(Math.min(nextNameRefresh, nextDatasetRefresh), System.currentTimeMillis()));

          // Default refresh time only used when there are issues in the per-source namespace update
          defaultRefreshMs = Math.min(policy.getNamesRefreshMs(), policy.getDatasetDefinitionRefreshAfterMs());
        } finally {
          wasRun.countDown();
        }
    });
    if (!task.isDone()) {
      try {
        wasRun.await();
      } catch (InterruptedException e) {
        logger.warn("InterruptedExeption while waiting for scheduleMetadataRefresh");
        Thread.currentThread().interrupt();
      }
    }
    // it needs a chance to be scheduled otherwise during task leader failover it won't
    // continue running on another node
    scheduleUpdate(nextRefresh.get());
  }

  private void scheduleUpdate(long nextStartTimeMs) {
    synchronized(runLock) {
      cancelWork = false;
      lastTask = scheduler.schedule(ScheduleUtils.scheduleForRunningOnceAt(Instant.ofEpochMilli(nextStartTimeMs),
        sourceKey.getRoot()), refreshTask);
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
    // TODO DX-14139 to fix behavior when node 1 does metadata refresh, while node 2 modifies/deletes source
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
        SourceConfig config = systemNamespace.getSource(sourceKey);
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
          logger.debug("Full update for source '{}'", sourceKey);
          lastFullRefresh = currentTimeMs;
          lastNameRefresh = currentTimeMs;
          refreshFull(null);
        } else if (currentTimeMs >= lastNameRefresh + policy.getNamesRefreshMs() - SCHEDULER_GRANULARITY_MS) {
          logger.debug("Name-only update for source '{}'", sourceKey);
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
        logger.warn("Failed to update namespace for plugin '{}'", sourceKey, e);
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
    final Set<NamespaceKey> existingDatasets = Sets.newHashSet(systemNamespace.getAllDatasets(sourceKey));

    boolean changed = false;

    try {
      final SourceMetadata sourceMetadata = plugin.get();
      if (sourceMetadata instanceof SupportsListingDatasets) {
        final SupportsListingDatasets listingProvider = (SupportsListingDatasets) sourceMetadata;
        final GetDatasetOption[] options = msp.getDefaultRetrievalOptions().asGetDatasetOptions(null);

        logger.debug("Source '{}' names sync started", sourceKey);
        try (DatasetHandleListing listing = listingProvider.listDatasetHandles(options)) {
          final Iterator<? extends DatasetHandle> iterator = listing.iterator();
          while (iterator.hasNext()) {
            final DatasetHandle handle = iterator.next();
            final NamespaceKey datasetKey = MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath());

            // names are only added, removal happens in full sync
            if (existingDatasets.remove(datasetKey)) {
              continue;
            }

            final DatasetConfig newConfig = MetadataObjectsUtils.newShallowConfig(handle);
            try {
              systemNamespace.addOrUpdateDataset(datasetKey, newConfig);
              changed = true;
            } catch (ConcurrentModificationException ignored) {
              // race condition
              logger.debug("Dataset '{}' add failed (CME)", datasetKey);
            }
          }
        }
      }

      // persist last refresh for across restarts.
      SourceInternalData srcData = sourceDataStore.get(sourceKey);

      if (srcData == null) {
        srcData = new SourceInternalData();
        srcData.setName(sourceKey.getRoot());
      }
      srcData.setLastNameRefreshDateMs(System.currentTimeMillis());
      sourceDataStore.put(sourceKey, srcData);

    } catch (Exception ex) {
      logger.warn("Source '{}' names sync failed. Names maybe incomplete", sourceKey, ex);
    }

    return changed;
  }

  private boolean refreshFull(MetadataPolicy metadataPolicy) throws NamespaceException {

    final DatasetRetrievalOptions retrievalOptions;
    if (metadataPolicy == null) {
      metadataPolicy = msp.getMetadataPolicy();
      retrievalOptions = msp.getDefaultRetrievalOptions(); // based on msp.getMetadataPolicy();
    } else {
      retrievalOptions = DatasetRetrievalOptions.fromMetadataPolicy(metadataPolicy)
          .toBuilder()
          .setMaxMetadataLeafColumns(msp.getMaxMetadataColumns())
          .build();
    }
    retrievalOptions.withFallback(DatasetRetrievalOptions.DEFAULT);

    if (metadataPolicy.getDatasetUpdateMode() == UpdateMode.UNKNOWN) {
      return false;
    }

    final MetadataSynchronizer synchronizeRun = new MetadataSynchronizer(systemNamespace, sourceKey, plugin.get(),
        metadataPolicy, saver, () -> cancelWork, retrievalOptions);
    synchronizeRun.setup();
    final MetadataSynchronizer.SyncStatus syncStatus = synchronizeRun.go();

    if (cancelWork) {
      return syncStatus.isRefreshed();
    }

    SourceInternalData srcData = sourceDataStore.get(sourceKey);
    if (srcData == null) {
      srcData = new SourceInternalData();
    }
    lastRefreshTime = System.currentTimeMillis();
    srcData.setLastFullRefreshDateMs(lastRefreshTime)
        .setLastNameRefreshDateMs(lastRefreshTime);
    sourceDataStore.put(sourceKey, srcData);

    return syncStatus.isRefreshed();
  }

  UpdateStatus refreshDataset(NamespaceKey datasetKey, DatasetRetrievalOptions options)
      throws ConnectorException, NamespaceException {
    options.withFallback(msp.getDefaultRetrievalOptions());

    DatasetConfig knownConfig = null;
    try {
      knownConfig = systemNamespace.getDataset(datasetKey);
    } catch (NamespaceNotFoundException ignored) {
    }
    final DatasetConfig currentConfig = knownConfig;

    final boolean exists = currentConfig != null;
    final boolean isExtended = exists && currentConfig.getReadDefinition() != null;
    final EntityPath entityPath = new EntityPath(datasetKey.getPathComponents());

    logger.debug("Dataset '{}' is being synced (exists: {}, isExtended: {})", datasetKey, exists, isExtended);
    final SourceMetadata sourceMetadata = plugin.get();
    final Optional<DatasetHandle> handle = sourceMetadata.getDatasetHandle(entityPath,
        options.asGetDatasetOptions(currentConfig));

    if (!handle.isPresent()) { // dataset is not in the source
      if (!exists) {
        throw new DatasetNotFoundException(entityPath);
      }

      if (!options.deleteUnavailableDatasets()) {
        logger.debug("Dataset '{}' unavailable, but not deleted", datasetKey);
        return UpdateStatus.UNCHANGED;
      }

      try {
        systemNamespace.deleteDataset(datasetKey, currentConfig.getTag());
        logger.trace("Dataset '{}' deleted", datasetKey);
        return UpdateStatus.DELETED;
      } catch (NamespaceException e) {
        logger.debug("Dataset '{}' delete failed", datasetKey, e);
        return UpdateStatus.UNCHANGED;
      }
    }

    final DatasetHandle datasetHandle = handle.get();
    if (!options.forceUpdate() && exists && isExtended && sourceMetadata instanceof SupportsReadSignature) {
      final SupportsReadSignature supportsReadSignature = (SupportsReadSignature) sourceMetadata;
      final DatasetMetadata currentExtended = new DatasetMetadataAdapter(currentConfig);

      final ByteString readSignature = currentConfig.getReadDefinition().getReadSignature();
      final MetadataValidity metadataValidity = supportsReadSignature.validateMetadata(
          readSignature == null ? BytesOutput.NONE : os -> ByteString.writeTo(os, readSignature),
          datasetHandle, currentExtended);

      if (metadataValidity == MetadataValidity.VALID) {
        logger.trace("Dataset '{}' metadata is valid, skipping", datasetKey);
        return UpdateStatus.UNCHANGED;
      }
    }

    final DatasetConfig datasetConfig;
    if (exists) {
      datasetConfig = currentConfig;
    } else {
      datasetConfig = MetadataObjectsUtils.newShallowConfig(datasetHandle);
    }

    saver.save(datasetConfig, datasetHandle, sourceMetadata, false, options);
    logger.trace("Dataset '{}' metadata saved to namespace", datasetKey);
    return UpdateStatus.CHANGED;
  }

  public long getLastFullRefreshDateMs() {
    return lastRefreshTime;
  }

  private final class RefreshTask implements Runnable {
    private final Stopwatch refreshWatch = Stopwatch.createUnstarted();

    @Override
    public void run() {
      // hold run lock so we block on replace/shutdown until the metadata refresh is stopped.
      synchronized(runLock) {
        logger.info("Source '{}' sync run started", sourceKey.getRoot());
        refreshWatch.reset();
        refreshWatch.start();
        doNextRefresh();
        refreshWatch.stop();
        logger.info("Source '{}' sync run ended. Took {} milliseconds", sourceKey.getRoot(), refreshWatch.elapsed(TimeUnit.MILLISECONDS));
      }
    }

    @Override
    public String toString() {
      return "metadata-refresh-" + sourceKey.getRoot();
    }
  }

}
