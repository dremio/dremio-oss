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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.dremio.common.collections.Tuple;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.UnsupportedDatasetHandleListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.connector.metadata.extensions.SupportsReadSignature.MetadataValidity;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshUtils;
import com.dremio.exec.store.metadatarefresh.SupportsUnlimitedSplits;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Synchronizes metadata from the connector to the namespace.
 */
public class MetadataSynchronizer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataSynchronizer.class);

  private static final int NUM_RETRIES = 1;
  private final SyncStatus syncStatus = new SyncStatus(true);

  private final NamespaceService systemNamespace;
  private final NamespaceKey sourceKey;
  private final SourceMetadata sourceMetadata;
  private final ManagedStoragePlugin.MetadataBridge bridge;
  private final DatasetSaver saver;
  private final DatasetRetrievalOptions options;

  private final UpdateMode updateMode;
  private final Set<NamespaceKey> ancestorsToKeep;
  private final List<Tuple<String, String>> failedDatasets;
  private final OptionManager optionManager;
  private final Orphanage orphanage;

  private Set<NamespaceKey> orphanedDatasets;

  MetadataSynchronizer(
      NamespaceService systemNamespace,
      NamespaceKey sourceKey,
      ManagedStoragePlugin.MetadataBridge bridge,
      MetadataPolicy metadataPolicy,
      DatasetSaver saver,
      DatasetRetrievalOptions options,
      OptionManager optionManager
  ) {
    this.systemNamespace = Preconditions.checkNotNull(systemNamespace);
    this.sourceKey = Preconditions.checkNotNull(sourceKey);
    this.bridge = Preconditions.checkNotNull(bridge);
    this.sourceMetadata = Preconditions.checkNotNull(bridge.getMetadata());
    this.saver = saver;
    this.options = options;

    this.updateMode = metadataPolicy.getDatasetUpdateMode();
    this.ancestorsToKeep = new HashSet<>();
    this.failedDatasets = new ArrayList<>();
    this.optionManager = optionManager;
    this.orphanage = bridge.getOrphanage();
  }

  /**
   * Set up the synchronizer.
   */
  public void setup() throws NamespaceException {
    Preconditions.checkState(updateMode == UpdateMode.PREFETCH || updateMode == UpdateMode.PREFETCH_QUERIED,
        "only PREFETCH and PREFETCH_QUERIED are supported");

    // Initially we assume all datasets are orphaned, until we discover they still exist in the source
    orphanedDatasets = Sets.newHashSet(systemNamespace.getAllDatasets(sourceKey));
    ancestorsToKeep.add(sourceKey);

    logger.debug("Source '{}' sync setup ({} datasets)", sourceKey, orphanedDatasets.size());
    logger.trace("Source '{}' has datasets: '{}'", sourceKey, orphanedDatasets);
  }

  /**
   * Perform synchronization.
   *
   * @return status
   */
  public SyncStatus go() {

    final Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      logger.debug("Source '{}' sync started", sourceKey);

      // 1. update datasets in namespace with the ones in source
      synchronizeDatasets();

      // remaining entries in orphanedDatasets must be deleted

      // 2. delete all the folders that have no references
      deleteOrphanFolders();

      // 3. delete all the orphaned datasets
      deleteOrphanedDatasets();
    } catch (ManagedStoragePlugin.StoragePluginChanging e) {
      syncStatus.setInterrupted(true);
      logger.info("Source '{}' sync aborted due to plugin changing during the sync. Will try again later", sourceKey);
    } catch (Exception e) {
      logger.warn("Source '{}' sync failed unexpectedly. Will try again later", sourceKey, e);
    } finally {
      if (!failedDatasets.isEmpty()) {
        logger.warn("Source '{}' sync failed for {} datasets. Few failed datasets and reasons:\n{}",
            sourceKey,
            failedDatasets.size(),
            failedDatasets.stream()
                .map(tuple -> "\t" + tuple.first + ": " + tuple.second)
                .limit(10)
                .collect(Collectors.joining("\n"))
        );
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Source '{}' sync ended. Took {} milliseconds",
            sourceKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));
      }
    }

    return syncStatus;
  }

  private DatasetHandleListing getDatasetHandleListing(GetDatasetOption... options) throws ConnectorException {
    if (sourceMetadata instanceof SupportsListingDatasets) {
      return ((SupportsListingDatasets) sourceMetadata).listDatasetHandles(options);
    }

    return new NamespaceListing(systemNamespace, sourceKey, sourceMetadata, this.options);
  }

  /**
   * Brings the namespace up to date by gathering metadata from the source about existing and new datasets.
   *
   * @throws NamespaceException if it cannot be handled due to namespace error
   * @throws ConnectorException if it cannot be handled due to an error in the source connection
   */
  private void synchronizeDatasets() throws NamespaceException, ConnectorException {
    logger.debug("Source '{}' syncing datasets", sourceKey);
    try (DatasetHandleListing datasetListing = getDatasetHandleListing(options.asGetDatasetOptions(null))) {
      if (datasetListing instanceof UnsupportedDatasetHandleListing) {
        logger.debug("Source '{}' does not support listing datasets, assuming all are valid", sourceKey);
        orphanedDatasets.clear();
        return;
      }
      final Iterator<? extends DatasetHandle> iterator = datasetListing.iterator();
      do {
        try {
          if (!iterator.hasNext()) {
            break;
          }
          final DatasetHandle handle = iterator.next();
          final NamespaceKey datasetKey = MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath());
          final boolean existing = orphanedDatasets.remove(datasetKey);
          if (logger.isTraceEnabled()) {
            logger.trace("Dataset '{}' sync started ({})", datasetKey, existing ? "existing" : "new");
          }
          if (existing) {
            addAncestors(datasetKey, ancestorsToKeep);
            handleExistingDataset(datasetKey, handle);
          } else {
            handleNewDataset(datasetKey, handle);
          }
        } catch (DatasetMetadataTooLargeException e) {
          final boolean existing = orphanedDatasets.remove(new NamespaceKey(PathUtils.parseFullPath(e.getMessage())));
          logger.error("Dataset {} sync failed ({}) due to Metadata too large. Please check.", e.getMessage(), existing ? "existing" : "new");
        }
      } while (true);
    }
  }

  /**
   * Handle metadata sync for the given existing dataset.
   *
   * @param datasetKey dataset key
   * @param handle     dataset handle
   */
  private void handleExistingDataset(NamespaceKey datasetKey, DatasetHandle handle) {
    int tryCount = 0;
    while (true) {
      if (tryCount++ > NUM_RETRIES) {
        logger.debug("Dataset '{}' sync failed {} times (CME). Will retry next sync", datasetKey, NUM_RETRIES);
        break;
      }

      final Stopwatch stopwatch = Stopwatch.createStarted();
      try {
        tryHandleExistingDataset(datasetKey, handle);
        break;
      } catch (ConcurrentModificationException ignored) {
        // retry
        // continue;
      } catch (DatasetNotFoundException e) {
        // race condition: metadata will be removed from catalog in next sync
        logger.debug("Dataset '{}' is no longer valid, skipping sync", datasetKey, e);
        break;
      } catch (Exception e) {
        // TODO: this should not be an Exception. Once exception handling is defined, change this. This is unfortunately
        //  the current behavior.
        logger.debug("Dataset '{}' sync failed unexpectedly. Will retry next sync", datasetKey, e);
        failedDatasets.add(Tuple.of(datasetKey.getSchemaPath(), e.getMessage()));
        syncStatus.incrementExtendedUnreadable();
        break;
      } finally {
        if (logger.isDebugEnabled()) {
          logger.debug("Dataset '{}' sync took {} milliseconds",
              datasetKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
      }
    }
  }

  /**
   * Try handling metadata sync for the given existing dataset.
   *
   * @param datasetKey    dataset key
   * @param datasetHandle dataset handle
   * @throws NamespaceException if it cannot be handled
   */
  private void tryHandleExistingDataset(NamespaceKey datasetKey, DatasetHandle datasetHandle)
      throws NamespaceException, ConnectorException {
    // invariant: only metadata attributes of currentConfig are overwritten, and then the same currentConfig is saved,
    // so the rest of the attributes are as is; so CME is handled by retrying this entire block

    final DatasetConfig currentConfig;
    try {
      currentConfig = systemNamespace.getDataset(datasetKey);
    } catch (NamespaceNotFoundException ignored) {
      // race condition
      logger.debug("Dataset '{}' no longer in namespace, skipping", datasetKey);
      return;
    }

    final boolean isExtended = currentConfig.getReadDefinition() != null;
    if (updateMode == UpdateMode.PREFETCH_QUERIED && !isExtended) {
      // this run only refreshes, and not create new entries

      logger.trace("Dataset '{}' does not have extended attributes, skipping", datasetKey);
      syncStatus.incrementShallowUnchanged();
      return;
    }

    if (isExtended && sourceMetadata instanceof SupportsReadSignature) {
      String user = SystemUser.SYSTEM_USERNAME;
      if (options.datasetRefreshQuery().isPresent()) {
        user = options.datasetRefreshQuery().get().getUser();
      }
      boolean supportsIcebergMetadata = (sourceMetadata instanceof SupportsUnlimitedSplits) &&
              ((SupportsUnlimitedSplits) sourceMetadata).allowUnlimitedSplits(datasetHandle, currentConfig, user);
      final boolean isIcebergMetadata = currentConfig.getPhysicalDataset() != null &&
              Boolean.TRUE.equals(currentConfig.getPhysicalDataset().getIcebergMetadataEnabled());
      final boolean unlimitedSplitsSupportEnabled = MetadataRefreshUtils.unlimitedSplitsSupportEnabled(optionManager);
      final boolean forceUpdateNotRequired = !supportsIcebergMetadata || isIcebergMetadata || !unlimitedSplitsSupportEnabled;

      if (forceUpdateNotRequired) {
        final SupportsReadSignature supportsReadSignature = (SupportsReadSignature) sourceMetadata;
        final DatasetMetadata currentExtended = new DatasetMetadataAdapter(currentConfig);
        final ByteString readSignature = currentConfig.getReadDefinition().getReadSignature();
        final MetadataValidity metadataValidity = supportsReadSignature.validateMetadata(
                readSignature==null ? BytesOutput.NONE:os -> ByteString.writeTo(os, readSignature),
                datasetHandle, currentExtended);
        if (metadataValidity==MetadataValidity.VALID) {
          logger.trace("Dataset '{}' metadata is valid, skipping", datasetKey);
          syncStatus.incrementExtendedUnchanged();
          return;
        }
      }
    }

    saver.save(currentConfig, datasetHandle, sourceMetadata, false, options);
    logger.trace("Dataset '{}' metadata saved to namespace", datasetKey);
    syncStatus.setRefreshed();
    syncStatus.incrementExtendedChanged();
  }

  /**
   * Handle new dataset based on the metadata policy.
   *
   * @param datasetKey dataset key
   * @param handle     dataset handle
   * @throws NamespaceException if it cannot be handled
   */
  private void handleNewDataset(NamespaceKey datasetKey, DatasetHandle handle)
      throws NamespaceException {
    switch (updateMode) {

    case PREFETCH:
      // this mode will soon be deprecated, for now save, perform name sync

      // fall-through

    case PREFETCH_QUERIED: {
      final DatasetConfig newConfig = MetadataObjectsUtils.newShallowConfig(handle);
      try {
        systemNamespace.addOrUpdateDataset(datasetKey, newConfig);
        syncStatus.setRefreshed();
        syncStatus.incrementShallowAdded();
      } catch (ConcurrentModificationException ignored) {
        // race condition
        logger.debug("Dataset '{}' add failed (CME)", datasetKey);
      }
      return;
    }

    default:
      throw new IllegalStateException("unknown dataset update mode: " + updateMode);
    }
  }

  /**
   * Delete orphan folders. These are folders that are no longer contain datasets.
   *
   */
  private void deleteOrphanFolders() {
    /*
    if (!options.deleteUnavailableDatasets()) {
      logger.debug("Source '{}' in state {} may have orphaned folders, not deleting them to honor source property",
        sourceKey, bridge.getState());
      return;
    }
    */

    logger.debug("Source '{}' recursively deleting orphan folders", sourceKey);
    for (NamespaceKey toBeDeleted : orphanedDatasets) {

      final Iterator<NamespaceKey> ancestors = getAncestors(toBeDeleted);

      while (ancestors.hasNext()) {
        final NamespaceKey ancestorKey = ancestors.next();
        if (ancestorsToKeep.contains(ancestorKey)) {
          continue;
        }

        try {
          final FolderConfig folderConfig = systemNamespace.getFolder(ancestorKey);
          systemNamespace.deleteFolder(ancestorKey, folderConfig.getTag());
          logger.trace("Folder '{}' deleted", ancestorKey);
          syncStatus.setRefreshed();
        } catch (NamespaceNotFoundException ignored) {
          // either race condition, or ancestorKey is not a folder
          logger.debug("Folder '{}' not found", ancestorKey);
        } catch (NamespaceException ex) {
          logger.debug("Folder '{}' delete failed", ancestorKey, ex);
        }
      }
    }
  }

  /**
   * Deleted orphan datasets. These are datasets that are no longer present in the source.
   *
   */
  private void deleteOrphanedDatasets() {
    if (!options.deleteUnavailableDatasets()) {
      logger.debug("Source '{}' in state {} has {} unavailable datasets, but not deleted: {}",
        sourceKey, bridge.getState(), orphanedDatasets.size(), orphanedDatasets);
      return;
    }

    if (orphanedDatasets.size() > 0) {
      logger.info("Source '{}' in state {} has {} unavailable datasets to be deleted: {}",
        sourceKey, bridge.getState(), orphanedDatasets.size(), orphanedDatasets.stream().limit(Math.min(orphanedDatasets.size(),100)).collect(Collectors.toSet()));
    }

    for (NamespaceKey toBeDeleted : orphanedDatasets) {

      final DatasetConfig datasetConfig;
      try {
        datasetConfig = systemNamespace.getDataset(toBeDeleted);
        if (CatalogUtil.hasIcebergMetadata(datasetConfig)) {
          CatalogUtil.addIcebergMetadataOrphan(datasetConfig, orphanage);
        }
        systemNamespace.deleteDataset(toBeDeleted, datasetConfig.getTag());
        syncStatus.setRefreshed();
        if (datasetConfig.getReadDefinition() == null) {
          syncStatus.incrementShallowDeleted();
        } else {
          syncStatus.incrementExtendedDeleted();
        }
        logger.trace("Dataset '{}' deleted", toBeDeleted);
      } catch (NamespaceNotFoundException ignored) {
        // race condition - this is expected if the dataset was contained in an orphaned folder recursively deleted
        logger.debug("Dataset '{}' not found", toBeDeleted);
        // continue;
      } catch (NamespaceException e) {
        logger.warn("Dataset '{}' to be deleted, but lookup failed", toBeDeleted, e);
        failedDatasets.add(Tuple.of(toBeDeleted.getSchemaPath(), e.getMessage()));
        // continue;
      }
    }
  }

  private static void addAncestors(NamespaceKey datasetKey, Set<NamespaceKey> ancestors) {
    NamespaceKey key = datasetKey.getParent();
    while (key.hasParent()) {
      ancestors.add(key);
      key = key.getParent();
    }
  }

  private static Iterator<NamespaceKey> getAncestors(NamespaceKey datasetKey) {
    return new Iterator<NamespaceKey>() {
      NamespaceKey currentKey = datasetKey;

      @Override
      public boolean hasNext() {
        return currentKey.hasParent();
      }

      @Override
      public NamespaceKey next() {
        if (!currentKey.hasParent()) {
          throw new NoSuchElementException();
        }
        currentKey = currentKey.getParent();
        return currentKey;
      }
    };
  }
}
