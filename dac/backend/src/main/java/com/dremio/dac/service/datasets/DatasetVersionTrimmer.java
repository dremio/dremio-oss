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
package com.dremio.dac.service.datasets;

import static com.dremio.dac.util.DatasetsUtil.isTemporaryPath;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.KVStore;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Trims dataset versions based on max number of versions to keep and an age limit. */
public final class DatasetVersionTrimmer {
  private static final Logger logger = LoggerFactory.getLogger(DatasetVersionTrimmer.class);

  /// Maximum number of versions to delete in a single call to KVStore.bulkDelete.
  private static final int MAX_VERSIONS_TO_DELETE = 10000;
  private static final int MAX_DELETE_VERSIONS_TO_LOG = 100;
  // Maximum number of versions to load via range query in memory for analysis.
  private static final int MAX_VERSIONS_IN_RANGE = 100000;
  // Versions not attached to any dataset can be deleted if they are older than this duration.
  private static final Duration ORPHAN_VERSION_TO_DELETE_AGE = Duration.ofDays(7);

  private final Clock clock;
  private final KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
      datasetVersionsStore;
  private final NamespaceService namespaceService;

  @WithSpan
  public static void trimHistory(
      Clock clock,
      KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> datasetVersionsStore,
      NamespaceService namespaceService,
      int maxVersionsToKeep,
      int minAgeDays) {
    new DatasetVersionTrimmer(clock, datasetVersionsStore, namespaceService)
        .trimHistory(maxVersionsToKeep, minAgeDays);
  }

  private DatasetVersionTrimmer(
      Clock clock,
      KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> datasetVersionsStore,
      NamespaceService namespaceService) {
    this.clock = clock;
    this.datasetVersionsStore = datasetVersionsStore;
    this.namespaceService = namespaceService;
  }

  /**
   * Performs maintenance on the history of dataset versions:
   * <li>Trims history of all dataset versions to a given maximum size.
   * <li>Deletes versions not attached to any dataset; when a dataset is deleted, versions are not.
   * <li>Attaches a valid linked list to a dataset if a dataset points to non-existing version.
   * <li>Deletes version lists with loops if they are not attached to dataset.
   */
  @WithSpan
  private void trimHistory(int maxVersionsToKeep, int minAgeInDays) {
    Preconditions.checkArgument(maxVersionsToKeep > 0, "maxVersionsToKeep must be positive");
    Preconditions.checkArgument(minAgeInDays > 0, "minAgeInDays must be positive");

    // Get all dataset "states", assume the size is somewhat limited, collect only keys to reduce
    // memory footprint. Datasets needing update are rare and are read/written one by one.
    // Datasets are needed for these cases:
    //   - Head version is missing in the dataset.
    //   - Dataset was deleted, versions are left dangling.
    //   - Linked list of versions is broken (e.g. there is a loop or multiple heads).
    Map<DatasetPath, DatasetState> datasetStates = findAllDatasets();
    logger.info("Collected {} datasets", datasetStates.size());

    // Assume number of datasets is somewhat small compared to number of versions.
    // First pass: count versions per dataset.
    Map<DatasetPath, Integer> counts = new HashMap<>();
    for (Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> entry :
        datasetVersionsStore.find()) {
      DatasetVersionMutator.VersionDatasetKey versionKey = entry.getKey();
      counts.compute(versionKey.getPath(), (key, count) -> count != null ? count + 1 : 1);

      DatasetState state = datasetStates.get(versionKey.getPath());
      if (state == null) {
        // tmp.UNTITLED must stay as these versions are referenced by jobs w/o having a dataset.
        if (!isTemporaryPath(versionKey.getPath().toPathList())) {
          // Dangling version or the version was created but dataset was not yet.
          datasetStates.put(
              versionKey.getPath(),
              new DatasetState(versionKey).setVersionExists(true).setDatasetExists(false));
        }
      } else {
        if (state.getVersionKey().equals(versionKey)) {
          // Version referenced by dataset exists.
          state.setVersionExists(true);
        }
      }
    }

    // Collect and order paths with more than requested number of versions.
    ImmutableList<Map.Entry<DatasetPath, Integer>> pathsWithCounts =
        counts.entrySet().stream()
            .sorted(Comparator.comparing(e -> e.getKey().toPathString()))
            .collect(ImmutableList.toImmutableList());
    ImmutableSet<DatasetPath> pathsSet =
        pathsWithCounts.stream()
            .filter(
                e -> {
                  // Either number of versions is large, dataset does not exist or the head version
                  // does not exist.
                  DatasetState state = datasetStates.get(e.getKey());
                  if (state == null) {
                    // Skip versions w/o state (e.g. temp versions).
                    return false;
                  }
                  return e.getValue() > maxVersionsToKeep
                      || !state.getDatasetExists()
                      || !state.getVersionExists();
                })
            .map(Map.Entry::getKey)
            .collect(ImmutableSet.toImmutableSet());

    if (!pathsSet.isEmpty()) {
      // Second pass: get versions to delete (past the maxVersionsToKeep) and update (set previous
      // version to null in the last element of the kept history).
      ArrayList<DatasetVersionMutator.VersionDatasetKey> keysToDelete = new ArrayList<>();
      Map<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> versionsToUpdate =
          new HashMap<>();
      ArrayList<DatasetState> datasetsToUpdate = new ArrayList<>();
      DatasetPath startPath = pathsWithCounts.get(0).getKey();
      int versionsInRange = 0;
      for (int index = 0; index < pathsWithCounts.size(); index++) {
        Map.Entry<DatasetPath, Integer> pathAndCount = pathsWithCounts.get(index);
        DatasetPath endPath = pathAndCount.getKey();
        versionsInRange += pathAndCount.getValue();
        if (versionsInRange < MAX_VERSIONS_IN_RANGE && index + 1 < pathsWithCounts.size()) {
          continue;
        }

        // Collect versions to trim/update in the range.
        logger.info("Collecting records to trim, batch: s: {} e: {}", startPath, endPath);
        keysToDelete.clear();
        versionsToUpdate.clear();
        datasetsToUpdate.clear();
        findVersionKeysToTrim(
            startPath,
            endPath,
            pathsSet,
            datasetStates,
            maxVersionsToKeep,
            minAgeInDays,
            keysToDelete,
            versionsToUpdate,
            datasetsToUpdate);

        // Update versions first, for any partial updates due to errors/conflicts etc, next run will
        // fix it.
        logger.info("Updating batch of {} older dataset versions", versionsToUpdate.size());
        for (Map.Entry<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> entry :
            versionsToUpdate.entrySet()) {
          logger.info("Updating version {} to set previous to null", entry.getKey());
          datasetVersionsStore.put(entry.getKey(), entry.getValue());
        }

        // Update datasets if possible.
        logger.info("Updating {} datasets", datasetsToUpdate.size());
        for (DatasetState datasetState : datasetsToUpdate) {
          NamespaceKey key = datasetState.getVersionKey().getPath().toNamespaceKey();
          try {
            DatasetConfig datasetConfig = namespaceService.getDataset(key);
            if (datasetConfig.getVirtualDataset() != null
                && datasetState.updateVersion(datasetConfig.getVirtualDataset())) {
              try {
                logger.info(
                    "Updating dataset {} to set version to {}",
                    key,
                    datasetConfig.getVirtualDataset().getVersion());

                // Update dataset to point to a valid version.
                namespaceService.addOrUpdateDataset(key, datasetConfig);

                // Once updated, it is safe to delete unusable versions.
                keysToDelete.addAll(datasetState.getUnusableVersionKeys());
              } catch (Exception e) {
                // Concurrent modification exception is possible, ignore it and any other update
                // exceptions.
                logger.warn("Failed to update dataset", e);
              }
            }
          } catch (NamespaceException e) {
            // Ignore namespace exceptions as the state of the dataset could've changed.
          }
        }

        // Finally, delete trimmed/unusable versions.
        logger.info("Deleting {} dataset versions", keysToDelete.size());
        for (List<DatasetVersionMutator.VersionDatasetKey> keysRange :
            Lists.partition(keysToDelete, MAX_VERSIONS_TO_DELETE)) {
          logger.info("Deleting batch of {} older dataset versions", keysRange.size());
          datasetVersionsStore.bulkDelete(keysRange);

          List<DatasetVersionMutator.VersionDatasetKey> keysRangeToLog =
              keysRange.size() <= MAX_DELETE_VERSIONS_TO_LOG
                  ? keysRange
                  : keysRange.subList(0, MAX_DELETE_VERSIONS_TO_LOG);
          logger.info(
              "Deleted versions ({} out of {} total): {}",
              keysRangeToLog.size(),
              keysRange.size(),
              keysRangeToLog);
        }

        // Reset range.
        startPath = endPath;
        versionsInRange = 0;
      }
    }
  }

  /**
   * Gets version information for all datasets. Much of the contents of {@link DatasetConfig} is not
   * used and is discarded to reduce memory usage.
   */
  @WithSpan
  private Map<DatasetPath, DatasetState> findAllDatasets() {
    Map<DatasetPath, DatasetState> headVersions = new HashMap<>();
    for (Document<NamespaceKey, NameSpaceContainer> entry : namespaceService.find(null)) {
      if (entry.getValue().getType() == NameSpaceContainer.Type.DATASET) {
        DatasetConfig datasetConfig = entry.getValue().getDataset();
        if (datasetConfig != null && datasetConfig.getVirtualDataset() != null) {
          // Dataset versions use "original" (aka canonical) path stored inside DatasetConfig.
          // The entry.getKey() may not match the case.
          DatasetPath path = new DatasetPath(datasetConfig.getFullPathList());
          headVersions.put(
              path,
              new DatasetState(
                      new DatasetVersionMutator.VersionDatasetKey(
                          path, datasetConfig.getVirtualDataset().getVersion()))
                  .setDatasetExists(true));
        }
      }
    }
    return headVersions;
  }

  /**
   * Finds versions to delete for all datasets in the range of keys.
   *
   * @param startPath Start dataset path in the range of paths (inclusive).
   * @param endPath End dataset path in the range of paths (inclusive).
   * @param pathsSet The range may include paths that should not be analyzed. The set includes paths
   *     to analyze.
   * @param datasetStates Dataset states with booleans for existence of the dataset version pointer
   *     in the dataset and existence of dataset itself.
   * @param maxVersionsToKeep Maximum number of versions to keep for a dataset.
   * @param minAgeInDays If number of versions is greater than maxVersionsToKeep, the versions older
   *     than this threshold past the maxVersionsToKeep are deleted (trimmed).
   * @param keysToDelete Version keys to delete are added to this list.
   * @param versionsToUpdate The new tails of the linked list (with null previous version) are added
   *     to this list.
   * @param datasetsToUpdate Datasets with non-existent versions are added to this list if there are
   *     valid head versions that can be used for the datasets.
   */
  @WithSpan
  private void findVersionKeysToTrim(
      DatasetPath startPath,
      DatasetPath endPath,
      ImmutableSet<DatasetPath> pathsSet,
      Map<DatasetPath, DatasetState> datasetStates,
      int maxVersionsToKeep,
      int minAgeInDays,
      List<DatasetVersionMutator.VersionDatasetKey> keysToDelete,
      Map<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> versionsToUpdate,
      List<DatasetState> datasetsToUpdate) {
    ImmutableFindByRange.Builder<DatasetVersionMutator.VersionDatasetKey> findByRangeBuilder =
        new ImmutableFindByRange.Builder<DatasetVersionMutator.VersionDatasetKey>()
            .setStart(
                new DatasetVersionMutator.VersionDatasetKey(startPath, DatasetVersion.MIN_VERSION))
            .setEnd(
                new DatasetVersionMutator.VersionDatasetKey(endPath, DatasetVersion.MAX_VERSION));

    // Collect versions by dataset path.
    Multimap<DatasetPath, Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
        allVersions = HashMultimap.create();
    for (Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> entry :
        datasetVersionsStore.find(findByRangeBuilder.build())) {
      // Range search may include paths that don't require any processing, exclude those.
      if (pathsSet.contains(entry.getKey().getPath())) {
        allVersions.put(entry.getKey().getPath(), entry);
      }
    }

    // Collect versions to delete and update.
    for (DatasetPath key : allVersions.keySet()) {
      Collection<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
          versions = allVersions.get(key);
      DatasetState datasetState = datasetStates.get(key);
      if (!datasetState.getDatasetExists()) {
        // Dataset no longer or not yet exists. Check last modified time of all versions
        // is old enough to delete the versions.
        if (allVersionsAreOld(versions)) {
          versions.forEach(v -> keysToDelete.add(v.getKey()));
        }
      } else {
        VersionList versionList = VersionList.build(versions);
        boolean updateDataset = !datasetState.getVersionExists();
        if (datasetState.getVersionExists()) {
          // Normal case: both head version and dataset exist, trimming can be done unless the
          // linked list is broken.
          Optional<
                  ImmutableList<
                      Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>>
              orderedVersionsOptional = versionList.getVersionsList(datasetState.getVersionKey());
          if (orderedVersionsOptional.isPresent()) {
            // Linked list is ok, apply trimming.
            ImmutableList<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
                orderedVersions = orderedVersionsOptional.get();
            int trimIndex = findIndexToTrimAt(orderedVersions, maxVersionsToKeep, minAgeInDays);
            if (trimIndex >= 0 && trimIndex < orderedVersions.size()) {
              // Stop history at the last kept entry.
              Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
                  versionToKeep = orderedVersions.get(trimIndex);
              versionsToUpdate.put(
                  versionToKeep.getKey(), versionToKeep.getValue().setPreviousVersion(null));

              // Collect keys to delete.
              if (trimIndex + 1 < orderedVersions.size()) {
                for (Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
                    entry : orderedVersions.subList(trimIndex + 1, orderedVersions.size())) {
                  keysToDelete.add(entry.getKey());
                }
              }
            }
          } else {
            // Linked list is broken, need to try to fix it.
            updateDataset = true;
          }
        }

        if (updateDataset) {
          // Dataset exists but the version it points to does not. This means that the version was
          // deleted as versions are created before datasets are updated with new version so it
          // cannot be that the non-existing version is not yet created. Or, the version will be
          // deleted because it points to a list with loop.
          // In these cases, the dataset config needs to point to a valid linked list.
          Optional<
                  ImmutableList<
                      Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>>
              orderedVersionsOptional = versionList.getFirstVersionsList();
          if (orderedVersionsOptional.isPresent()) {
            // Dataset config can be updated.
            datasetState.setUpdateVersion(
                orderedVersionsOptional.get().get(0).getKey().getVersion());
            datasetState.setUnusableVersionKeys(versionList.getUnusableKeys());
            datasetsToUpdate.add(datasetState);
          }
        }

        // If the dataset is supposed to be updated, do not add unusable keys for deletion as it is
        // better to have dataset pointing to a broken list than to a non-existing one in case
        // dataset update fails.
        if (!updateDataset) {
          // Add keys that are part of broken linked lists.
          keysToDelete.addAll(versionList.getUnusableKeys());
        }
      }
    }
  }

  /** Old enough orphaned versions can be deleted. This method decides if they are old enough. */
  private boolean allVersionsAreOld(
      Collection<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
          versions) {
    long cutoffTimeMillis = clock.instant().minus(ORPHAN_VERSION_TO_DELETE_AGE).toEpochMilli();
    return versions.stream()
        .allMatch(
            v -> {
              DatasetConfig datasetConfig = v.getValue().getDataset();
              return datasetConfig.getLastModified() != null
                      && datasetConfig.getLastModified() < cutoffTimeMillis
                  || datasetConfig.getCreatedAt() != null
                      && datasetConfig.getCreatedAt() < cutoffTimeMillis
                  || datasetConfig.getLastModified() == null
                      && datasetConfig.getCreatedAt() == null;
            });
  }

  private int findIndexToTrimAt(
      List<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
          orderedVersions,
      int maxVersionsToKeep,
      int minAgeInDays) {
    int index = maxVersionsToKeep - 1;
    if (orderedVersions.size() <= index) {
      // No need to trim.
      return -1;
    }

    // Find time threshold: versions with lastModifiedAt smaller than this value can be trimmed.
    // The orderedVersions list is sorted by last modified in descending order as it is constructed
    // from the linked list, the last modified may not be set. Look for first index
    // at or after (maxVersionsToKeep - 1) where timestamp is smaller than the cutoff time.
    long minEpochMillis = clock.instant().minus(minAgeInDays, ChronoUnit.DAYS).toEpochMilli();
    while (index < orderedVersions.size()) {
      Long lastModified = orderedVersions.get(index).getValue().getDataset().getLastModified();
      if (lastModified == null || lastModified < minEpochMillis) {
        // Break if last modified was not set (older versions, breaking change) or it is set and is
        // below the threshold.
        break;
      }
      index++;
    }
    return index;
  }

  /**
   * Describes state of a dataset: whether the version it points to exists, whether the dataset
   * exists.
   */
  private static final class DatasetState {
    private final DatasetVersionMutator.VersionDatasetKey versionKey;
    private boolean versionExists;
    private boolean datasetExists;

    // Version to use in the dataset.
    private DatasetVersion newVersion;
    // Version keys that can be deleted after dataset is updated to use the new version.
    private ImmutableSet<DatasetVersionMutator.VersionDatasetKey> unusableVersionKeys;

    private DatasetState(DatasetVersionMutator.VersionDatasetKey versionKey) {
      this.versionKey = versionKey;
    }

    private DatasetVersionMutator.VersionDatasetKey getVersionKey() {
      return versionKey;
    }

    private void setUpdateVersion(DatasetVersion newVersion) {
      this.newVersion = newVersion;
    }

    private void setUnusableVersionKeys(
        ImmutableSet<DatasetVersionMutator.VersionDatasetKey> unusableVersionKeys) {
      this.unusableVersionKeys = unusableVersionKeys;
    }

    /** Sets new version in the view if the current version still matches. */
    private boolean updateVersion(VirtualDataset virtualDataset) {
      Preconditions.checkNotNull(newVersion, "New version was not set.");

      // The versions has changed, skip update.
      if (!virtualDataset.getVersion().equals(versionKey.getVersion())) {
        return false;
      }
      virtualDataset.setVersion(newVersion);
      return true;
    }

    private ImmutableSet<DatasetVersionMutator.VersionDatasetKey> getUnusableVersionKeys() {
      return this.unusableVersionKeys;
    }

    private DatasetState setVersionExists(boolean value) {
      versionExists = value;
      return this;
    }

    private boolean getVersionExists() {
      return versionExists;
    }

    private DatasetState setDatasetExists(boolean value) {
      datasetExists = value;
      return this;
    }

    private boolean getDatasetExists() {
      return datasetExists;
    }
  }
}
