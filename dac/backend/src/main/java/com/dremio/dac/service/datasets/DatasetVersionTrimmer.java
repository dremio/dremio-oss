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

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.KVStore;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Trims dataset versions based on max number of versions to keep and an age limit. */
public final class DatasetVersionTrimmer {
  private static final Logger logger = LoggerFactory.getLogger(DatasetVersionTrimmer.class);

  private static final int MAX_VERSIONS_TO_DELETE = 10000;
  private static final int MAX_VERSIONS_IN_RANGE = 100000;

  private final Clock clock;
  private final KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
      datasetVersionsStore;

  @WithSpan
  public static void trimHistory(
      Clock clock,
      KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> datasetVersionsStore,
      int maxVersionsToKeep,
      int minAgeInDays) {
    new DatasetVersionTrimmer(clock, datasetVersionsStore)
        .trimHistory(maxVersionsToKeep, minAgeInDays);
  }

  private DatasetVersionTrimmer(
      Clock clock,
      KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
          datasetVersionsStore) {
    this.clock = clock;
    this.datasetVersionsStore = datasetVersionsStore;
  }

  /**
   * Trims history of all dataset versions to a given maximum size. FindOptions are used for any
   * customizations to record filtering allowing for custom filters per coordinator.
   */
  private void trimHistory(int maxVersionsToKeep, int minAgeInDays) {
    Preconditions.checkArgument(maxVersionsToKeep > 0, "maxVersionsToKeep must be positive");
    Preconditions.checkArgument(minAgeInDays > 0, "minAgeInDays must be positive");

    // Assume number of datasets is somewhat small compared to number of versions.
    // First pass: count versions per dataset.
    Map<DatasetPath, Integer> counts = Maps.newHashMap();
    for (Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> entry :
        datasetVersionsStore.find()) {
      counts.compute(entry.getKey().getPath(), (key, count) -> count != null ? count + 1 : 1);
    }

    // Collect and order paths with more than requested number of versions.
    ImmutableList<Map.Entry<DatasetPath, Integer>> pathsWithCounts =
        counts.entrySet().stream()
            .sorted(Comparator.comparing(e -> e.getKey().toPathString()))
            .collect(ImmutableList.toImmutableList());
    ImmutableSet<DatasetPath> pathsSet =
        pathsWithCounts.stream()
            .filter(e -> e.getValue() > maxVersionsToKeep)
            .map(Map.Entry::getKey)
            .collect(ImmutableSet.toImmutableSet());

    if (!pathsSet.isEmpty()) {
      // Second pass: get versions to delete (past the maxVersionsToKeep) and update (set previous
      // version to null in the last element of the kept history).
      ArrayList<DatasetVersionMutator.VersionDatasetKey> keysToDelete = new ArrayList<>();
      Map<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> versionsToUpdate =
          Maps.newHashMap();
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
        findVersionKeysToTrim(
            startPath,
            endPath,
            pathsSet,
            maxVersionsToKeep,
            minAgeInDays,
            keysToDelete,
            versionsToUpdate);

        // Update versions first, for any partial updates due to errors/conflicts etc, next run will
        // fix it.
        logger.info("Updating batch of {} older dataset versions", versionsToUpdate.size());
        for (Map.Entry<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> entry :
            versionsToUpdate.entrySet()) {
          datasetVersionsStore.put(entry.getKey(), entry.getValue());
        }
        for (List<DatasetVersionMutator.VersionDatasetKey> keysRange :
            Lists.partition(keysToDelete, MAX_VERSIONS_TO_DELETE)) {
          logger.info("Deleting batch of {} older dataset versions", keysRange.size());
          datasetVersionsStore.bulkDelete(keysRange);
        }

        // Reset range.
        startPath = endPath;
        versionsInRange = 0;
      }
    }
  }

  @WithSpan
  private void findVersionKeysToTrim(
      DatasetPath startPath,
      DatasetPath endPath,
      ImmutableSet<DatasetPath> pathsSet,
      int maxVersionsToKeep,
      int minAgeInDays,
      List<DatasetVersionMutator.VersionDatasetKey> keysToDelete,
      Map<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> valuesToUpdate) {
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
      // Range search may include paths that don't require trimming, exclude those.
      if (pathsSet.contains(entry.getKey().getPath())) {
        allVersions.put(entry.getKey().getPath(), entry);
      }
    }

    // Collect versions to delete and update.
    for (DatasetPath key : allVersions.keySet()) {
      // Order versions in reverse chronological order.
      List<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
          orderedVersions =
              allVersions.get(key).stream()
                  .sorted(
                      (d1, d2) ->
                          -Long.compare(
                              d1.getKey().getVersion().getValue(),
                              d2.getKey().getVersion().getValue()))
                  .collect(Collectors.toList());
      int trimIndex = findIndexToTrimAt(orderedVersions, maxVersionsToKeep, minAgeInDays);
      if (trimIndex >= 0 && trimIndex < orderedVersions.size()) {
        // Stop history at the last kept entry.
        Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> versionToKeep =
            orderedVersions.get(trimIndex);
        valuesToUpdate.put(
            versionToKeep.getKey(), versionToKeep.getValue().setPreviousVersion(null));

        // Collect keys to delete.
        if (trimIndex + 1 < orderedVersions.size()) {
          for (Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> entry :
              orderedVersions.subList(trimIndex + 1, orderedVersions.size())) {
            keysToDelete.add(entry.getKey());
          }
        }
      }
    }
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
    long minEpochMillis = clock.instant().minus(minAgeInDays, ChronoUnit.DAYS).toEpochMilli();
    while (index >= 0) {
      long lastModified = orderedVersions.get(index).getKey().getVersion().getTimestamp();
      if (lastModified < minEpochMillis) {
        break;
      }
      index--;
    }
    return index;
  }
}
