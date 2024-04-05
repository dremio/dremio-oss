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
package com.dremio.exec.planner.cost.iceberg;

import static com.dremio.exec.store.iceberg.model.IcebergConstants.ADDED_DATA_FILES;
import static com.dremio.exec.store.iceberg.model.IcebergConstants.DELETED_DATA_FILES;
import static org.apache.iceberg.TableProperties.GC_ENABLED;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.PropertyUtil;

/** Captures estimated costs for scanning Iceberg spec elements - manifests, data files etc. */
public class IcebergCostEstimates {

  // Each dataFile details into manifest take around 35-45 Bytes.This approximation is being
  // considered with a manifest of 8MB size.
  public static final long ESTIMATED_RECORDS_PER_MANIFEST = 100_000;

  private long snapshotsCount = 0L;
  private long dataFileEstimatedCount = 0L;
  private long manifestFileEstimatedCount = 0L;
  private long tablesCount = 1L;

  /** Loads the metadata of the abstracted Iceberg table, and estimates cost from there. */
  public IcebergCostEstimates(Table icebergTable) {
    /*
     * Here is a suboptimal plan to estimate the row accounts for Prels used in ExpireSnapshots plan. The 'suboptimal' mean
     * to directly load the Iceberg table and read back its all snapshots and stats of each snapshot for row estimates.
     * Another approach is tracked in DX-63280.
     */
    boolean gcEnabled = PropertyUtil.propertyAsBoolean(icebergTable.properties(), GC_ENABLED, true);
    if (gcEnabled) {
      for (Snapshot snapshot : icebergTable.snapshots()) {
        includeSnapshotCosts(snapshot, ++snapshotsCount);
      }
    }
  }

  public IcebergCostEstimates(
      long tablesCount,
      long snapshotsCount,
      long dataFileEstimatedCount,
      long manifestFileEstimatedCount) {
    this.snapshotsCount = snapshotsCount;
    this.dataFileEstimatedCount = dataFileEstimatedCount;
    this.manifestFileEstimatedCount = manifestFileEstimatedCount;
    this.tablesCount = tablesCount;
  }

  public long getSnapshotsCount() {
    return snapshotsCount;
  }

  public long getDataFileEstimatedCount() {
    return dataFileEstimatedCount;
  }

  public long getManifestFileEstimatedCount() {
    return manifestFileEstimatedCount;
  }

  public long getTablesCount() {
    return tablesCount;
  }

  public long getEstimatedRows() {
    return dataFileEstimatedCount
        + manifestFileEstimatedCount
        + snapshotsCount /*Manifest list file*/
        + snapshotsCount * 2 /*Partition stats files*/;
  }

  private void includeSnapshotCosts(Snapshot snapshot, long snapshotsCount) {
    // First snapshot
    Map<String, String> summary =
        Optional.ofNullable(snapshot).map(Snapshot::summary).orElseGet(ImmutableMap::of);
    if (1 == snapshotsCount) {
      long numDataFiles = Long.parseLong(summary.getOrDefault("total-data-files", "0"));
      dataFileEstimatedCount += numDataFiles;
      long numPositionDeletes = Long.parseLong(summary.getOrDefault("total-position-deletes", "0"));
      dataFileEstimatedCount += numPositionDeletes;
      long numEqualityDeletes = Long.parseLong(summary.getOrDefault("total-equality-deletes", "0"));
      dataFileEstimatedCount += numEqualityDeletes;

      manifestFileEstimatedCount +=
          Math.max(dataFileEstimatedCount / ESTIMATED_RECORDS_PER_MANIFEST, 1);
    } else {
      long numAddedDataFiles = Long.parseLong(summary.getOrDefault(ADDED_DATA_FILES, "0"));
      dataFileEstimatedCount += numAddedDataFiles;
      long numAddedDeleteFiles = Long.parseLong(summary.getOrDefault(DELETED_DATA_FILES, "0"));
      dataFileEstimatedCount += numAddedDeleteFiles;

      manifestFileEstimatedCount +=
          Math.max((numAddedDataFiles + numAddedDeleteFiles) / ESTIMATED_RECORDS_PER_MANIFEST, 1);
    }
  }

  @Override
  public String toString() {
    return "IcebergCostEstimates{"
        + "snapshotsCount="
        + snapshotsCount
        + ", dataFileEstimatedCount="
        + dataFileEstimatedCount
        + ", manifestFileEstimatedCount="
        + manifestFileEstimatedCount
        + ", tablesCount="
        + tablesCount
        + ", estimatedRows="
        + getEstimatedRows()
        + '}';
  }
}
