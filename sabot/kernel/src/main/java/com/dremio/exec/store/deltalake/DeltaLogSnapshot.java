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
package com.dremio.exec.store.deltalake;

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_COLUMN_MAPPING_ID;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_COLUMN_MAPPING_NAME;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_METADATA_COLUMN_MAPPING_MODE;

import com.dremio.connector.metadata.DatasetSplit;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Captures DeltaLake commit and metadata information present in one log file. The log file could be
 * a JSON or checkpoint.parquet. The class also provides merging capability in order to evaluate an
 * overall snapshot.
 */
public final class DeltaLogSnapshot implements Comparable<DeltaLogSnapshot> {
  private String operationType;
  private String schema = null;
  private long netFilesAdded; // could be negative
  private long netBytesAdded; // could be negative
  private long netOutputRows; // could be negative
  private long totalFileEntries; // All entries added or removed
  private long timestamp;
  private long versionId = -1;
  private boolean isCheckpoint;
  private List<String> partitionColumns = Collections.emptyList();
  private List<DatasetSplit> splits = Collections.emptyList();
  private Map<String, String> configuration = Collections.emptyMap();

  public DeltaLogSnapshot(
      String operationType,
      long netFilesAdded,
      long netBytesAdded,
      long netOutputRows,
      long totalFileEntries,
      long timestamp,
      boolean isCheckpoint) {
    this.operationType = operationType;
    this.netFilesAdded = netFilesAdded;
    this.netBytesAdded = netBytesAdded;
    this.netOutputRows = netOutputRows;
    this.timestamp = timestamp;
    this.isCheckpoint = isCheckpoint;
    this.totalFileEntries = totalFileEntries;
  }

  public DeltaLogSnapshot() {
    this(DeltaConstants.OPERATION_UNKNOWN, 0, 0, 0, 0, 0, false);
  }

  public void setSchema(
      String schema, List<String> partitionColumns, Map<String, String> configuration) {
    this.schema = schema;
    this.partitionColumns = partitionColumns != null ? partitionColumns : Collections.emptyList();
    this.configuration = configuration != null ? configuration : Collections.emptyMap();
  }

  public String getOperationType() {
    return operationType;
  }

  public String getSchema() {
    return schema;
  }

  public long getNetFilesAdded() {
    return netFilesAdded;
  }

  public long getNetBytesAdded() {
    return netBytesAdded;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getVersionId() {
    return versionId;
  }

  public long getNetOutputRows() {
    return netOutputRows;
  }

  public void setNetOutputRows(long netOutputRows) {
    this.netOutputRows = netOutputRows;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public DeltaColumnMappingMode getColumnMappingMode() {
    String mappingMode = configuration.get(DELTA_METADATA_COLUMN_MAPPING_MODE);
    if (DELTA_COLUMN_MAPPING_NAME.equalsIgnoreCase(mappingMode)) {
      return DeltaColumnMappingMode.NAME;
    } else if (DELTA_COLUMN_MAPPING_ID.equalsIgnoreCase(mappingMode)) {
      return DeltaColumnMappingMode.ID;
    } else {
      return DeltaColumnMappingMode.NONE;
    }
  }

  public long getDataFileEntryCount() {
    return totalFileEntries;
  }

  public void setVersionId(long versionId) {
    this.versionId = versionId;
  }

  public boolean containsCheckpoint() {
    return isCheckpoint;
  }

  public List<DatasetSplit> getSplits() {
    return splits;
  }

  public void setSplits(List<DatasetSplit> splits) {
    this.splits = splits;
  }

  public synchronized void merge(DeltaLogSnapshot that) {
    // Aggregate metrics and use the schema from the recent most version.
    if (that == null) {
      return;
    }
    this.operationType = DeltaConstants.OPERATION_COMBINED;
    this.netFilesAdded += that.netFilesAdded;
    this.netOutputRows += that.netOutputRows;
    this.netBytesAdded += that.netBytesAdded;
    this.totalFileEntries += that.totalFileEntries;
    if ((that.operationType.equalsIgnoreCase(DeltaConstants.OPERATION_TRUNCATE)
            || that.operationType.equalsIgnoreCase(DeltaConstants.OPERATION_DELETE))
        && this.netFilesAdded == 0) {
      // when TRUNCATE and DELETE remove all files, reset row and bytes metrics as well
      this.netOutputRows = 0;
      this.netBytesAdded = 0;
      this.totalFileEntries = 0;
    }
    if (DeltaConstants.CREATE_OPERATIONS.contains(that.operationType.toUpperCase())) {
      // transaction log contains metrics for newly created table
      this.netFilesAdded = that.netFilesAdded;
      this.netOutputRows = that.netOutputRows;
      this.netBytesAdded = that.netBytesAdded;
      this.totalFileEntries = that.totalFileEntries;
    }
    this.isCheckpoint = this.isCheckpoint || that.isCheckpoint;
    if (this.schema != null
        && that.schema != null
        && !this.partitionColumns.equals(that.partitionColumns)) {
      throw new IllegalStateException(
          "Different partitions detected across the commits."
              + " Dremio doesn't support scan on a repartitioned table.");
    }
    if (this.schema == null || (this.compareTo(that) < 0 && that.schema != null)) {
      this.schema = that.schema;
      this.partitionColumns = that.partitionColumns;
      this.configuration = that.configuration;
    }
    this.timestamp = Math.max(this.timestamp, that.timestamp);
    this.versionId = Math.max(this.versionId, that.versionId);
  }

  @Override
  @VisibleForTesting
  public DeltaLogSnapshot clone() {
    DeltaLogSnapshot clone =
        new DeltaLogSnapshot(
            this.operationType,
            this.netFilesAdded,
            this.netBytesAdded,
            this.netOutputRows,
            this.totalFileEntries,
            this.timestamp,
            this.isCheckpoint);
    clone.setSchema(this.schema, this.partitionColumns, this.configuration);
    clone.setVersionId(this.versionId);
    return clone;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DeltaLogSnapshot snapshot = (DeltaLogSnapshot) o;
    return netFilesAdded == snapshot.netFilesAdded
        && netBytesAdded == snapshot.netBytesAdded
        && netOutputRows == snapshot.netOutputRows
        && totalFileEntries == snapshot.totalFileEntries
        && timestamp == snapshot.timestamp
        && versionId == snapshot.versionId
        && isCheckpoint == snapshot.isCheckpoint
        && operationType.equals(snapshot.operationType)
        && Objects.equals(schema, snapshot.schema)
        && Objects.equals(partitionColumns, snapshot.partitionColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        operationType,
        schema,
        netFilesAdded,
        netBytesAdded,
        netOutputRows,
        timestamp,
        versionId,
        partitionColumns,
        isCheckpoint);
  }

  @Override
  public String toString() {
    return "DeltaLogSnapshot{"
        + "operationType='"
        + operationType
        + '\''
        + ", schema='"
        + (schema != null ? schema : "")
        + '\''
        + ", netFilesAdded="
        + netFilesAdded
        + ", netBytesAdded="
        + netBytesAdded
        + ", netOutputRows="
        + netOutputRows
        + ", totalFileEntries="
        + totalFileEntries
        + ", timestamp="
        + timestamp
        + ", versionId="
        + versionId
        + ", containsCheckpoint="
        + containsCheckpoint()
        + ", partitionColumns="
        + partitionColumns
        + '}';
  }

  @Override
  public int compareTo(DeltaLogSnapshot that) {
    return Comparator.comparing(DeltaLogSnapshot::getVersionId)
        .thenComparing(DeltaLogSnapshot::containsCheckpoint)
        .compare(this, that);
  }
}
