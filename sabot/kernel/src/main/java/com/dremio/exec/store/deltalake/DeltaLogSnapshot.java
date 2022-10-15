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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.io.file.FileAttributes;
import com.google.common.annotations.VisibleForTesting;

/**
 * Captures DeltaLake commit and metadata information present in one log file. The log file could be a JSON or checkpoint.parquet
 * The class also provides merging capability in order to evaluate an overall snapshot.
 */
public final class DeltaLogSnapshot implements Comparable<DeltaLogSnapshot> {
    private String operationType;
    private String schema;
    private long netFilesAdded; // could be negative
    private long netBytesAdded; // could be negative
    private long netOutputRows; // could be negative
    private long totalFileEntries; // All entries added or removed
    private long timestamp;
    private long versionId = -1;
    private boolean isCheckpoint = false;
    private FileAttributes fileAttrs;
    private List<String> partitionColumns = Collections.emptyList();
    private List<DatasetSplit> splits = Collections.emptyList();
    private boolean missingRequiredValues = false;

    public DeltaLogSnapshot(String operationType,
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
      this("UNKNOWN", 0, 0, 0, 0, 0, false);
      this.missingRequiredValues = false;
    }

    public void setSchema(String schema, List<String> partitionColumns) {
        this.schema = schema;
        this.partitionColumns = partitionColumns;
    }

    public boolean isMissingRequiredValues() {
      return missingRequiredValues;
    }

    public void setMissingRequiredValues(boolean missingRequiredValues) {
      this.missingRequiredValues = missingRequiredValues;
    }

    public void finaliseMissingRequiredValues() {
      if (this.netOutputRows == 0) {
        this.netOutputRows = DremioCost.LARGE_ROW_COUNT;
      }
      if (this.netFilesAdded == 0) {
        this.netFilesAdded = DremioCost.LARGE_FILE_COUNT;
      }
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

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public long getDataFileEntryCount() {
        return totalFileEntries;
    }

    public void setVersionId(long versionId) {
      this.versionId = versionId;
    }

    public void setFileAttrs(FileAttributes fileAttrs) {
      this.fileAttrs = fileAttrs;
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
        this.operationType = "COMBINED";
        this.netFilesAdded += that.netFilesAdded;
        this.netOutputRows += that.netOutputRows;
        if (this.isMissingRequiredValues() || that.isMissingRequiredValues()) {
          this.netFilesAdded = Math.min(this.netFilesAdded, DremioCost.LARGE_FILE_COUNT);
          this.netOutputRows = Math.min(this.netOutputRows, DremioCost.LARGE_ROW_COUNT);
          setMissingRequiredValues(true);
        }
        this.netBytesAdded += that.netBytesAdded;
        this.totalFileEntries += that.totalFileEntries;
        this.isCheckpoint = this.isCheckpoint || that.isCheckpoint;
        if (this.schema != null && that.schema != null && !this.partitionColumns.equals(that.partitionColumns)) {
            throw new IllegalStateException("Different partitions detected across the commits." +
                    " Dremio doesn't support scan on a repartitioned table.");
        }
        if (this.schema == null || (this.compareTo(that) < 0 && that.schema != null)){
            this.schema = that.schema;
            this.partitionColumns = that.partitionColumns;
        }
        this.timestamp = Math.max(this.timestamp, that.timestamp);
        this.versionId = Math.max(this.versionId, that.versionId);
    }

    @VisibleForTesting
    public DeltaLogSnapshot clone() {
        DeltaLogSnapshot clone = new DeltaLogSnapshot(this.operationType, this.netFilesAdded, this.netBytesAdded,
          this.netOutputRows, this.totalFileEntries, this.timestamp, this.isCheckpoint);
        clone.setSchema(this.schema, this.partitionColumns);
        clone.setVersionId(this.versionId);
        clone.setMissingRequiredValues(this.isMissingRequiredValues());
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
        return netFilesAdded == snapshot.netFilesAdded &&
                netBytesAdded == snapshot.netBytesAdded &&
                netOutputRows == snapshot.netOutputRows &&
                totalFileEntries == snapshot.totalFileEntries &&
                timestamp == snapshot.timestamp &&
                versionId == snapshot.versionId &&
                isCheckpoint == snapshot.isCheckpoint &&
                missingRequiredValues == snapshot.missingRequiredValues &&
                operationType.equals(snapshot.operationType) &&
                Objects.equals(schema, snapshot.schema) &&
                Objects.equals(partitionColumns, snapshot.partitionColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationType, schema, netFilesAdded, netBytesAdded, netOutputRows, timestamp, versionId, partitionColumns, isCheckpoint, missingRequiredValues);
    }

    @Override
    public String toString() {
        return "DeltaLogSnapshot{" +
                "operationType='" + operationType + '\'' +
                ", schema='" + schema + '\'' +
                ", netFilesAdded=" + netFilesAdded +
                ", netBytesAdded=" + netBytesAdded +
                ", netOutputRows=" + netOutputRows +
                ", totalFileEntries=" + totalFileEntries +
                ", timestamp=" + timestamp +
                ", versionId=" + versionId +
                ", containsCheckpoint=" + containsCheckpoint() +
                ", missingRequiredValues=" + isMissingRequiredValues() +
                ", partitionColumns=" + partitionColumns +
                '}';
    }

    @Override
    public int compareTo(DeltaLogSnapshot that) {
        return Comparator
                .comparing(DeltaLogSnapshot::getVersionId)
                .thenComparing(DeltaLogSnapshot::containsCheckpoint)
                .compare(this, that);
    }
  }
