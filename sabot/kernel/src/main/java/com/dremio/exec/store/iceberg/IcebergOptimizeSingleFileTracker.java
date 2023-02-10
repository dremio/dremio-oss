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
package com.dremio.exec.store.iceberg;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identification of lone file rewrites within partitions
 */
public class IcebergOptimizeSingleFileTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergOptimizeSingleFileTracker.class);

  private final Map<PartitionInfo, RewritablePartitionFiles> rewriteCandidates = new HashMap<>();

  public void consumeAddDataFile(DataFile dataFile) {
    PartitionInfo partitionInfo = new PartitionInfo(dataFile.specId(), dataFile.partition());
    RewritablePartitionFiles rewriteFiles = rewriteCandidates.computeIfAbsent(partitionInfo, p -> new RewritablePartitionFiles());
    rewriteFiles.consumeAddedFilePath(dataFile);
  }

  public void consumeDeletedDataFile(DataFile dataFile) {
    PartitionInfo partitionInfo = new PartitionInfo(dataFile.specId(), dataFile.partition());
    RewritablePartitionFiles rewriteFiles = rewriteCandidates.computeIfAbsent(partitionInfo, p -> new RewritablePartitionFiles());
    rewriteFiles.consumeDeletedFilePath(dataFile);
  }

  /**
   * Removes the tracked single file rewrites per partition from the input lists
   */
  public Set<String> removeSingleFileChanges(Set<DataFile> addedDataFiles, Set<DataFile> deleteDataFiles) {
    Set<String> skipAddFilePaths = new HashSet<>();
    Set<String> skipDeleteFilePaths = new HashSet<>();
    rewriteCandidates.entrySet().stream().filter(e -> e.getValue().isSameFileChange()).forEach(e -> {
      skipAddFilePaths.add(e.getValue().getFirstAddedFilePath());
      skipDeleteFilePaths.add(e.getValue().getFirstDeletedFilePath());
    });

    LOGGER.debug("Skip Entries: Add: {}, Delete: {}", skipAddFilePaths, skipDeleteFilePaths);
    addedDataFiles.removeIf(d -> skipAddFilePaths.contains(d.path().toString()));
    deleteDataFiles.removeIf(d -> skipDeleteFilePaths.contains(d.path().toString()));

    return skipAddFilePaths;
  }

  class RewritablePartitionFiles {
    private String firstAddedFilePath = null;
    private long firstAddedFileRecords = 0L;
    private String firstDeletedFilePath = null;
    private long firstDeletedFileRecords = 0L;
    private boolean hasMultipleFiles = false;

    public String getFirstAddedFilePath() {
      return firstAddedFilePath;
    }

    public void consumeAddedFilePath(DataFile addedFile) {
      if (this.firstAddedFilePath != null) {
        this.hasMultipleFiles = true;
      } else {
        this.firstAddedFilePath = addedFile.path().toString();
        this.firstAddedFileRecords = addedFile.recordCount();
      }
    }

    public String getFirstDeletedFilePath() {
      return firstDeletedFilePath;
    }

    public void consumeDeletedFilePath(DataFile deletedFile) {
      if (this.firstDeletedFilePath != null) {
        this.hasMultipleFiles = true;
      } else {
        this.firstDeletedFilePath = deletedFile.path().toString();
        this.firstDeletedFileRecords = deletedFile.recordCount();
      }
    }

    public boolean hasMultipleFiles() {
      return hasMultipleFiles;
    }

    public boolean isSameFileChange() {
      return !this.hasMultipleFiles()
        && this.getFirstAddedFilePath() != null
        && this.getFirstDeletedFilePath() != null
        && this.firstAddedFileRecords == this.firstDeletedFileRecords;
    }
  }

  class PartitionInfo {
    private int specId;
    private StructLike partitionData;

    public PartitionInfo(int specId, StructLike partitionData) {
      this.specId = specId;
      this.partitionData = partitionData;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionInfo that = (PartitionInfo) o;
      return specId == that.specId && Objects.equals(partitionData, that.partitionData);
    }

    @Override
    public int hashCode() {
      return Objects.hash(specId, partitionData);
    }
  }
}
