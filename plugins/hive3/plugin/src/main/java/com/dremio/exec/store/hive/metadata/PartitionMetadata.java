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
package com.dremio.exec.store.hive.metadata;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;

import com.dremio.connector.metadata.PartitionValue;
import com.dremio.hive.proto.HiveReaderProto.PartitionXattr;

/**
 * Helper class to hold elements needed to construct Dremio PartitionChunk objects. There is one for
 * every Hive Partition object. Each instance is checked by {@link HivePartitionChunkListing} to see
 * if the {@link #inputSplitBatchIterator} has been exhausted to determine if another PartitionChunk
 * is needed for this Hive Partition.
 */
public class PartitionMetadata {

  private final int partitionId;
  private final Partition partition;
  private final List<PartitionValue> partitionValues;
  private final InputSplitBatchIterator inputSplitBatchIterator;
  private final DatasetSplitBuildConf datasetSplitBuildConf;
  private final PartitionXattr partitionXattr;

  private PartitionMetadata(final int partitionId, final Partition partition, List<PartitionValue> partitionValues,
                            InputSplitBatchIterator inputSplitBatchIterator, DatasetSplitBuildConf datasetSplitBuildConf,
                            PartitionXattr partitionXattr) {
    this.partitionId = partitionId;
    this.partition = partition;
    this.partitionValues = Collections.unmodifiableList(partitionValues);
    this.inputSplitBatchIterator = inputSplitBatchIterator;
    this.datasetSplitBuildConf = datasetSplitBuildConf;
    this.partitionXattr = partitionXattr;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public Partition getPartition() {
    return partition;
  }

  public List<PartitionValue> getPartitionValues() {
    return partitionValues;
  }

  public InputSplitBatchIterator getInputSplitBatchIterator() {
    return inputSplitBatchIterator;
  }

  public DatasetSplitBuildConf getDatasetSplitBuildConf() {
    return datasetSplitBuildConf;
  }

  public PartitionXattr getPartitionXattr() {
    return partitionXattr;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private int partitionId;
    private Partition partition;
    private List<PartitionValue> partitionValues;
    private InputSplitBatchIterator inputSplitBatchIterator;
    private DatasetSplitBuildConf datasetSplitBuildConf;
    private PartitionXattr partitionXattr;

    private Builder() {
    }

    public Builder partitionId(int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    public Builder partition(Partition partition) {
      this.partition = partition;
      return this;
    }

    public Builder partitionValues(List<PartitionValue> partitionValues) {
      this.partitionValues = partitionValues;
      return this;
    }

    public Builder inputSplitBatchIterator(InputSplitBatchIterator inputSplitBatchIterator) {
      this.inputSplitBatchIterator = inputSplitBatchIterator;
      return this;
    }

    public Builder datasetSplitBuildConf(DatasetSplitBuildConf datasetSplitBuildConf) {
      this.datasetSplitBuildConf = datasetSplitBuildConf;
      return this;
    }

    public Builder partitionXattr(PartitionXattr partitionXattr) {
      this.partitionXattr = partitionXattr;
      return this;
    }

    public PartitionMetadata build() {
      Objects.requireNonNull(partitionId, "partition id is required");
      Objects.requireNonNull(inputSplitBatchIterator, "input split batch iterator is required");
      Objects.requireNonNull(partitionValues, "partition values is required");

      return new PartitionMetadata(partitionId, partition, partitionValues, inputSplitBatchIterator, datasetSplitBuildConf, partitionXattr);
    }
  }
}
