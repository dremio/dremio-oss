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
package com.dremio.connector.metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Interface for a connector to provide details about a partition. TODO: add what this is */
public interface PartitionChunk {

  /**
   * Get the partition values.
   *
   * @return list of partition column values, not null
   */
  default List<PartitionValue> getPartitionValues() {
    return Collections.emptyList();
  }

  /**
   * Get the number of splits in the partition chunk.
   *
   * @return number of splits
   */
  long getSplitCount();

  /**
   * Returns a new listing of dataset splits in the partition chunk. There must be one or more
   * dataset splits in a partition chunk.
   *
   * @return listing of dataset splits, not null
   */
  DatasetSplitListing getSplits();

  /**
   * Get any additional information about the partition chunk.
   *
   * <p>This will be provided by the catalog to other modules that request the catalog about the
   * partition chunk, so any custom state could be returned.
   *
   * @return extra information, not null
   */
  default BytesOutput getExtraInfo() {
    return BytesOutput.NONE;
  }

  /**
   * Create {@code PartitionChunk}.
   *
   * @param datasetSplits dataset splits
   * @return partition chunk
   */
  static PartitionChunk of(DatasetSplit... datasetSplits) {
    return of(Collections.emptyList(), Arrays.asList(datasetSplits));
  }

  /**
   * Create {@code PartitionChunk}.
   *
   * @param datasetSplits dataset splits
   * @return partition chunk
   */
  static PartitionChunk of(List<DatasetSplit> datasetSplits) {
    return of(Collections.emptyList(), datasetSplits, BytesOutput.NONE);
  }

  /**
   * Create {@code PartitionChunk}.
   *
   * @param datasetSplits dataset splits
   * @param extraInfo extra information
   * @return partition chunk
   */
  static PartitionChunk of(List<DatasetSplit> datasetSplits, BytesOutput extraInfo) {
    return of(Collections.emptyList(), datasetSplits, extraInfo);
  }

  /**
   * Create {@code PartitionChunk}.
   *
   * @param partitionValues partition values
   * @param datasetSplits dataset splits
   * @return partition chunk
   */
  static PartitionChunk of(List<PartitionValue> partitionValues, List<DatasetSplit> datasetSplits) {
    return of(partitionValues, datasetSplits, BytesOutput.NONE);
  }

  /**
   * Create {@code PartitionChunk}.
   *
   * @param partitionValues partition values
   * @param datasetSplits dataset splits
   * @param extraInfo extra information
   * @return partition chunk
   */
  static PartitionChunk of(
      List<PartitionValue> partitionValues,
      List<DatasetSplit> datasetSplits,
      BytesOutput extraInfo) {
    Objects.requireNonNull(partitionValues, "partition values is required");
    Objects.requireNonNull(datasetSplits, "dataset splits is required");
    Objects.requireNonNull(extraInfo, "extra info is required");

    return new PartitionChunkImpl(partitionValues, datasetSplits.size(), datasetSplits, extraInfo);
  }
}
