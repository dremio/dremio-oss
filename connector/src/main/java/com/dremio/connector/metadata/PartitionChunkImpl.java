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

import java.util.List;

/** Default implementation. */
final class PartitionChunkImpl implements PartitionChunk {

  private final List<PartitionValue> partitionValues;
  private final long splitCount;
  private final List<DatasetSplit> datasetSplits;
  private final BytesOutput extraInfo;

  PartitionChunkImpl(
      List<PartitionValue> partitionValues,
      long splitCount,
      List<DatasetSplit> datasetSplits,
      BytesOutput extraInfo) {
    this.partitionValues = partitionValues;
    this.splitCount = splitCount;
    this.datasetSplits = datasetSplits;
    this.extraInfo = extraInfo;
  }

  @Override
  public List<PartitionValue> getPartitionValues() {
    return partitionValues;
  }

  @Override
  public long getSplitCount() {
    return splitCount;
  }

  @Override
  public DatasetSplitListing getSplits() {
    return datasetSplits::iterator;
  }

  @Override
  public BytesOutput getExtraInfo() {
    return extraInfo;
  }
}
