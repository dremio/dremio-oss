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
package com.dremio.exec.store;

import java.util.Objects;

import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedDatasetSplitInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holder for the pair of Partition and split.
 */
public class SplitAndPartitionInfo {
  private final NormalizedPartitionInfo partitionInfo;
  private final NormalizedDatasetSplitInfo datasetSplitInfo;

  @JsonCreator
  public SplitAndPartitionInfo(@JsonProperty("partitionInfo") final NormalizedPartitionInfo partitionInfo,
                               @JsonProperty("splitInfo") final NormalizedDatasetSplitInfo datasetSplitInfo) {
    this.partitionInfo =  partitionInfo;
    this.datasetSplitInfo = datasetSplitInfo;
  }

  public NormalizedPartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  public NormalizedDatasetSplitInfo getDatasetSplitInfo() {
    return datasetSplitInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SplitAndPartitionInfo other = (SplitAndPartitionInfo) o;
    return partitionInfo.equals(other.partitionInfo) && datasetSplitInfo.equals(other.datasetSplitInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionInfo, datasetSplitInfo);
  }

  @Override
  public String toString() {
    return '{' + "partitionInfo=" + partitionInfo + ", splitInfo=" + datasetSplitInfo + '}';
  }
}
