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
package com.dremio.service.namespace;

import java.util.concurrent.atomic.AtomicLong;

import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.Affinity;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

/**
 * Provides access to the members of a partition chunk proto
 */
public abstract class AbstractPartitionChunkMetadata implements PartitionChunkMetadata {
  private static final AtomicLong idGenerator = new AtomicLong(0L);
  private final PartitionChunk partitionChunk;
  private final NormalizedPartitionInfo normalizedPartitionInfo;

  AbstractPartitionChunkMetadata(PartitionChunk partitionChunk) {
    this.partitionChunk = partitionChunk;

    // we need a unique key per partition (atleast at the operator level), which is used as a
    // reference key in the normalized splits. This works fine, but is a bit hacky.
    this.normalizedPartitionInfo = NormalizedPartitionInfo
      .newBuilder()
      .setId(String.valueOf(idGenerator.incrementAndGet()))
      .setSplitKey(partitionChunk.getSplitKey())
      .setSize(partitionChunk.getSize())
      .setExtendedProperty(partitionChunk.getPartitionExtendedProperty())
      .addAllValues(partitionChunk.getPartitionValuesList())
      .build();
  }

  protected PartitionChunk getPartitionChunk() {
    return partitionChunk;
  }

  @Override
  public long getSize() {
    return partitionChunk.getSize();
  }

  @Override
  public long getRowCount() {
    return partitionChunk.getRowCount();
  }

  @Override
  public Iterable<PartitionValue> getPartitionValues() {
    return partitionChunk.getPartitionValuesList();
  }

  @Override
  public String getSplitKey() {
    return partitionChunk.getSplitKey();
  }

  @Override
  public int getSplitCount() {
    return Math.toIntExact(partitionChunk.getSplitCount());
  }

  @Override
  public ByteString getPartitionExtendedProperty() {
    return partitionChunk.getPartitionExtendedProperty();
  }

  @Override
  @Deprecated
  public Iterable<Affinity> getAffinities() {
    return partitionChunk.getAffinitiesList();
  }

  @Override
  public NormalizedPartitionInfo getNormalizedPartitionInfo() {
    return normalizedPartitionInfo;
  }

  @VisibleForTesting
  boolean hasDatasetSplit() {
    return partitionChunk.hasDatasetSplit();
  }
}
