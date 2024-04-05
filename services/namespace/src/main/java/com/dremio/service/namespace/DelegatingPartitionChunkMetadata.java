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

import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.protobuf.ByteString;

/** Delegating implementation of PartitionChunkMetadata */
public class DelegatingPartitionChunkMetadata implements PartitionChunkMetadata {
  private final PartitionChunkMetadata inner;

  public DelegatingPartitionChunkMetadata(PartitionChunkMetadata inner) {
    this.inner = inner;
  }

  @Override
  public long getSize() {
    return inner.getSize();
  }

  @Override
  public long getRowCount() {
    return inner.getRowCount();
  }

  @Override
  public Iterable<PartitionProtobuf.PartitionValue> getPartitionValues() {
    return inner.getPartitionValues();
  }

  @Override
  public String getSplitKey() {
    return inner.getSplitKey();
  }

  @Override
  public int getSplitCount() {
    return inner.getSplitCount();
  }

  @Override
  public void mayGetDatasetSplits() {
    inner.mayGetDatasetSplits();
  }

  @Override
  public Iterable<PartitionProtobuf.DatasetSplit> getDatasetSplits() {
    return inner.getDatasetSplits();
  }

  @Override
  public ByteString getPartitionExtendedProperty() {
    return inner.getPartitionExtendedProperty();
  }

  @Override
  public Iterable<PartitionProtobuf.Affinity> getAffinities() {
    return inner.getAffinities();
  }

  @Override
  public PartitionProtobuf.NormalizedPartitionInfo getNormalizedPartitionInfo() {
    return inner.getNormalizedPartitionInfo();
  }

  @Override
  public boolean checkPartitionChunkMetadataConsistency() {
    return inner.checkPartitionChunkMetadataConsistency();
  }
}
