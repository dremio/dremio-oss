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

import java.io.IOException;
import java.io.InputStream;

import org.xerial.snappy.SnappyInputStream;

import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.MultiSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

/**
 * A partition chunk represented by a pair of objects:
 * - a PartitionChunk proto
 * - if the partition chunk has more than one split, a MultiSplit proto
 * The MultiSplit proto is lazily instantiated -- only created when users ask for the splits of this partition chunk
 *
 * Note: if the parition chunk contains only a single split,
 */
public class PartitionChunkMetadataImpl extends AbstractPartitionChunkMetadata {
  private final Supplier<MultiSplit> multiSplitSupplier;
  private Iterable<DatasetSplit> materializedDatasetSplits;  // Materialized only on first lookup

  /**
   * Constructor
   * @param partitionChunk           The partition chunk. Can be either one of the three flavors (see comments in partition.proto)
   * @param multiSplitSupplier   When invoked, materializes the backing MultiSplit that in turn contains this partition chunk's
   *                                 dataset splits
   */
  public PartitionChunkMetadataImpl(PartitionChunk partitionChunk, Supplier<MultiSplit> multiSplitSupplier) {
    super(partitionChunk);
    Preconditions.checkNotNull(multiSplitSupplier);
    Preconditions.checkArgument(partitionChunk.hasSplitCount(), "Must be constructed with a partitionChunk with a set split_count");
    this.multiSplitSupplier = Suppliers.memoize(multiSplitSupplier);
    this.materializedDatasetSplits = null;
  }

  @Override
  public Iterable<DatasetSplit> getDatasetSplits() {
    if (materializedDatasetSplits != null) {
      return materializedDatasetSplits;
    }

    PartitionChunk partitionChunk = getPartitionChunk();
    if (partitionChunk.hasDatasetSplit()) {
      Preconditions.checkState(partitionChunk.getSplitCount() == 1,
        String.format("Only a partition chunk with 1 split should have a dataset split set directly. split_count == %d", partitionChunk.getSplitCount()));
      materializedDatasetSplits = ImmutableList.of(partitionChunk.getDatasetSplit());
      return materializedDatasetSplits;
    }
    Preconditions.checkState(partitionChunk.getSplitCount() > 1,
      String.format("Partition chunk with 1 split should have a dataset split set directly. split_count == %d", partitionChunk.getSplitCount()));
    // Build a list of dataset splits from the now-materialized multiset split
    MultiSplit multiSplit = multiSplitSupplier.get();    // NB: multiSplit cached in the supplier
    final long splitCount = multiSplit.getSplitCount();
    Preconditions.checkState(splitCount == partitionChunk.getSplitCount());
    ImmutableList.Builder<DatasetSplit> datasetSplits = new ImmutableList.Builder<>();
    InputStream splitDataStream = multiSplit.getSplitData().newInput();
    try {
      switch (multiSplit.getCodec()) {
        case UNCOMPRESSED:
          // nothing to do
          break;
        case SNAPPY:
          splitDataStream = new SnappyInputStream(splitDataStream);
          break;
        case UNKNOWN:
        default:
          throw new IllegalStateException("Unsupported multi-split codec: " + multiSplit.getCodec());
      }
      for (long i = 0; i < splitCount; i++) {
        datasetSplits.add(DatasetSplit.parseDelimitedFrom(splitDataStream));
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unable to get dataset splits from partition chunk with key %s", getSplitKey()), e);
    }

    materializedDatasetSplits = datasetSplits.build();
    return materializedDatasetSplits;
  }
}
