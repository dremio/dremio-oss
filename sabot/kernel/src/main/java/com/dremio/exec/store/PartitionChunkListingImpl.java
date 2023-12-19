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

import java.util.Iterator;
import java.util.List;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Implementation of PartitionChunkListing for eager evaluation of partition chunks.
 */
public class PartitionChunkListingImpl implements PartitionChunkListing {

  private final int maxSplitsPerChunk;
  private final ArrayListMultimap<List<PartitionValue>, DatasetSplit> partitionChunkEntry;
  private List<PartitionChunk> partitionChunks;

  public PartitionChunkListingImpl() {
    this(Integer.MAX_VALUE);
  }

  public PartitionChunkListingImpl(int maxSplitsPerChunk) {
    this.maxSplitsPerChunk = maxSplitsPerChunk;
    this.partitionChunkEntry = ArrayListMultimap.create();

  }

  public void computePartitionChunks() {
    final ImmutableList.Builder<PartitionChunk> builder = ImmutableList.builder();

    for (List<PartitionValue> key : partitionChunkEntry.keySet()) {
      // Partition the full list of splits for this partition into sub-lists based on maxSplitsPerChunk
      for (List<DatasetSplit> splits : Lists.partition(partitionChunkEntry.get(key), maxSplitsPerChunk)) {
        builder.add(PartitionChunk.of(key, splits));
      }
    }
    partitionChunks = builder.build();
  }

  public void put(List<PartitionValue> partitionValue, DatasetSplit split) {
    Preconditions.checkState(!computed(), "Cannot add splits after partition chunks have been iterated over.");
    partitionChunkEntry.put(partitionValue, split);
  }

  public boolean computed() {
    return partitionChunks != null;
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    if (!computed()) {
      computePartitionChunks();
    }
    return partitionChunks.iterator();
  }
}
