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
package com.dremio.exec.catalog;


import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.service.namespace.LegacyPartitionChunkMetadata;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * An immutable pointer to a set of splits. Already materialized
 */
public final class MaterializedSplitsPointer extends AbstractSplitsPointer {
  private final long splitVersion;
  private final int totalSplitCount;
  private final List<PartitionChunkMetadata> materializedPartitionChunks;

  // Caching hashcode because computation might be expensive
  private volatile Integer computedHashcode = null;

  MaterializedSplitsPointer(long splitVersion, Iterable<PartitionChunkMetadata> partitionChunks, int totalSplitCount) {
    this.splitVersion = splitVersion;
    this.materializedPartitionChunks = ImmutableList.copyOf(partitionChunks);
    this.totalSplitCount = totalSplitCount;
  }

  /**
   * Create a pointer to a fully materialized list of splits for the given table definition
   * @param config
   * @return
   */
  public static SplitsPointer oldObsoleteOf(long splitVersion, Iterable<PartitionChunk> splits, int totalSplitCount) {
    Iterable<PartitionChunkMetadata> partitionChunks = FluentIterable
      .from(splits)
      .transform(LegacyPartitionChunkMetadata::new);
    return new MaterializedSplitsPointer(splitVersion, partitionChunks, totalSplitCount);
  }

  public static SplitsPointer of(long splitVersion, Iterable<PartitionChunkMetadata> partitionChunks, int totalSplitCount) {
    return new MaterializedSplitsPointer(splitVersion, partitionChunks, totalSplitCount);
  }

  /**
   * Prune a pointer to a fully materialized list of splits for the given table definition
   */
  public static SplitsPointer prune(SplitsPointer pointer, Iterable<PartitionChunkMetadata> partitionChunks) {
    return MaterializedSplitsPointer.of(pointer.getSplitVersion(), partitionChunks, pointer.getTotalSplitsCount());
  }

  @Override
  public long getSplitVersion() {
    return splitVersion;
  }

  @Override
  public SplitsPointer prune(SearchQuery partitionFilterQuery) {
    return this;
  }

  @Override
  public Iterable<PartitionChunkMetadata> getPartitionChunks() {
    return materializedPartitionChunks;
  }

  @Override
  public int getTotalSplitsCount() {
    return totalSplitCount;
  }

  @Override
  public boolean equals(final Object other) {
    // It is okay not to try to compare with a non-materialized splits pointer
    // as we are trying to be reasonably fast
    if (!(other instanceof MaterializedSplitsPointer)) {
      return false;
    }
    MaterializedSplitsPointer castOther = (MaterializedSplitsPointer) other;
    return this.splitVersion == castOther.splitVersion
        && Objects.equals(totalSplitCount, castOther.totalSplitCount)
        && splitsEquals(materializedPartitionChunks, castOther.materializedPartitionChunks);
  }

  @Override
  public int hashCode() {
    if (computedHashcode == null) {
      computedHashcode = Long.hashCode(splitVersion) + 31 * (totalSplitCount + hashSplits(materializedPartitionChunks));
    }
    return computedHashcode.intValue();

  }

  private static int hashSplits(List<PartitionChunkMetadata> partitionChunks) {
    // Similar to Arrays#hashCode(Object[])
    int result = 1;

    // Note: splitVersion is not included in hash to not make code too complex
    // May create more collision but version only changes because of a concurrent refresh.
    for (PartitionChunkMetadata partitionChunk : partitionChunks) {
      result = 31 * result + (partitionChunk == null ? 0 : Objects.hash(partitionChunk.getSplitKey()));
    }

    return result;
  }

  private static boolean splitsEquals(List<PartitionChunkMetadata> thisPartitionChunks, List<PartitionChunkMetadata> thatPartitionChunks) {
    if (thisPartitionChunks.size() != thatPartitionChunks.size()) {
      return false;
    }

    Iterator<PartitionChunkMetadata> thisIterator = thisPartitionChunks.iterator();
    Iterator<PartitionChunkMetadata> thatIterator = thatPartitionChunks.iterator();

    // Now assume that all splits have the same version for a given list.
    while(thisIterator.hasNext() && thatIterator.hasNext()) {
      PartitionChunkMetadata thisPartitionChunk = thisIterator.next();
      PartitionChunkMetadata thatPartitionChunk = thatIterator.next();

      boolean result = Objects.equals(thisPartitionChunk.getSplitKey(), thatPartitionChunk.getSplitKey());
      if (!result) {
        // Exit early if not matching
        return false;
      }
    }

    // Making sure that both iterators are at the same state (since the lists have the same size)
    Preconditions.checkState(thisIterator.hasNext() == thatIterator.hasNext());
    return true;
  }
}
