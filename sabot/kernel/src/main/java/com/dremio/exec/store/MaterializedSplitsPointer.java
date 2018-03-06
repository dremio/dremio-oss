/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.Objects;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * An immutable pointer to a set of splits. Already materialized
 */
public final class MaterializedSplitsPointer extends AbstractSplitsPointer {
  private final int totalSplitCount;
  private final List<DatasetSplit> materializedSplits;
  private final Integer splitCount;

  // Caching hashcode because computation might be expensive
  private volatile Integer computedHashcode = null;

  MaterializedSplitsPointer(Iterable<DatasetSplit> splits, int totalSplitCount) {
    this.materializedSplits = ImmutableList.copyOf(splits);
    this.totalSplitCount = totalSplitCount;
    this.splitCount = materializedSplits.size();
  }

  /**
   * Create a pointer to a fully materialized list of splits for the given table definition
   * @param config
   * @return
   */
  public static SplitsPointer of(List<DatasetSplit> splits, int totalSplitCount) {
    return new MaterializedSplitsPointer(splits, totalSplitCount);
  }

  @Override
  public void materialize(){
    // No-op
  }

  @Override
  public SplitsPointer prune(SearchQuery partitionFilterQuery) {
    return this;
  }

  @Override
  public Iterable<DatasetSplit> getSplitIterable() {
    return materializedSplits;
  }

  @Override
  public int getSplitsCount() {
    return splitCount;
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
    return Objects.equals(totalSplitCount, castOther.totalSplitCount)
        && splitsEquals(materializedSplits, castOther.materializedSplits);
  }

  @Override
  public int hashCode() {
    if (computedHashcode == null) {
      computedHashcode = Integer.valueOf(31 * totalSplitCount + hashSplits(materializedSplits));
    }
    return computedHashcode.intValue();

  }

  private static int hashSplits(List<DatasetSplit> splits) {
    // Similar to Arrays#hashCode(Object[])
    int result = 1;

    // Note: splitVersion is not included in hash to not make code too complex
    // May create more collision but version only changes because of a concurrent refresh.
    for (DatasetSplit split : splits) {
      result = 31 * result + (split == null ? 0 : Objects.hash(split.getSplitKey()));
    }

    return result;
  }

  private static boolean splitsEquals(List<DatasetSplit> thisSplits, List<DatasetSplit> thatSplits) {
    if (thisSplits.size() != thatSplits.size()) {
      return false;
    }

    Iterator<DatasetSplit> thisIterator = thisSplits.iterator();
    Iterator<DatasetSplit> thatIterator = thatSplits.iterator();

    // First check: check both key and version
    if (thisIterator.hasNext() && thatIterator.hasNext()) {
      DatasetSplit thisSplit = thisIterator.next();
      DatasetSplit thatSplit = thatIterator.next();

      boolean result = Objects.equals(thisSplit.getSplitVersion(), thatSplit.getSplitVersion())
          && Objects.equals(thisSplit.getSplitKey(), thatSplit.getSplitKey());
      if (!result) {
        // Exit early if not matching
        return false;
      }
    } else {
      // both lists are empty
      return true;
    }

    // Now assume that all splits have the same version for a given list.
    while(thisIterator.hasNext() && thatIterator.hasNext()) {
      DatasetSplit thisSplit = thisIterator.next();
      DatasetSplit thatSplit = thatIterator.next();

      boolean result = Objects.equals(thisSplit.getSplitKey(), thatSplit.getSplitKey());
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
