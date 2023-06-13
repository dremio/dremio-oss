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

import java.util.stream.StreamSupport;

import com.dremio.exec.store.SplitsPointer;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

/**
 * Base implementation of {@code SplitsPointer}
 *
 * check {@code MaterializedSplitsPointer} and {@code LazySplitsPointer} for
 * concrete implementations of {@code SplitsPointer}.
 *
 * Note that this class doesn't provide implementations for hashCode and equals
 * methods: each concrete class should provides a fast implementation for those
 * methods as we are more concerned about performance over strict equality
 * checking for the pointers.
 */
public abstract class AbstractSplitsPointer implements SplitsPointer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSplitsPointer.class);
  private int splitsCount;

  protected AbstractSplitsPointer() {
    splitsCount = -1;
  }

  @Override
  public abstract long getSplitVersion();

  @Override
  public double getSplitRatio() {
    final int totalSplitsCount = getTotalSplitsCount();
    // if totalSplitsCount is 0, splitsCount should be 0 too, and so ratio would be 1.0 since
    // same number of splits (making it consistent with isPruned())
    return totalSplitsCount != 0 ? ((double) getSplitsCount())/getTotalSplitsCount() : 1.0;
  }

  @Override
  public int getSplitsCount() {
    if (splitsCount != -1) {
      return splitsCount;
    }
    splitsCount = StreamSupport.stream(getPartitionChunks().spliterator(), false)
            .mapToInt(PartitionChunkMetadata::getSplitCount)
            .sum();
    return splitsCount;
  }

  @Override
  public boolean isPruned() {
    final boolean isPruned = getTotalSplitsCount() != getSplitsCount();

    // TODO(DX-15877): remove this block once the ticket is resolved
    if (isPruned) {
      logger.warn("Version: {}, total count: {}, split count: {}, partition chunks: {}, \nsplits: {}",
          getSplitVersion(), getTotalSplitsCount(), getSplitsCount(), FluentIterable.from(getPartitionChunks()).size(),
          FluentIterable.from(getPartitionChunks())
              .transform(input -> "[key: " + input.getSplitKey()
                  + ", count: " + input.getSplitCount()
                  + ", split count: " + FluentIterable.from(input.getDatasetSplits()).size()
                  + ", splits: " + FluentIterable.from(input.getDatasetSplits()).limit(5).join(Joiner.on(","))
                  + "]")
              .limit(5)
              .join(Joiner.on("\n"))
      );
    }

    return isPruned;
  }

  @Override
  public SplitsPointer prune(Predicate<PartitionChunkMetadata> partitionPredicate) {
    return MaterializedSplitsPointer.of(getSplitVersion(),
      FluentIterable.from(getPartitionChunks()).filter(partitionPredicate),
      getTotalSplitsCount());
  }

  @Override
  public String computeDigest() {
    return Integer.toString(hashCode());
  }

}
