/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.List;
import java.util.Map.Entry;

import com.dremio.exec.store.SplitsPointer;
import com.dremio.service.namespace.DatasetSplitId;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.google.common.base.Function;
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

  protected static Function<Entry<DatasetSplitId, DatasetSplit>, DatasetSplit> SPLIT_VALUES = new Function<Entry<DatasetSplitId, DatasetSplit>, DatasetSplit>() {
      @Override
      public DatasetSplit apply(Entry<DatasetSplitId, DatasetSplit> input) {
        return input.getValue();
      }
    };

  protected AbstractSplitsPointer() {
  }

  @Override
  public double getSplitRatio() {
    return ((double) getSplitsCount())/getTotalSplitsCount();
  }

  @Override
  public boolean isPruned() {
    return getTotalSplitsCount() != getSplitsCount();
  }

  @Override
  public SplitsPointer prune(Predicate<DatasetSplit> splitPredicate) {
    List<DatasetSplit> splits = FluentIterable.from(getSplitIterable()).filter(splitPredicate).toList();
    return new MaterializedSplitsPointer(splits, getTotalSplitsCount());
  }

  @Override
  public String computeDigest() {
    return Integer.toString(hashCode());
  }

}
