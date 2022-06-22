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
package com.dremio.exec.store.hive.metadata;

import java.util.Arrays;
import java.util.Collections;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;

/**
 * Helper class to build proto-compatible dictionaries for {@link com.dremio.hive.proto.HiveReaderProto.HiveTableXattr}
 * dictionary members.
 * <p>
 * T must implement {@link Object#hashCode}.
 */
public class DictionaryBuilder<T> {
  private static final int DEFAULT_VALUE = -1;

  private final ObjectIntHashMap<T> valueAccumulator;
  private final Class<T> clazzType;

  private int lookupIndex;
  private boolean isBuilt;

  public DictionaryBuilder(final Class<T> clazzType) {
    this.clazzType = clazzType;
    this.valueAccumulator = new ObjectIntHashMap<>();
    this.lookupIndex = -1;
    this.isBuilt = false;
  }

  /**
   * Checks if the value is already stored in the dictionary and returns the index. Otherwise the value
   * will be added to the dictionary and the new index will be returned.
   *
   * @param value The value to look up or store in the dictionary.
   * @return the index into the dictionary which can be used when looking up the value later.
   */
  public int getOrCreateSubscript(final T value) {
    Preconditions.checkArgument(!isBuilt, "can not store values after build is called");
    Preconditions.checkNotNull(value, "can not store null values");

    final int existingLookupIndex = valueAccumulator.getOrDefault(value, DEFAULT_VALUE);
    if (DEFAULT_VALUE != existingLookupIndex) {
      return existingLookupIndex;
    }

    valueAccumulator.put(value, ++lookupIndex);

    return lookupIndex;
  }

  public Iterable<T> build() {
    Preconditions.checkArgument(!isBuilt, "build can only be called once");
    isBuilt = true;

    if (valueAccumulator.isEmpty()) {
      return Collections.emptyList();
    }

    final T[] values = ObjectArrays.newArray(clazzType, valueAccumulator.size());

    for (ObjectIntCursor<T> entry : valueAccumulator) {
      values[entry.value] = entry.key;
    }

    return Arrays.asList(values);
  }
}
