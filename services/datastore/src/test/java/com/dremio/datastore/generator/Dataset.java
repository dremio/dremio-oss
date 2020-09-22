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
package com.dremio.datastore.generator;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Dataset helper class used for testing kv stores.
 * Essentially a container for keys and values.
 *
 * IMPORTANT: If keys cannot be sorted from least to greatest, then the format does not support
 * find and must be marked as such in tests.
 * @param <K> key type
 * @param <V> value type
 */
public class Dataset<K, V> {
  private final List<K> keys;
  private final List<V> vals;

  Dataset(List<K> ks, List<V> vs) {
    Preconditions.checkArgument(ks.size() == vs.size());
    keys = ks;
    vals = vs;
  }

  public K getKey(int n) {
    return keys.get(n);
  }

  public V getVal(int n) {
    return vals.get(n);
  }

  public List<K> getKeys() {
    return keys;
  }

  public List<V> getValues() {
    return vals;
  }

  public Iterable<Map.Entry<K, V>> getDatasetSlice(int start, int end) {
    Preconditions.checkArgument(start <= end);
    Preconditions.checkArgument(end < keys.size());
    Preconditions.checkArgument(end < vals.size());

    final ImmutableList.Builder<Map.Entry<K, V>> entriesBuilder = ImmutableList.builder();
    for (int i = start; i <= end; i++) {
      entriesBuilder.add(new AbstractMap.SimpleEntry<>(getKey(i), getVal(i)));
    }
    return entriesBuilder.build();
  }
}
