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
package com.dremio.datastore;

import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * Small Utilities to work with kv store.
 */
public class KVUtil {

  /**
   * Converts an Iterable<Entry<K, V>> into a similarly ordered ImmutableMap<K, V>
   * @param Iterable to transform.
   * @return new ordered map of items.
   */
  public static <K, V> ImmutableMap<K, V> asMap(Iterable<Entry<K, V>> iterable){
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    for(Entry<K, V> entry : iterable){
      builder.put(entry.getKey(), entry.getValue());
    }
    return builder.build();

  }

  /**
   * Get the values for a Iterable<Entry<K, V>>
   * @param Iterable to lazily transform.
   * @return new Iterable of type <K>
   */
  public static <K, V> Iterable<V> values(Iterable<Entry<K, V>> iterable){
    return Iterables.transform(iterable, new Function<Entry<K, V>, V>(){
      @Override
      public V apply(Entry<K, V> input) {
        return input.getValue();
      }});
  }

  /**
   * Get the keys for a Iterable<Entry<K, V>>
   * @param iterable Iterable to lazily transform.
   * @return new Iterable of type <K>
   */
  public static <K, V> Iterable<K> keys(Iterable<Entry<K, V>> iterable){
    return Iterables.transform(iterable, new Function<Entry<K, V>, K>(){
      @Override
      public K apply(Entry<K, V> input) {
        return input.getKey();
      }});
  }

}
