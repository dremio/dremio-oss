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

import java.util.Comparator;
import java.util.Map;

/**
 * Comparator for comparing Map.Entry with K, V as key and value types.
 *
 * @param <K> key type K.
 * @param <V> value type V.
 */
public class EntryComparator<K, V> implements Comparator<Map.Entry<K, V>> {
  private Comparator<K> formatComparator;

  public EntryComparator(Comparator<K> formatComparator) {
    this.formatComparator = formatComparator;
  }

  @Override
  public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
    return (formatComparator == null)
        ? ((Comparable) o1.getKey()).compareTo(o2.getKey())
        : formatComparator.compare(o1.getKey(), o2.getKey());
  }
}
