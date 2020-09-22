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

import com.dremio.datastore.api.Document;

/**
 * Comparator for comparing Map.Entry with K, V as key and value types.
 *
 * @param <K> key type K.
 * @param <V> value type V.
 */
public class DocumentComparator<K, V> implements Comparator<Document<K, V>> {
  private final Comparator<K> formatComparator;

  public DocumentComparator(Comparator<K> formatComparator) {
    this.formatComparator = formatComparator;
  }

  @Override
  public int compare(Document<K, V> o1, Document<K, V> o2) {
    return (formatComparator == null)?
      ((Comparable) o1.getKey()).compareTo(o2.getKey()) :
      formatComparator.compare(o1.getKey(), o2.getKey());
  }
}
