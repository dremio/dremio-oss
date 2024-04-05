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

import com.dremio.datastore.api.Document;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

/**
 * Dataset helper class used for testing kv stores. Essentially a container for keys and values.
 *
 * <p>IMPORTANT: If keys cannot be sorted from least to greatest, then the format does not support
 * find and must be marked as such in tests.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class DocumentDataset<K, V> {
  private final List<Document<K, V>> documents;

  public DocumentDataset(List<Document<K, V>> docs) {
    documents = docs;
  }

  public DocumentDataset(Iterable<Document<K, V>> docs) {
    final List<Document<K, V>> documentList = new ArrayList<>();
    docs.forEach(documentList::add);
    documents = documentList;
  }

  public Document<K, V> getDocument(int n) {
    return documents.get(n);
  }

  public List<Document<K, V>> getAllDocuments() {
    return documents;
  }

  public List<K> getKeys() {
    final List<K> keys = new ArrayList<>();
    documents.forEach(doc -> keys.add(doc.getKey()));
    return keys;
  }

  /**
   * Retrieves a subset of the dataset.
   *
   * @param start start of slice, inclusive.
   * @param end end of slice, inclusive.
   * @return a map of keys paired to their respective values.
   */
  public Iterable<Document<K, V>> getDocumentDatasetSlice(int start, int end) {
    Preconditions.checkArgument(start <= end);
    Preconditions.checkArgument(end < documents.size());
    // Sublist toIndex is exclusive
    return documents.subList(start, end + 1);
  }
}
