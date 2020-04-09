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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.dremio.datastore.api.Document;
import com.google.common.collect.ImmutableList;

/**
 * Used by abstract kv store tests to generate test data.
 *
 * Should issue values in increasing order.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface DataGenerator<K, V> {

  /**
   * Generates a new key of type K.
   *
   * @return a new key.
   */
  K newKey();

  /**
   * Generates a new value of type V.
   *
   * @return a new value.
   */
  V newVal();

  /**
   * Generates a new value of type V with null fields.
   *
   * @return a new value with null fields.
   */
  V newValWithNullFields();

  default void reset() {
    //No-op
  }

  /**
   * Compares two keys and fails if they are not identical.
   *
   * @param expected the expected key.
   * @param actual the actual key.
   */
  default void assertKeyEquals(K expected, K actual) {
    assertThat(actual, is(equalTo(expected)));
  }

  /**
   * Compares two values and fails if they are not identical.
   *
   * @param expected the expected value.
   * @param actual the actual value.
   */
  default void assertValueEquals(V expected, V actual) {
    assertThat(actual, is(equalTo(expected)));
  }

  /**
   * Retrieves the object comparator of the generator.
   *
   * @return {@code null} by default.
   */
  default Comparator<K> getComparator() {
    return null;
  }

  /**
   * Convert an unsorted dataset into a sorted dataset by sorting the keys.
   * Used by tests for correct range sampling.
   *
   * @param dataset the unsorted dataset.
   * @return a dataset sorted by keys.
   */
  default Dataset<K, V> sortDataset(Dataset<K, V> dataset) {
    final Iterable<Map.Entry<K, V>> entries = dataset.getDatasetSlice(0, dataset.getKeys().size() - 1);
    final ImmutableList.Builder<K> keys = ImmutableList.builderWithExpectedSize(dataset.getKeys().size());
    final ImmutableList.Builder<V> vals = ImmutableList.builderWithExpectedSize(dataset.getValues().size());

    StreamSupport.stream(entries.spliterator(), false)
      .sorted(new EntryComparator<>(getComparator()))
      .forEach(entry -> {
        keys.add(entry.getKey());
        vals.add(entry.getValue());
      });

    return new Dataset<>(keys.build(), vals.build());
  }

  /**
   * Sort an Iterable of Map.Entry with key type K and value type V.
   *
   * @param entries an Iterable of unsorted Map.Entry with key type K and value type V.
   * @return an Iterable sorted by keys.
   */
  default Iterable<Map.Entry<K, V>> sortEntries(Iterable<Map.Entry<K, V>> entries) {
    return StreamSupport.stream(entries.spliterator(), false)
      .sorted(new EntryComparator<>(getComparator()))
      .collect(Collectors.toList());
  }

  /**
   * Creates an unsorted dataset given the number of key, value pairs.
   *
   * @param nPairs number of key, value pairs in the dataset.
   * @return a dataset sorted by keys.
   */
  default Dataset<K, V> makeDataset(int nPairs) {
    final ImmutableList.Builder<K> keys = ImmutableList.builderWithExpectedSize(nPairs);
    final ImmutableList.Builder<V> vals = ImmutableList.builderWithExpectedSize(nPairs);
    for (int i = 0; i < nPairs; i++) {
      keys.add(this.newKey());
      vals.add(this.newVal());
    }
    return new Dataset<>(keys.build(), vals.build());
  }

  /**
   * Sorts an unsorted DocumentDataset by sorting the keys.
   * Used by tests for correct range sampling.
   *
   * @param documentDataset the unsorted DocumentDataset.
   * @return a DocumentDataset sorted by keys.
   */
  default DocumentDataset<K, V> sortDocumentDataset(DocumentDataset<K, V> documentDataset) {
    return new DocumentDataset<>(sortDocuments(documentDataset.getAllDocuments()));
  }

  /**
   * Sorts an unsorted Iterable of Documents by sorting the keys.
   * Used by tests for correct range sampling.
   *
   * @param documents the unsorted Iterable of Documents.
   * @return an Iterable of Documents sorted by keys.
   */
  default Iterable<Document<K, V>> sortDocuments(Iterable<Document<K, V>> documents) {
    final List<Document<K, V>> documentList = new ArrayList<>();
    documents.forEach(documentList::add);
    documentList.sort(new DocumentComparator<>(getComparator()));
    return documentList;
  }
}
