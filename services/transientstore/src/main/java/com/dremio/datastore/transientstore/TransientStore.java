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
package com.dremio.datastore.transientstore;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import java.util.List;

/**
 * An interface for using it for caching purpose. Mostly supports a regular kvstore APIs with some
 * enhancements.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface TransientStore<K, V> {

  /**
   * Return document associated with the key, or {@code null} if no such entry exists.
   *
   * @param key the key to use to look for the value.
   * @param options extra options for GET operation.
   * @return the a Document with the value and tag associated with the key, or {@code null} if no
   *     such entry exists.
   */
  Document<K, V> get(K key, KVStore.GetOption... options);

  /**
   * Get the Documents for each of the keys provided.
   *
   * @param keys a list of keys which their values are to be retrieved.
   * @param options extra options for GET operations.
   * @return an Iterable of Documents associated with the list of keys provided. Iterable entry is
   *     {@code null} if no such value exists.
   */
  Iterable<Document<K, V>> get(List<K> keys, KVStore.GetOption... options);

  /**
   * Saves a document to the KV Store with the corresponding key. If the store already contains a
   * value associated with the key, the old value is discarded and replaced by the new value.
   *
   * @param key the key to save the value.
   * @param value the value to save.
   * @param options extra options for PUT operation.
   * @return the document that is updated or created, with the latest version tag.
   * @throws java.util.ConcurrentModificationException when VersionOption is passed in as a
   *     PutOption and that the version tag provided by VersionOption is outdated. The provided
   *     document is not saved nor updated.
   */
  Document<K, V> put(K key, V value, KVStore.PutOption... options);

  /**
   * Removes the entry with the provided key value. If version option is passed then it matches the
   * document tag with the version and deletes it and if version doesn't match, then throws
   * ConcurrentModificationException.
   *
   * @param key the key of the document to be removed from the KV Store.
   * @param options version option for DELETE operation.
   * @throws java.util.ConcurrentModificationException when VersionOption doesn't match with with
   *     the document version.
   */
  void delete(K key, KVStore.DeleteOption... options);

  /**
   * Checks if the Store contains a document corresponding to the provided key.
   *
   * @param key the key of the document to search for.
   * @return true if KV Store contains a document with the provided key value.
   */
  boolean contains(K key);

  /**
   * Find documents by the given pattern
   *
   * @param pattern
   * @param options
   * @return
   */
  Iterable<Document<K, V>> find(String pattern, KVStore.GetOption... options);
}
