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
package com.dremio.datastore.api;

import com.dremio.datastore.indexed.IndexKey;

/**
 * Writer for adding documents to the index.
 */
public interface DocumentWriter {
  /**
   * Add the provided string values to the index.
   * @param key index key.
   * @param values values to be indexed.
   */
  void write(IndexKey key, String... values);

  /**
   * Add the provided byte values to the index.
   * @param key index key.
   * @param values values to be indexed.
   */
  void write(IndexKey key, byte[]... values);

  /**
   * Add the provided long values to the index.
   * @param key index key.
   * @param value values to be indexed.
   */
  void write(IndexKey key, Long value);

  /**
   * Add the provided double values to the index.
   * @param key index key.
   * @param value values to be indexed.
   */
  void write(IndexKey key, Double value);

  /**
   * Add the provided integer values to the index.
   * @param key index key.
   * @param value values to be indexed.
   */
  void write(IndexKey key, Integer value);

  /**
   * Add the provided Long values to the TTL index.
   * KVStores that support TTL based expiry should implement this method in their DocumentWriter implementation
   * @param key index key.
   * @param value TTL expireAt values to be indexed in long format.
   */
  default void writeTTLExpireAt(IndexKey key, Long value){
    // NO-OP by default
  }
}
