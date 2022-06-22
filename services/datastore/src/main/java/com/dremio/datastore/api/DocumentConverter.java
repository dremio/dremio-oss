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
 * Converter that converts a value into a indexable document.
 *
 * @param <K> The Key type to convert.
 * @param <V> The value type to convert.
 */
public interface DocumentConverter<K, V> {
  IndexKey VERSION_INDEX_KEY = IndexKey.newBuilder("version", "version", Integer.class).build();

  /**
   * Does the actual conversion after pre conversion step.
   * Callers of conversion logic should call this method instead of
   * {@link #convert(DocumentWriter, Object, Object)} convert}
   *
   * @param writer the document writer.
   * @param key the key of the document.
   * @param record the value of the document.
   */
  default void doConvert(DocumentWriter writer, K key, V record) {
    writer.write(VERSION_INDEX_KEY, getVersion());
    convert(writer, key, record);
  }

  /**
   * Convert the key/value pair using the providing writer. All
   * implementations should implement convert and may not override
   * {@link #doConvert(DocumentWriter, Object, Object)}
   *
   * @param writer the document writer.
   * @param key the key of the document.
   * @param record the value of the document.
   */
  void convert(DocumentWriter writer, K key, V record);

  /**
   * Version of the Indices.
   *
   * @return version.
   */
  Integer getVersion();
}
