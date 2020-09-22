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
package com.dremio.datastore;

import java.io.IOException;
import java.util.Base64;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.VersionOption;
import com.google.common.hash.Hashing;

/**
 * A byte[], byte[] kvstore that is closeable and allows key deletions. This isn't on the main interface since it is for internal purposes.
 */
interface ByteStore extends KVStore<byte[], byte[]>, AutoCloseable {
  void deleteAllValues() throws IOException;

  /**
   * Validate the currently stored value before updating the store.
   *
   * @param key the key.
   * @param newValue the new value.
   * @param versionInfo information about the tag the caller is expecting.
   * @param options Options for the put operation.
   * @return a non-null Document if validation succeeded.
   */
  Document<byte[], byte[]> validateAndPut(byte[] key, byte[] newValue, VersionOption.TagInfo versionInfo, PutOption... options);

  /**
   * Validate the currently stored value before deleting the key-value entry.
   *
   * @param key the key.
   * @param versionInfo the TagInfo.
   * @param options Options for the delete operation.
   * @return true if delete was successful, false otherwise.
   */
  boolean validateAndDelete(byte[] key, VersionOption.TagInfo versionInfo, DeleteOption... options);

  /**
   * Generates a tag by fingerprinting the value of the key-value store entry.
   *
   * @param value the value to fingerprint.
   * @return a String representation of the fingerprint generated.
   */
  static String generateTagFromBytes(byte[] value) {
    return Base64.getEncoder().encodeToString(Hashing.farmHashFingerprint64().hashBytes(value, 0, value.length).asBytes());
  }
}
