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

import java.io.IOException;

/**
 * A byte[], byte[] kvstore that is closeable and allows key deletions. This isn't on the main interface since it is for internal purposes.
 */
interface ByteStore extends KVStore<byte[], byte[]>, AutoCloseable {
  void deleteAllValues() throws IOException;

  /**
   * Validate the currently stored value before updating the store
   *
   * @param key the key
   * @param newValue the new value
   * @param validator a ByteValidator that ensures that the current item stored in the store for the key is valid
   * @return if the validation succeeded or not
   */
  boolean validateAndPut(byte[] key, byte[] newValue, ByteValidator validator);

  boolean validateAndDelete(byte[] key, ByteValidator validator);

  interface ByteValidator {
    boolean validate(byte[] oldValue);
  }
}
