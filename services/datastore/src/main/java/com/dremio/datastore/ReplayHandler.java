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

/**
 * Handler to replay updates made to the {@link CoreKVStore}. Typically, the implementations are idempotent.
 */
public interface ReplayHandler {

  /**
   * Put entry with serialized key and serialized value to the table.
   *
   * @param tableName table name
   * @param key       serialized key
   * @param value     serialized value
   */
  void put(String tableName, byte[] key, byte[] value);

  /**
   * Delete entry with serialized key in the table.
   *
   * @param tableName table name
   * @param key       serialized key
   */
  void delete(String tableName, byte[] key);

  /**
   * Sink implementation.
   */
  ReplayHandler NO_OP = new ReplayHandler() {
    @Override
    public void put(String tableName, byte[] key, byte[] value) {
    }

    @Override
    public void delete(String tableName, byte[] key) {
    }
  };
}
