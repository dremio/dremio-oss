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

/**
 * Function used to build a KVStore. Class is used as a key to later access the singleton KVStore.
 *
 * @param <T> KVStore of type T.
 */
public interface StoreCreationFunction<T extends KVStore<?, ?>> {
  /**
   * How to build this KVStore.
   * @param factory StoreBuildingFactory to indicate how to build this store.
   * @return KVStore of type T.
   */
  T build(StoreBuildingFactory factory);
}
