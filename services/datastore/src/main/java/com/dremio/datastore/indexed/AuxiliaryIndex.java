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
package com.dremio.datastore.indexed;

import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;

/**
 * Auxilary index for a KV store. The auxiliary index uses the same key as the KV store but uses a
 * separate DocumentConverter when building the index. Allows the store to be searched by the
 * auxiliary index.
 *
 * @param <K> - The KV store key
 * @param <V> - The value of the KV store
 * @param <T> - The value used to build the auxiliary index
 */
public interface AuxiliaryIndex<K, V, T> {
  void index(K key, T indexValue);

  Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByCondition condition);
}
