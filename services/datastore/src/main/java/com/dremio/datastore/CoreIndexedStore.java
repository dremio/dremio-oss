/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;
import java.util.Map;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchTypes.SearchQuery;

/**
 * Core indexed store.
 */
public interface CoreIndexedStore<K, V> extends CoreKVStore<K, V> {

  /**
   * Search kvstore and return matching values for given condition.
   * @param find contains search queries/sorting conditions to search for.
   * @return matching key values
   */
  Iterable<Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByCondition find);

  /**
   * Provide a count of the number of documents that match each of the requested
   * conditions.
   *
   * @param conditions
   * @return
   */
  List<Integer> getCounts(SearchQuery... conditions);

  /**
   * ReIndex all the entries in the store
   * @return number of entries which got reIndexed.
   */
   int reIndex();

}
