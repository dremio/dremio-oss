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

import java.util.List;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;

/**
 * Core indexed store.
 */
public interface CoreIndexedStore<K, V> extends CoreKVStore<K, V> {

  String ID_FIELD_NAME = "_id";

  /**
   * Search kvstore and return matching values for given condition.
   * @param find contains search queries/sorting conditions to search for.
   * @param options the search options.
   * @return matching key values
   */
  Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByCondition find, FindOption... options);

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
   int reindex();

  /**
   * Version for the indicies.
   *
   *  @return version number.
   */
  Integer version();
}
