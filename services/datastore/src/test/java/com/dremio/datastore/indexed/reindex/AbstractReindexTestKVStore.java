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

package com.dremio.datastore.indexed.reindex;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.indexed.doughnut.Doughnut;
import com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys;
import com.dremio.datastore.indexed.doughnut.DoughnutIndexedStore;
import com.dremio.datastore.indexed.doughnut.UpgradedDoughnutStoreCreator;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Abstract class for reindex api */
public abstract class AbstractReindexTestKVStore {
  private static final Integer NUMBER_OF_DOUGHNUTS = 24;
  private static final Integer NUMBER_OF_FLAVORS = 4;

  private IndexedStore<String, Doughnut> kvStore;
  private IndexedStore<String, Doughnut> upgradedKVStore;

  protected abstract KVStoreProvider createKVStoreProvider() throws Exception;

  protected abstract KVStoreProvider createUpgradedKVStoreProvider() throws Exception;

  protected abstract void closeProvider() throws Exception;

  @Before
  public void init() throws Exception {
    KVStoreProvider kvStoreProvider = createKVStoreProvider();
    kvStore = kvStoreProvider.getStore(DoughnutIndexedStore.class);
  }

  @After
  public void shutdown() throws Exception {
    closeProvider();
  }

  private void initUpgradedStore() throws Exception {
    KVStoreProvider kvStoreProvider = createUpgradedKVStoreProvider();
    upgradedKVStore = kvStoreProvider.getStore(UpgradedDoughnutStoreCreator.class);
  }

  @Test
  @Ignore
  public void testReindex() throws Exception {
    addData(kvStore);
    verifyDocumentsWithFlavourExists(kvStore);

    initUpgradedStore();

    FindByCondition findByCondition =
        new ImmutableFindByCondition.Builder()
            .setPageSize(26)
            .setLimit(26)
            .setCondition(
                SearchQueryUtils.not(
                    SearchQueryUtils.newTermQuery(
                        DocumentConverter.VERSION_INDEX_KEY, upgradedKVStore.version())))
            .build();

    upgradedKVStore.reindex(findByCondition);
    verifyDocumentsWithNoFlavour(upgradedKVStore);
  }

  private void addData(IndexedStore<String, Doughnut> indexedStore) {
    for (int i = 0; i < NUMBER_OF_DOUGHNUTS; i++) {
      final String name = Integer.toString(i);
      indexedStore.put(name, new Doughnut(name, "good_flavor_" + (i % NUMBER_OF_FLAVORS), i));
    }
  }

  private void verifyDocumentsWithFlavourExists(IndexedStore<String, Doughnut> indexedStore) {
    verifyData(indexedStore, (count) -> Assert.assertTrue(count > 0));
  }

  private void verifyDocumentsWithNoFlavour(IndexedStore<String, Doughnut> indexedStore) {
    verifyData(indexedStore, (count) -> Assert.assertEquals(0, (long) count));
  }

  private void verifyData(
      IndexedStore<String, Doughnut> indexedStore, Consumer<Long> assertFunction) {
    for (int i = 0; i < NUMBER_OF_FLAVORS; i++) {
      final SearchTypes.SearchQuery nameQuery =
          SearchQueryUtils.newTermQuery(DoughnutIndexKeys.FLAVOR, "good_flavor_" + i);
      final FindByCondition condition =
          new ImmutableFindByCondition.Builder().setCondition(nameQuery).build();
      assertFunction.accept(
          StreamSupport.stream(indexedStore.find(condition).spliterator(), true).count());
    }
  }
}
