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
package com.dremio.datastore.utility;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.dremio.datastore.StoreReplacement;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.api.StoreCreationFunction;
import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/** StoreLoader tests */
public class TestStoreLoader {
  private static IndexedStore testMock = mock(IndexedStore.class);
  private static IndexedStore replacementMock = mock(IndexedStore.class);

  @Test
  public void testReplacement() throws Exception {
    Set<Class<? extends StoreCreationFunction>> impls = new HashSet<>();

    impls.add(TestCreator.class);
    impls.add(ReplacementTestCreator.class);

    ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>> map =
        StoreLoader.buildStores(impls, mock(StoreBuildingFactory.class));

    // TestCreate and ReplacementTestCreator should point at the replacement mock
    assertEquals(2, map.size());
    assertEquals(map.get(TestCreator.class), replacementMock);
    assertEquals(map.get(TestCreator.class), map.get(ReplacementTestCreator.class));
  }

  /** Test LegacyIndexedStoreCreationFunction */
  public static class TestCreator implements LegacyIndexedStoreCreationFunction<String, String> {
    @Override
    public LegacyIndexedStore<String, String> build(LegacyStoreBuildingFactory factory) {
      return null;
    }

    @Override
    public IndexedStore<String, String> build(StoreBuildingFactory factory) {
      return testMock;
    }
  }

  /** Replacement Test LegacyIndexedStoreCreationFunction */
  @StoreReplacement(TestCreator.class)
  public static class ReplacementTestCreator
      implements LegacyIndexedStoreCreationFunction<String, String> {
    @Override
    public LegacyIndexedStore<String, String> build(LegacyStoreBuildingFactory factory) {
      return null;
    }

    @Override
    public IndexedStore<String, String> build(StoreBuildingFactory factory) {
      return replacementMock;
    }
  }
}
