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
package com.dremio.datastore.adapter;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;

/**
 * Tests retrieval of legacy stores with invalid key format.
 * @param <K> key type K.
 * @param <V> value type V.
 */
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public abstract class AbstractTestLegacyInvalidStoreFormats<K, V> {
  /**
   * Creates a store with byte[] keys and values.
   * This store cannot be created by LegacyStoreLoader.
   * Retrieval of this store should result in a NullPointerException.
   */
  public static class LegacyByteKeyStore implements LegacyStoreCreationFunction<LegacyKVStore<byte[], byte[]>> {
    @Override
    public LegacyKVStore<byte[], byte[]> build(LegacyStoreBuildingFactory factory) {
      return factory.<byte[], byte[]>newStore()
        .name("legacy-byte-key-store")
        .keyFormat(Format.ofBytes())
        .valueFormat(Format.ofBytes())
        .build();
    }
  }

  /**
   * Creates a store with ByteContainer keys and values.
   * This store cannot be created by LegacyStoreLoader.
   * Retrieval of this store should result in a NullPointerException.
   */
  public static class LegacyByteContainerKeyStore implements LegacyStoreCreationFunction<LegacyKVStore<ByteContainerStoreGenerator.ByteContainer, ByteContainerStoreGenerator.ByteContainer>> {
    @Override
    public LegacyKVStore<ByteContainerStoreGenerator.ByteContainer, ByteContainerStoreGenerator.ByteContainer> build(LegacyStoreBuildingFactory factory) {
      return factory.<ByteContainerStoreGenerator.ByteContainer, ByteContainerStoreGenerator.ByteContainer>newStore()
        .name("legacy-byte-container-key-store")
        .keyFormat(Format.wrapped(ByteContainerStoreGenerator.ByteContainer.class, ByteContainerStoreGenerator.ByteContainer::getBytes, ByteContainerStoreGenerator.ByteContainer::new, Format.ofBytes()))
        .valueFormat(Format.wrapped(ByteContainerStoreGenerator.ByteContainer.class, ByteContainerStoreGenerator.ByteContainer::getBytes, ByteContainerStoreGenerator.ByteContainer::new, Format.ofBytes()))
        .build();
    }
  }

  @Parameterized.Parameters(name = "Table: {0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {LegacyByteKeyStore.class},
      {LegacyByteContainerKeyStore.class}
    });
  }

  // CHECKSTYLE:OFF VisibilityModifier
  @Parameterized.Parameter
  public Class<LegacyStoreCreationFunction<LegacyKVStore<K, V>>> storeCreationFunction;

  protected LegacyKVStoreProvider provider;

  protected abstract LegacyKVStoreProvider createProvider();

  @Before
  public void init() throws Exception {
    provider = createProvider();
    provider.start();
  }

  @After
  public void tearDown() throws Exception {
    provider.close();
  }

  @Test(expected = NullPointerException.class)
  public void testGetInvalidStore() {
    provider.getStore(storeCreationFunction);
  }
}
