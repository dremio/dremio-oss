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

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;
import com.dremio.datastore.generator.DataGenerator;
import com.dremio.datastore.generator.ProtobufStoreGenerator;
import com.dremio.datastore.generator.ProtostuffStoreGenerator;
import com.dremio.datastore.generator.RawByteStoreGenerator;
import com.dremio.datastore.generator.StringStoreGenerator;
import com.dremio.datastore.generator.UUIDStoreGenerator;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;
import com.dremio.datastore.stores.ByteContainerStore;
import com.dremio.datastore.stores.ProtobufStore;
import com.dremio.datastore.stores.ProtostuffStore;
import com.dremio.datastore.stores.RawByteStore;
import com.dremio.datastore.stores.StringStore;
import com.dremio.datastore.stores.UUIDStore;

/**
 * Tests OCCKVStore
 */
@RunWith(Parameterized.class)
public abstract class AbstractTestOCCKVStore<K, V> {
  private static final String TAG_ASSERT_FAILURE_MSG = "All documents should have a non-null, non-empty tag";

  private KVStore<K, V> kvStore;
  private KVStoreProvider provider;

  @Parameter
  public Class<? extends StoreCreationFunction<K, V, KVStore<K, V>>> storeCreationFunction;

  @Parameter(1)
  public DataGenerator<K, V> gen;

  @Parameterized.Parameters(name = "Table: {0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {UUIDStore.class, new UUIDStoreGenerator()},
      {ProtobufStore.class, new ProtobufStoreGenerator()},
      {ProtostuffStore.class, new ProtostuffStoreGenerator()},
      //Variable-length
      {StringStore.class, new StringStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH)},
      {RawByteStore.class, new RawByteStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH)},
      {ByteContainerStore.class, new ByteContainerStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH)},
      //Fixed-length
      {StringStore.class, new StringStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH)},
      {RawByteStore.class, new RawByteStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH)},
      {ByteContainerStore.class, new ByteContainerStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH)}
    });
  }

  protected abstract KVStoreProvider createKVStoreProvider() throws Exception;
  protected void closeResources() throws Exception {
  }

  @SuppressWarnings("resource")
  @Before
  public void before() throws Exception {
    provider = createKVStoreProvider();
    kvStore = provider.getStore(storeCreationFunction);
    gen.reset();
  }

  @After
  public void after() throws Exception {
    closeResources();
    provider.close();
  }

  @Test(expected = NullPointerException.class)
  public void testShouldNotPutNulls() {
    OCCKVStoreTests.testShouldNotPutNulls(kvStore, gen);
  }

  @Test
  public void testPutWithoutCreateOptionIsValid() {
    OCCKVStoreTests.testPutWithoutCreateOptionIsValid(kvStore, gen);
  }

  @Test
  public void testPutWithCreateOption() {
    OCCKVStoreTests.testPutWithCreateOption(kvStore, gen);
  }

  @Test
  public void testPutWithCreateOptionFailsToOverwrite() {
    OCCKVStoreTests.testPutWithCreateOptionFailsToOverwrite(kvStore, gen);
  }

  @Test
  public void testModificationWithoutOptionsAlwaysOverwrites() {
    OCCKVStoreTests.testModificationWithoutOptionsAlwaysOverwrites(kvStore, gen);
  }

  @Test
  public void testPutAfterDeleteWithoutOptions() {
    OCCKVStoreTests.testPutAfterDeleteWithoutOptions(kvStore, gen);
  }

  @Test
  public void testPutAlwaysUsingOptionsShouldUpdate() {
    OCCKVStoreTests.testPutAlwaysUsingOptionsShouldUpdate(kvStore, gen);
  }

  @Test
  public void testPutLateOptionAdoptionShouldStillUpdate() {
    OCCKVStoreTests.testPutLateOptionAdoptionShouldStillUpdate(kvStore, gen);
  }

  @Test
  public void testPutLateOptionAdoptionShouldStillProtectFromCM() {
    OCCKVStoreTests.testPutLateOptionAdoptionShouldStillProtectFromCM(kvStore, gen);
  }

  @Test
  public void testCreateWithManyOptionsFails() {
    OCCKVStoreTests.testCreateWithManyOptionsFails(kvStore, gen);
  }

  @Test
  public void testPutWithMultipleVersionsFails() {
    OCCKVStoreTests.testPutWithMultipleVersionsFails(kvStore, gen);
  }

  @Test
  public void testPutWithOutdatedVersionFails() {
    OCCKVStoreTests.testPutWithOutdatedVersionFails(kvStore, gen);
  }

  @Test
  public void testDeleteWithoutTagAlwaysDeletes() {
    OCCKVStoreTests.testDeleteWithoutTagAlwaysDeletes(kvStore, gen);
  }

  @Test
  public void testDeleteWithOutdatedVersionFails() {
    OCCKVStoreTests.testDeleteWithOutdatedVersionFails(kvStore, gen);
  }

  @Test
  public void testDeleteWithLateOptionShouldStillProtectFromCM() {
    OCCKVStoreTests.testDeleteWithLateOptionShouldStillProtectFromCM(kvStore, gen);
  }

  @Test
  public void testDeleteWithLatestVersionShouldDelete() {
    OCCKVStoreTests.testDeleteWithLatestVersionShouldDelete(kvStore, gen);
  }

  @Test
  public void testDeleteWithLateVersionAdoptionShouldDelete() {
    OCCKVStoreTests.testDeleteWithLateVersionAdoptionShouldDelete(kvStore, gen);
  }

  @Test
  public void testDeleteWithManyVersionsFails() {
    OCCKVStoreTests.testDeleteWithManyVersionsFails(kvStore, gen);
  }
}
