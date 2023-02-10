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
package com.dremio.service.reindexer.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.NoopIndexedStore;
import com.dremio.datastore.NoopKVStore;
import com.dremio.datastore.api.AbstractStoreBuilder;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.utility.StoreLoader;
import com.dremio.service.reindexer.proto.CheckpointInfo;
import com.dremio.service.reindexer.proto.ReindexVersionInfo;
import com.dremio.service.reindexer.proto.STATUS;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;

/**
 * Test for version store
 */
public class TestReindexVersionStore extends DremioTest {

  public static final String COLLECTION = "SampleCollection";
  public static final int VERSION = 1;
  public static final String DOC_ID = UUID.randomUUID().toString();
  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TestReindexVersionStore.class);
  private static final String VERSION_INFO_COLLECTION = "versioninfo";
  private ReindexVersionStore reindexVersionStore;
  private KVStoreProvider kvStoreProvider;

  @BeforeClass
  public static void init() {
  }

  @Before
  public void setup() throws Exception {
    LOGGER.info("setup");
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    reindexVersionStore = new ReindexVersionStoreImpl(() -> kvStoreProvider, TestReindexVersionStoreCreator.class);
    reindexVersionStore.start();
  }

  @After
  public void doCleanup() throws Exception {
    reindexVersionStore.close();
    LOGGER.info("cleanup");
  }

  private ReindexVersionInfo createReindexVersionInfo() {
    CheckpointInfo checkpointInfo = CheckpointInfo.newBuilder().setDocId(DOC_ID).build();
    ReindexVersionInfo reindexVersionInfo = ReindexVersionInfo.newBuilder()
      .setVersion(VERSION)
      .setCollectionName(COLLECTION)
      .setProgress(10)
      .setStatus(STATUS.INPROGRESS)
      .setDatetime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
      .setCheckpointInfo(Any.pack(checkpointInfo))
      .build();

    LOGGER.info("Creating version info {}", reindexVersionInfo.getVersion());
    reindexVersionStore.save(COLLECTION, reindexVersionInfo);
    return reindexVersionInfo;
  }

  @Test
  public void testGetReindexVersionInfo() {
    ReindexVersionInfo versionInfo = createReindexVersionInfo();
    ReindexVersionInfo versionInfo1 = reindexVersionStore.get(COLLECTION);
    assertNotNull(versionInfo1);
    assertNotNull(versionInfo1.getCheckpointInfo());
    assertEquals(versionInfo, versionInfo1);
    reindexVersionStore.delete(COLLECTION);
  }

  @Test
  public void testUpdateReindexVersionInfo() throws ReindexVersionStoreException {
    ReindexVersionInfo versionInfo = createReindexVersionInfo();
    reindexVersionStore.update(COLLECTION, (curVersion) -> {
      return curVersion.toBuilder().setProgress(100).setStatus(STATUS.COMPLETED).build();
    }, (state) -> {
      return true;
    });
    ReindexVersionInfo versionInfo1 = reindexVersionStore.get(COLLECTION);
    assertEquals(versionInfo1.getStatus(), STATUS.COMPLETED);
    assertEquals(versionInfo1.getProgress(), 100);
    assertNotEquals(versionInfo, versionInfo1);
    reindexVersionStore.delete(COLLECTION);
  }

  @Test
  public void testVersionInfoCollection() {
    ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>> stores = StoreLoader.buildStores(DremioTest.CLASSPATH_SCAN_RESULT, TestReindexVersionStore.this::newStore);
    assertTrue(stores.containsKey(TestReindexVersionStoreCreator.class));
    assertEquals(stores.get(TestReindexVersionStoreCreator.class).getName(), VERSION_INFO_COLLECTION);
  }

  private <K, V> KVStoreProvider.StoreBuilder<K, V> newStore() {
    return new PropStoreBuilder<>();
  }

  /**
   * Test Store creator implementation of {@link ReindexVersionStoreCreator}
   */
  public static class TestReindexVersionStoreCreator extends ReindexVersionStoreCreator {
    @Override
    public String name() {
      return VERSION_INFO_COLLECTION;
    }
  }

  /**
   * Store builder which builds noop indexedstores
   *
   * @param <K> key type K.
   * @param <V> value type V.
   */
  static class PropStoreBuilder<K, V> extends AbstractStoreBuilder<K, V> {
    @Override
    protected KVStore<K, V> doBuild() {
      return new NoopKVStore<>(getStoreBuilderHelper());
    }

    @Override
    protected IndexedStore<K, V> doBuildIndexed(DocumentConverter<K, V> documentConverter) {
      getStoreBuilderHelper().documentConverter(documentConverter);
      return new NoopIndexedStore<>(getStoreBuilderHelper());
    }
  }
}
