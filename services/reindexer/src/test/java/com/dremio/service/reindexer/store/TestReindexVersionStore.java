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

import java.time.Instant;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.reindexer.proto.CheckpointInfo;
import com.dremio.service.reindexer.proto.ReindexVersionInfo;
import com.dremio.service.reindexer.proto.STATUS;
import com.dremio.test.DremioTest;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;

/**
 * Test for version store
 */
public class TestReindexVersionStore extends DremioTest {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TestReindexVersionStore.class.getName());
  public static final String COLLECTION = "version_info";
  public static final int VERSION = 1;
  public static final String DOC_ID = UUID.randomUUID().toString();

  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ReindexVersionStore reindexVersionStore;
  private KVStoreProvider kvStoreProvider;

  @BeforeClass
  public static void init() {
  }

  @Before
  public void setup() throws Exception {
    LOGGER.info("setup");
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    reindexVersionStore = new ReindexVersionStoreImpl(() -> kvStoreProvider, COLLECTION);
    kvStoreProvider.start();
    reindexVersionStore.start();
  }

  @After
  public void doCleanup() throws Exception{
    kvStoreProvider.close();
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
    ReindexVersionInfo versionInfo1 = reindexVersionStore.get(COLLECTION, VERSION);
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
    ReindexVersionInfo versionInfo1 = reindexVersionStore.get(COLLECTION, VERSION);
    assertEquals(versionInfo1.getStatus(), STATUS.COMPLETED);
    assertEquals(versionInfo1.getProgress(), 100);
    assertNotEquals(versionInfo, versionInfo1);
    reindexVersionStore.delete(COLLECTION);
  }
}
