/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test store metadata manager.
 */
public class TestStoreMetadataManager {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void regularFlow() throws Exception {
    final String storeName = "test-store";
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      StoreMetadataManager smm = new StoreMetadataManager(bsm);
      smm.start();
      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 1);
      assertEquals(1, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 2);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(3, (long) smm.getFromCache(storeName));
      assertEquals(2, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(4, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber());
    }
  }

  @Test
  public void pauseUpdates() throws Exception {
    final String storeName = "test-store";
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      StoreMetadataManager smm = new StoreMetadataManager(bsm);
      smm.start();

      smm.setLatestTransactionNumber(storeName, 1);
      assertEquals(null, smm.getFromCache(storeName));
      assertEquals(Long.MAX_VALUE, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 2);
      assertEquals(null, smm.getFromCache(storeName));
      assertEquals(Long.MAX_VALUE, smm.getLowestTransactionNumber());

      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(3, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(4, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber());
    }
  }

  @Test
  public void blockAndAllowUpdates() throws Exception {
    final String storeName = "test-store";
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      StoreMetadataManager smm = new StoreMetadataManager(bsm);
      smm.start();
      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 1);
      assertEquals(1, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 2);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber());

      smm.blockUpdates();

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber());

      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(3, (long) smm.getFromCache(storeName));
      assertEquals(2, smm.getLowestTransactionNumber());

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(4, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber());
    }
  }
}
