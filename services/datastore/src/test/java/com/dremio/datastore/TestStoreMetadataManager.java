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

import static org.junit.Assert.assertEquals;

import java.util.function.Predicate;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test store metadata manager. */
public class TestStoreMetadataManager {

  private static final Predicate<String> TRUE = s -> true;

  private static final Predicate<String> FALSE = s -> false;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void regularFlow() throws Exception {
    final String storeName = "test-store";
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      ByteStoreManager.StoreMetadataManagerImpl smm =
          (ByteStoreManager.StoreMetadataManagerImpl) bsm.getMetadataManager();
      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 1);
      assertEquals(1, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 2);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(3, (long) smm.getFromCache(storeName));
      assertEquals(2, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(4, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber(TRUE));

      assertEquals(-1, smm.getLowestTransactionNumber(FALSE));
    }
  }

  @Test
  public void pauseUpdates() throws Exception {
    final String storeName = "test-store";
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      ByteStoreManager.StoreMetadataManagerImpl smm =
          (ByteStoreManager.StoreMetadataManagerImpl) bsm.getMetadataManager();

      smm.setLatestTransactionNumber(storeName, 1);
      assertEquals(null, smm.getFromCache(storeName));
      assertEquals(-1L, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 2);
      assertEquals(null, smm.getFromCache(storeName));
      assertEquals(-1L, smm.getLowestTransactionNumber(TRUE));

      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(3, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(4, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber(TRUE));

      assertEquals(-1, smm.getLowestTransactionNumber(FALSE));
    }
  }

  @Test
  public void blockAndAllowUpdates() throws Exception {
    final String storeName = "test-store";
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      ByteStoreManager.StoreMetadataManagerImpl smm =
          (ByteStoreManager.StoreMetadataManagerImpl) bsm.getMetadataManager();
      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 1);
      assertEquals(1, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 2);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber(TRUE));

      smm.blockUpdates();

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(2, (long) smm.getFromCache(storeName));
      assertEquals(1, smm.getLowestTransactionNumber(TRUE));

      smm.allowUpdates();

      smm.setLatestTransactionNumber(storeName, 3);
      assertEquals(3, (long) smm.getFromCache(storeName));
      assertEquals(2, smm.getLowestTransactionNumber(TRUE));

      smm.setLatestTransactionNumber(storeName, 4);
      assertEquals(4, (long) smm.getFromCache(storeName));
      assertEquals(3, smm.getLowestTransactionNumber(TRUE));

      assertEquals(-1, smm.getLowestTransactionNumber(FALSE));
    }
  }
}
