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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

/**
 * Tests for {@code ByteStoreManager}
 */
public class TestByteStoreManager {

  @Rule
  public final TestRule timeout = new DisableOnDebug(Timeout.seconds(60));

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Test that BSM can open already created databases.
   *
   * A bug in BSM was causing the default column family to be open twice,
   * but recent versions of RocksDB have been less lenient with this and
   * threw assertion errors during close.
   *
   * @throws Exception
   */
  @Test
  public void testMultipleOpen() throws Exception {
    String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    // Empty directory
    try(ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();
    }

    // Re opening just created database
    try(ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();
    }
  }

  private static class TestThread extends Thread {
    private final CountDownLatch ready = new CountDownLatch(1);
    private final CountDownLatch started = new CountDownLatch(1);

    private final AtomicBoolean result = new AtomicBoolean(false);

    private final ByteStoreManager bsm;

    public TestThread(ByteStoreManager bsm) {
      super("TestByteStoreManager-thread");
      this.bsm = bsm;
    }

    @Override
    public void run() {
      try {
        ready.await();
        try {
          bsm.start();
          result.set(true);
        } finally {
          started.countDown();
        }
      } catch(Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }
  /**
   * Test that {@code ByteStoreManager#start()} waits until RocksDB lock
   * is released
   */
  @Test
  public void testConcurrentOpenSleep() throws Exception {
    String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try(ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      TestThread tt = new TestThread(bsm);

      try(RocksDB db = RocksDB.open(new File(dbPath, ByteStoreManager.CATALOG_STORE_NAME).getAbsolutePath());
          ColumnFamilyHandle handle = db.getDefaultColumnFamily()) {
        tt.start();

        tt.ready.countDown();
        // Wait for multiple attempts
        TimeUnit.MILLISECONDS.sleep(300);

        // Lock should still be in place
        assertEquals(1, tt.started.getCount());
        assertFalse("RocksDB lock didn't work", tt.result.get());
      }

      // RocksDB is now closed, lock should be freed
      tt.started.await();
      assertTrue("RocksDB lock not released properly", tt.result.get());
    }
  }
}
