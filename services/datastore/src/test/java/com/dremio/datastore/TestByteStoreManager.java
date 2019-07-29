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

import static com.dremio.datastore.RocksDBStore.BLOB_FILTER_DEFAULT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

import com.google.common.collect.Lists;

/**
 * Tests for {@code ByteStoreManager}
 */
public class TestByteStoreManager {

  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

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

  private static byte[] getBytes(String string) {
    return string.getBytes(StandardCharsets.UTF_8);
  }

  private enum ReplayType {
    ADD,
    DELETE
  }

  @Test
  public void replaySince() throws Exception {
    String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      // We reuse a whitelist blob value to ensure that we get the blobbing behavior
      final String storeName = ByteStoreManager.BLOB_WHITELIST_STORE;
      ByteStore bs = bsm.getStore(storeName);
      final long txn = bsm.getMetadataManager()
          .getLatestTransactionNumber();

      final byte[] one = getBytes("one");
      final byte[] two = getBytes("two");
      bs.put(one, getBytes("1"));
      bs.put(one, getBytes("2"));
      bs.put(two, getBytes("3"));
      bs.put(two, getBytes("3"));
      bs.delete(one);

      // create a big value that gets blobbed
      final byte[] three = getBytes("three");
      final byte[] bigValue = new byte[(int) BLOB_FILTER_DEFAULT + 1];
      final Random r = new Random(123);
      r.nextBytes(bigValue);
      bs.put(three, bigValue);

      final List<ReplayType> updates = Lists.newArrayList();
      final List<Map.Entry<byte[], byte[]>> entries = Lists.newArrayList();
      bsm.replaySince(txn, new ReplayHandler() {
        @Override
        public void put(String tableName, byte[] key, byte[] value) {
          assertEquals(tableName, storeName);
          updates.add(ReplayType.ADD);
          entries.add(new AbstractMap.SimpleEntry<>(key, value));
        }

        @Override
        public void delete(String tableName, byte[] key) {
          assertEquals(tableName, storeName);
          updates.add(ReplayType.DELETE);
          entries.add(new AbstractMap.SimpleEntry<>(key, null));
        }
      });

      assertEquals(updates.size(), 6);
      assertEquals(entries.size(), 6);

      assertEquals(updates.get(0), ReplayType.ADD);
      assertArrayEquals(entries.get(0).getKey(), one);
      assertArrayEquals(entries.get(0).getValue(), getBytes("1"));

      assertEquals(updates.get(1), ReplayType.ADD);
      assertArrayEquals(entries.get(1).getKey(), one);
      assertArrayEquals(entries.get(1).getValue(), getBytes("2"));

      assertEquals(updates.get(2), ReplayType.ADD);
      assertArrayEquals(entries.get(2).getKey(), two);
      assertArrayEquals(entries.get(2).getValue(), getBytes("3"));

      assertEquals(updates.get(3), ReplayType.ADD);
      assertArrayEquals(entries.get(3).getKey(), two);
      assertArrayEquals(entries.get(3).getValue(), getBytes("3"));

      assertEquals(updates.get(4), ReplayType.DELETE);
      assertArrayEquals(entries.get(4).getKey(), one);
      assertEquals(entries.get(4).getValue(), null);

      assertEquals(updates.get(5), ReplayType.ADD);
      assertArrayEquals(entries.get(5).getKey(), three);
      // the big value should be returned and not the blob pointer
      assertArrayEquals(entries.get(5).getValue(), bigValue);
    }
  }

  @Test
  public void replayAnother() throws Exception {
    String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      final String storeName = "test-store";
      ByteStore bs = bsm.getStore(storeName);

      final byte[] one = getBytes("one");
      final byte[] two = getBytes("two");
      bs.put(one, getBytes("1"));
      bs.put(one, getBytes("2"));
      bs.put(two, getBytes("3")); // this will be replayed as well!

      final long txn = bsm.getMetadataManager()
          .getLatestTransactionNumber();
      bs.put(two, getBytes("3"));
      bs.delete(one);

      final List<ReplayType> updates = Lists.newArrayList();
      final List<Map.Entry<byte[], byte[]>> entries = Lists.newArrayList();
      bsm.replaySince(txn, new ReplayHandler() {
        @Override
        public void put(String tableName, byte[] key, byte[] value) {
          assertEquals(tableName, storeName);
          updates.add(ReplayType.ADD);
          entries.add(new AbstractMap.SimpleEntry<>(key, value));
        }

        @Override
        public void delete(String tableName, byte[] key) {
          assertEquals(tableName, storeName);
          updates.add(ReplayType.DELETE);
          entries.add(new AbstractMap.SimpleEntry<>(key, null));
        }
      });

      assertEquals(updates.size(), 3);
      assertEquals(entries.size(), 3);

      assertEquals(updates.get(0), ReplayType.ADD);
      assertArrayEquals(entries.get(0).getKey(), two);
      assertArrayEquals(entries.get(0).getValue(), getBytes("3"));

      assertEquals(updates.get(1), ReplayType.ADD);
      assertArrayEquals(entries.get(1).getKey(), two);
      assertArrayEquals(entries.get(1).getValue(), getBytes("3"));

      assertEquals(updates.get(2), ReplayType.DELETE);
      assertArrayEquals(entries.get(2).getKey(), one);
      assertEquals(entries.get(2).getValue(), null);
    }
  }

  @Test
  @Ignore // the exception is not propagated
  public void closeNicelyOnThrow() throws Exception {
    thrownException.expect(RuntimeException.class);

    String dbPath = temporaryFolder.newFolder().getAbsolutePath();
    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();

      final String storeName = "test-store";
      ByteStore bs = bsm.getStore(storeName);
      final long txn = bsm.getMetadataManager()
          .getLatestTransactionNumber();

      bs.put(getBytes("one"), getBytes("1"));

      bsm.replaySince(txn, new ReplayHandler() {
        @Override
        public void put(String tableName, byte[] key, byte[] value) {
          throw new RuntimeException("troll");
        }

        @Override
        public void delete(String tableName, byte[] key) {
        }
      });
    }
  }

  @Test
  public void testNoDBOpenRetry() throws Exception {
    String dbPath = temporaryFolder.newFolder().getAbsolutePath();

    try (ByteStoreManager bsm = new ByteStoreManager(dbPath, false)) {
      bsm.start();
      ByteStoreManager bsm2 = new ByteStoreManager(dbPath, false, true);
      bsm2.start();

      fail("ByteStoreManager shouldn't have been able to open a locked instance");
    } catch (RocksDBException e) {
      assertTrue("RocksDBException isn't IOError type", Status.Code.IOError.equals(e.getStatus().getCode()));
      assertTrue("Incorrect error message", e.getStatus().getState().contains("While lock"));
    }
  }
}
