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

import static com.dremio.datastore.RocksDBStore.FILTER_SIZE_IN_BYTES;
import static com.dremio.datastore.RocksDBStore.META_MARKER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.dremio.datastore.RocksDBStore.RocksMetaManager;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.api.options.VersionOption;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Some robustness tests for {@code RocksDBStore} */
public class TestRocksDBStore {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestRocksDBStore.class);

  private static final long BLOB_FILTER_SIZE = 1024;

  private final class RocksDBResource extends ExternalResource {
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private String dbPath;
    private RocksDB db;

    @Override
    protected void before() throws Throwable {
      dbPath = temporaryFolder.newFolder().getPath();
      db = RocksDB.open(dbPath);
    }

    @Override
    public Statement apply(Statement base, Description description) {
      return temporaryFolder.apply(super.apply(base, description), description);
    }

    public RocksDB get() {
      return db;
    }

    public String getDbDir() {
      return dbPath;
    }

    @Override
    protected void after() {
      db.close();
    }
  }

  @Rule public final Timeout timeout = Timeout.seconds(90);

  @Rule public final RocksDBResource rocksDBResource = new RocksDBResource();

  private RocksDBStore store;

  private byte[] specialKey = "special".getBytes(UTF_8);

  @Before
  public void setUpStore() {
    ColumnFamilyHandle handle = rocksDBResource.get().getDefaultColumnFamily();
    final RocksMetaManager blobManager =
        new RocksMetaManager(rocksDBResource.dbPath, "test", BLOB_FILTER_SIZE);
    store =
        new RocksDBStore(
            "test",
            new ColumnFamilyDescriptor("test".getBytes(UTF_8)),
            handle,
            rocksDBResource.get(),
            4,
            blobManager);

    // Making sure test is repeatable
    Random random = new Random(42);
    for (int i = 0; i < 1 << 16; i++) {
      store.put(newRandomValue(random), newRandomValue(random));
    }
    store.put(specialKey, newRandomValue(random));
  }

  @After
  public void closeStore() throws IOException {
    store.close();
  }

  @Test()
  public void testFlush() throws IOException, RocksDBException {
    try {
      // Setup a new RocksDBStore with a new column family.
      String testColumnFamName = "testColumnFamName";
      ColumnFamilyDescriptor columnFamilyDescriptor =
          new ColumnFamilyDescriptor(testColumnFamName.getBytes(UTF_8));
      ColumnFamilyHandle handle = rocksDBResource.get().createColumnFamily(columnFamilyDescriptor);
      final RocksMetaManager blobManager =
          new RocksMetaManager(rocksDBResource.getDbDir(), testColumnFamName, FILTER_SIZE_IN_BYTES);
      RocksDBStore newStore =
          new RocksDBStore(
              testColumnFamName,
              columnFamilyDescriptor,
              handle,
              rocksDBResource.get(),
              4,
              blobManager);
      File rocksDbDir = new File(rocksDBResource.getDbDir());

      // Add KV pair to ensure newStore is working.
      byte[] testKey = "testKey".getBytes();
      byte[] testValue = "testValue".getBytes();
      newStore.put(testKey, testValue);
      assertArrayEquals(testValue, newStore.get(testKey).getValue());

      // Confirm no sst exists yet.
      for (int i = 0; i < rocksDbDir.listFiles().length; i++) {
        if (rocksDbDir.listFiles()[i].getName().endsWith(".sst")) {
          fail("SST file exists prior to shutdown - Prior flush has occurred.");
        }
      }

      // Close both RocksDBStores.
      store.close();
      newStore.close();
      try {
        store.get(specialKey);
        fail();
      } catch (DatastoreFatalException ignored) {
      }

      // Confirm that there is at least one sst file & that there is at most one log file of size 0.
      int sstCounter = 0;
      int logCounter = 0;
      File logFile = null;

      for (int i = 0; i < rocksDbDir.listFiles().length; i++) {
        if (rocksDbDir.listFiles()[i].getName().endsWith(".sst")) {
          ++sstCounter;
        }
        if (rocksDbDir.listFiles()[i].getName().endsWith(".log")) {
          ++logCounter;
          logFile = rocksDbDir.listFiles()[i].getAbsoluteFile();
        }
      }

      assertThat(sstCounter).isGreaterThanOrEqualTo(1);
      assertThat(logCounter).isLessThanOrEqualTo(1);
      assertEquals(0L, logFile.length());
    } finally {
      // Reset the RocksDBStore for other tests.
      ColumnFamilyHandle handle = rocksDBResource.get().getDefaultColumnFamily();
      final RocksMetaManager blobManager =
          new RocksMetaManager(rocksDBResource.getDbDir(), "test", FILTER_SIZE_IN_BYTES);
      store =
          new RocksDBStore(
              "test",
              new ColumnFamilyDescriptor("test".getBytes(UTF_8)),
              handle,
              rocksDBResource.get(),
              4,
              blobManager);
    }
  }

  @Test
  public void checkBlobOps() throws IOException {
    byte[] randomKey = new byte[5];
    byte[] randomValue1 = new byte[(int) BLOB_FILTER_SIZE + 1];
    byte[] randomValue2 = new byte[(int) BLOB_FILTER_SIZE + 1];
    Random r = new Random(123);
    r.nextBytes(randomKey);
    r.nextBytes(randomValue1);
    r.nextBytes(randomValue2);

    final Document<byte[], byte[]> document0 =
        store.put(
            randomKey, randomValue1, new ImmutableVersionOption.Builder().setTag("0").build());
    final String tag0 = document0.getTag();

    Assert.assertArrayEquals(randomValue1, store.get(randomKey).getValue());
    store
        .find()
        .forEach(
            e -> {
              if (Arrays.equals(randomKey, e.getKey())) {
                Assert.assertArrayEquals(randomValue1, e.getValue());
              }
            });

    String stats = store.getAdmin().getStats();
    assertThat(stats).contains("Estimated Blob Count: 1");
    assertThat(stats).contains("Estimated Blob Bytes: 1050");

    // fail the put and check we don't corrupt.
    store.validateAndPut(randomKey, randomValue2, new VersionOption.TagInfo(true, false, "999"));
    Assert.assertArrayEquals(randomValue1, store.get(randomKey).getValue());

    // actually put.
    final Document<byte[], byte[]> document1 =
        store.validateAndPut(randomKey, randomValue2, new VersionOption.TagInfo(true, false, tag0));
    final String tag1 = document1.getTag();

    Assert.assertArrayEquals(randomValue2, store.get(randomKey).getValue());

    // check a validated delete.
    assertEquals(
        true, store.validateAndDelete(randomKey, new VersionOption.TagInfo(true, false, tag1)));

    // reinsert the record and check a non-validated delete
    store.put(randomKey, randomValue1);
    store.delete(randomKey);

    // reinsert the record several times using put - should not result in any orphan blobs
    store.put(randomKey, randomValue1);
    store.put(randomKey, randomValue2);
    store.put(randomKey, randomValue1);
    store.delete(randomKey);

    Path blobDir = Paths.get(rocksDBResource.getDbDir(), "blob", "test");
    try (Stream<Path> stream = Files.list(blobDir)) {
      List<Path> remainingBlobFiles = stream.collect(Collectors.toList());
      assertEquals("Expected zero remaining files.", Collections.EMPTY_LIST, remainingBlobFiles);
    }

    // do empty gets and make sure things work correctly.
    assertEquals(null, store.get(randomKey));
  }

  @Test()
  public void testNotClosed() throws InterruptedException, ExecutionException {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    Future<?>[] futures = new Future[256];
    try {
      for (int i = 0; i < futures.length; i++) {
        futures[i] =
            executor.submit(
                new Callable<Integer>() {
                  @Override
                  public Integer call() {
                    int result = 0;
                    Iterable<Document<byte[], byte[]>> iterable = store.find();
                    for (@SuppressWarnings("unused") Document<byte[], byte[]> entry : iterable) {
                      // JVM might optimize aggressively no-op loop
                      result++;
                    }
                    return result;
                  }
                });
      }

      // Join on the calls
      executor.shutdown();
      boolean terminated = executor.awaitTermination(60, TimeUnit.SECONDS);
      assertThat(terminated).as("All the tasks didn't complete in time").isTrue();
      for (Future<?> future : futures) {
        future.get(); // Checking that the execution didn't fail
      }

      // Try to force gc by pressuring memory. There should be no reference left to rocksdb
      // iterators
      long[] dummy = new long[0];
      try {
        for (int j = 0; j < 10; j++) {
          for (int i = 1; i < 28; i++) {
            dummy = new long[1 << i];
          }
        }
      } catch (OutOfMemoryError e) {
        // ignore
      }
      assertNotNull(dummy);
      System.gc();
      System.gc();
      store.cleanReferences();
      assertThat(store.openedIterators()).isLessThanOrEqualTo(256L);
      assertThat(store.closedIterators()).isLessThanOrEqualTo(256L);
      assertThat(store.gcIterators()).isLessThanOrEqualTo(256L);
      assertThat(store.currentlyOpenIterators()).isGreaterThanOrEqualTo(0);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test()
  public void testClosed() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    Future<?>[] futures = new Future[256];

    try {
      for (int i = 0; i < futures.length; i++) {
        futures[i] =
            executor.submit(
                new Callable<Integer>() {
                  @Override
                  public Integer call() {
                    int result = 0;
                    Iterable<Document<byte[], byte[]>> iterable = store.find();
                    for (@SuppressWarnings("unused") Document<byte[], byte[]> entry : iterable) {
                      // JVM might optimize aggressively no-op loop
                      result++;
                    }
                    return result;
                  }
                });
      }

      // Join on the calls
      executor.shutdown();
      boolean terminated = executor.awaitTermination(60, TimeUnit.SECONDS);
      store.close();
      assertThat(terminated).as("All the tasks didn't complete in time").isTrue();
      for (Future<?> future : futures) {
        future.get(); // Checking that the execution didn't fail
      }

      assertThat(store.openedIterators()).isEqualTo(256L);
      assertThat(store.closedIterators()).isEqualTo(256L);
      logger.info("GCed iterators: " + store.gcIterators());
      assertThat(store.currentlyOpenIterators()).isEqualTo(0);
    } finally {
      executor.shutdownNow();
    }
  }

  private static final byte[] newRandomValue(Random r) {
    int size = r.nextInt(Byte.MAX_VALUE);
    byte[] res = new byte[size];

    r.nextBytes(res);

    if (size > 0) {
      // ensure that the random value doesn't contain our blob prefix
      res[0] = META_MARKER + 1;
    }
    return res;
  }
}
