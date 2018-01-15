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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
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

/**
 * Some robustness tests for {@code RocksDBStore}
 */
public class TestRocksDBStore {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestRocksDBStore.class);

  private final class RocksDBResource extends ExternalResource {
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private RocksDB db;

    @Override
    protected void before() throws Throwable {
      db = RocksDB.open(temporaryFolder.newFolder().getPath());
    }

    @Override
    public Statement apply(Statement base, Description description) {
      return temporaryFolder.apply(super.apply(base, description), description);
    }

    public RocksDB get() {
      return db;
    }

    @Override
    protected void after() {
      db.close();
    }
  }

  @Rule
  public final Timeout timeout = Timeout.seconds(60);

  @Rule
  public final RocksDBResource rocksDBResource = new RocksDBResource();

  private RocksDBStore store;

  @Before
  public void setUpStore() {
    ColumnFamilyHandle handle = rocksDBResource.get().getDefaultColumnFamily();
    store = new RocksDBStore("test", new ColumnFamilyDescriptor("test".getBytes(UTF_8)), handle, rocksDBResource.get(), 4);

    // Making sure test is repeatable
    Random random = new Random(42);
    for(int i = 0; i < 1 << 16; i++ ) {
      store.put(newRandomValue(random), newRandomValue(random));
    }
  }

  @After
  public void closeStore() throws IOException {
    store.close();
  }


  @Test()
  public void testNotClosed() throws InterruptedException, ExecutionException {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    Future<?>[] futures = new Future[256];
    try {
      for(int i = 0; i<futures.length; i++) {
        futures[i] = executor.submit(new Callable<Integer>() {
          @Override
          public Integer call() {
            int result = 0;
            Iterable<Entry<byte[], byte[]>> iterable = store.find();
            for(@SuppressWarnings("unused") Entry<byte[], byte[]> entry: iterable) {
              // JVM might optimize aggressively no-op loop
              result++;
            }
            return result;
          }
        });
      }

      // Join on the calls
      executor.shutdown();
      boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
      assertTrue("All the tasks didn't complete in time", terminated);
      for(Future<?> future: futures) {
        future.get(); // Checking that the execution didn't fail
      }

      // Try to force gc by pressuring memory. There should be no reference left to rocksdb iterators
      long[] dummy = new long[0];
      try {
        for(int i = 1; i<32; i++) {
          dummy = new long[1 << i];
        }
      } catch(OutOfMemoryError e) {
        // ignore
      }
      assertNotNull(dummy);
      System.gc(); System.gc();
      store.cleanReferences();
      assumeThat(store.openedIterators(), equalTo(256L));
      assumeThat(store.closedIterators(), equalTo(256L));
      assumeThat(store.gcIterators(), equalTo(256L));
      assumeThat(store.currentlyOpenIterators(), equalTo(0));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test()
  public void testClosed() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    Future<?>[] futures = new Future[256];

    try {
      for(int i = 0; i<futures.length; i++) {
        futures[i] = executor.submit(new Callable<Integer>() {
          @Override
          public Integer call() {
            int result = 0;
            Iterable<Entry<byte[], byte[]>> iterable = store.find();
            for(@SuppressWarnings("unused") Entry<byte[], byte[]> entry: iterable) {
              // JVM might optimize aggressively no-op loop
              result++;
            }
            return result;
          }
        });
      }

      // Join on the calls
      executor.shutdown();
      boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
      store.close();
      assertTrue("All the tasks didn't complete in time", terminated);
      for(Future<?> future: futures) {
        future.get(); // Checking that the execution didn't fail
      }

      assertThat(store.openedIterators(), equalTo(256L));
      assertThat(store.closedIterators(), equalTo(256L));
      logger.info("GCed iterators: " + store.gcIterators());
      assertThat(store.currentlyOpenIterators(), equalTo(0));
    } finally {
      executor.shutdownNow();
    }
  }

  private static final byte[] newRandomValue(Random r) {
    int size = r.nextInt(Byte.MAX_VALUE);
    byte[] res = new byte[size];

    r.nextBytes(res);

    return res;
  }

}
