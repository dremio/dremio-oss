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
package com.dremio.exec.store.cache;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Test for {@link RocksDbBroker}
 */
public class RocksDbBrokerTest {
  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  private static final byte[] KEY = "hello".getBytes();
  private static final byte[] VALUE = "world".getBytes();

  @Test
  public void testSingleInstance() throws Exception {
    String dbPath = tempDir.newFolder().getPath();
    RocksDbBroker broker = new RocksDbBroker(dbPath);
    RocksDB db = broker.getDb();
    assertNotNull(db);

    assertThatThrownBy(() -> new RocksDbBroker(dbPath))
      .isInstanceOf(RocksDBException.class)
      .hasMessageContaining("No locks available");
  }

  @Test
  public void testReloadRocksDBAfterHostRestart() throws Exception {
    String dbPath = tempDir.newFolder().getPath();
    RocksDbBroker broker = new RocksDbBroker(dbPath);
    RocksDB db = broker.getDb();
    assertNotNull(db);

    db.close(); // simulates host shutdown
    RocksDbBroker broker2 = new RocksDbBroker(dbPath);
    assertNotNull(broker2.getDb());
  }

  @Test
  public void testColumnFamilyValidation_missingExpectedColumnFamilies() {
    assertThatThrownBy(() -> RocksDbBroker.validateColumnFamilies(Collections.emptyList()))
      .isInstanceOf(RocksDBException.class)
      .hasMessageContaining("Did not load the expected number of column families");
  }

  @Test
  public void testColumnFamilyValidation_wrongColumnFamily() {
    List<byte[]> columnFamilyHandles = Arrays.asList(RocksDB.DEFAULT_COLUMN_FAMILY, "blah-cf".getBytes());
    assertThatThrownBy(() -> RocksDbBroker.validateColumnFamilies(columnFamilyHandles))
      .isInstanceOf(RocksDBException.class)
      .hasMessageContaining("Unexpected column family");
  }

  @Test
  public void testGetReturnsNullForAbsentKey() throws Exception {
    RocksDbBroker broker = new RocksDbBroker(tempDir.newFolder().getPath());
    byte[] value = broker.get("hello".getBytes());
    assertNull(value);
  }

  @Test
  public void testGetReturnsWhatPutAdded() throws Exception {
    RocksDbBroker broker = new RocksDbBroker(tempDir.newFolder().getPath());
    broker.put(KEY, VALUE);
    assertEquals("world", new String(broker.get(KEY)));
  }

  @Test
  public void testMultipleGetsReturnTheSameValue() throws Exception {
    RocksDbBroker broker = new RocksDbBroker(tempDir.newFolder().getPath());
    broker.put(KEY, VALUE);
    assertArrayEquals(VALUE, broker.get(KEY));
    assertArrayEquals(VALUE, broker.get(KEY));
  }

  @Test
  public void testStateChangeOnReads() throws Exception {
    RocksDbBroker broker = new RocksDbBroker(tempDir.newFolder().getPath());
    assertEquals(RocksDbBroker.RocksDBState.OK, broker.getReadState());

    RocksDB db = spy(broker.getDb());
    doThrow(new RocksDBException("Couldn't get the value for key")).when(db).get(broker.getColumnFamily(), KEY);
    broker.setDb(db);

    broker.get(KEY);
    assertEquals(RocksDbBroker.RocksDBState.ERROR, broker.getReadState());

    reset(db);
    broker.get(KEY);
    assertEquals(RocksDbBroker.RocksDBState.OK, broker.getReadState());
  }

  @Test
  public void testStateChangeOnWrites() throws Exception {
    RocksDbBroker broker = new RocksDbBroker(tempDir.newFolder().getPath());
    assertEquals(RocksDbBroker.RocksDBState.OK, broker.getWriteState());

    RocksDB db = spy(broker.getDb());
    doThrow(new RocksDBException("Couldn't add the key-value mapping")).when(db).put(broker.getColumnFamily(), KEY, VALUE);
    broker.setDb(db);

    broker.put(KEY, VALUE);
    assertEquals(RocksDbBroker.RocksDBState.ERROR, broker.getWriteState());

    reset(db);
    broker.put(KEY, VALUE);
    assertEquals(RocksDbBroker.RocksDBState.OK, broker.getWriteState());
  }
}
