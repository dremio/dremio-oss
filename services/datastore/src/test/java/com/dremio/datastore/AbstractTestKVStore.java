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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.datastore.KVStore.FindByRange;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Test kvstore + key value serde storage.
 */
public abstract class AbstractTestKVStore {

  @Before
  public void init() throws Exception {
    initProvider();
    backend = createBackEndForKVStore();
    kvStore = createKVStore(backend);
  }

  abstract void initProvider() throws Exception;
  abstract void closeProvider() throws Exception;
  abstract Backend createBackEndForKVStore();
  abstract KVStore<String, String> createKVStore(Backend backend);

  @After
  public void tearDown() throws Exception {
    closeProvider();
  }

  /**
   * Methods implemented by backend.
   */
  public interface Backend {
    String get(String key);
    void put(String key, String value);
    String getName();
  }

  private KVStore<String, String> kvStore;

  private Backend backend;

  public void init(KVStore<String, String> kvStore, Backend backend) {
    this.kvStore = kvStore;
    this.backend = backend;
  }

  public KVStore<String, String> getKvStore() {
    return kvStore;
  }

  protected void setKvStore(KVStore<String, String> kvStore) {
    this.kvStore = kvStore;
  }

  public Backend getBackend() {
    return backend;
  }

  protected void setBackend(Backend backend) {
    this.backend = backend;
  }

  @Test
  public void testGet() throws Exception {
    backend.put("key1", "value1");
    assertEquals("value1", kvStore.get("key1"));
  }

  @Test
  public void testEntries() {
   backend.put("random key", "random value");
   backend.put("another random key", "another random value");

    assertEquals(
        ImmutableMap.of("random key", "random value", "another random key", "another random value"),
        KVUtil.asMap(kvStore.find()));
  }

  @Test
  public void testGetMissingKey() {
    backend.put("random key", "random value");
    assertNull(kvStore.get("another random key"));
  }

  @Test
  public void testGetMissingKeys() {
    backend.put("random key", "random value");
    assertEquals(Lists.newArrayList(null, null), kvStore.get(ImmutableList.of("another random key", "random key 2")));
  }

  @Test
  public void testEntriesGetSorted() throws Exception {
    backend.put("key1", "value1");
    backend.put("key3", "value3");
    backend.put("key2", "value2");
    final Iterable<Map.Entry<String, String>> entries = kvStore.find();
    assertSize(entries, 3);

    assertEquals(
        ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3"),
        KVUtil.asMap(kvStore.find()));
  }

  @Test
  public void testContainsMissingKey() {
    backend.put("random key", "random value");
    assertFalse(kvStore.contains("another random key"));
  }

  @Test
  public void testContains() {
    backend.put("random key", "random value");
    assertTrue("random value", kvStore.contains("random key"));
  }

  @Test
  public void testPut() throws Exception {
    kvStore.put("key1", "value1");
    assertEquals("value1", backend.get("key1"));
    kvStore.put("key1", "value11");
    assertEquals("value11", backend.get("key1"));
    kvStore.put("key1", "value1");
    assertEquals("value1", backend.get("key1"));
  }

  @Test
  public void testPutWithNullValue() throws Exception {
    try {
      kvStore.put("key1", null);
      fail("KVtore null value insertions should fail with NullPointerException");
    } catch (NullPointerException e) {}
  }

  @Test
  public void testPutAlreadyExists() {
    backend.put("random key", "random value");
    kvStore.put("random key", "another random value");
    assertEquals("another random value", backend.get("random key"));
  }

  @Test
  public void testDeleteNonExistentKey() {
    backend.put("random key", "random value");
    kvStore.delete("another random key");
    // Just making sure no exception is thrown and that the backend is untouched
    assertEquals("random value", backend.get("random key"));
  }

  @Test
  public void testDelete() {
    backend.put("random key", "random value");
    kvStore.delete("random key");
    assertNull(backend.get("random key"));
  }

  @Test
  public void testCheckAndPut() throws Exception {
    boolean updated = kvStore.checkAndPut("key1", null, "value1");
    assertTrue("Failed to put new value value1 for key key1 for the first time", updated);
    assertEquals("value1", backend.get("key1"));
    updated = kvStore.checkAndPut("key2", "value2", "value22");
    assertFalse("checkAndPut should fail to replace new value value22 with old value value2", updated);
    assertEquals("value1", backend.get("key1"));
    updated = kvStore.checkAndPut("key1", "value1", "value11");
    assertTrue("Failed to put new value value11 for key key1", updated);
    assertEquals("value11", backend.get("key1"));
  }

  @Test
  public void testCheckAndPutWithNullValue() throws Exception {
    try {
      kvStore.checkAndPut("key1", "value11", null);
      fail("KVtore checkAndPut should fail with NullPointerException for null new value");
    } catch (NullPointerException e) {}
  }

  @Test
  public void testGetRange() throws Exception {
    // only on kvStore
    getBackend().put("key1", "value1");
    getBackend().put("key3", "value3");
    getBackend().put("key2", "value2");
    getBackend().put("key30", "value30");
    getBackend().put("key0", "value0");
    getBackend().put("key100", "value100");

    Iterable<Entry<String, String>> range = kvStore.find(new FindByRange<>("key0", false, "key2", false));
    assertRange(range, 2, "key1", "key100");

    range = kvStore.find(new FindByRange<>("key0", true, "key1", false));
    assertSize(range, 1);
    assertRange(range, 1, "key0");

    range = kvStore.find(new FindByRange<>("key0", false, "key1", true));
    assertRange(range, 1, "key1");

    range = kvStore.find(new FindByRange<>("key0", false, "key40", false));
    assertRange(range, 5, "key1", "key100", "key2", "key3", "key30");
  }

  private static void assertRange(Iterable<Entry<String, String>> range, int size, String... keys){
    assertSize(range, size);
    assertIncluded(range, keys);
  }

  private static void assertIncluded(Iterable<Entry<String, String>> range, String... keys){
    for(final String key : keys){
      Optional<Entry<String, String>> out = Iterables.tryFind(range, new Predicate<Entry<String, String>>() {
        @Override
        public boolean apply(Entry<String, String> input) {
          return input.getKey().equals(key);
        }
      });
      assertTrue(String.format("Missing key expected: %s", key), out.isPresent());
    }
  }

  private static void assertSize(Iterable<Entry<String, String>> range, int size){
    assertEquals(size, Iterables.size(range));
  }

  @Test
  public void testEmptyRange() throws Exception {
    getBackend().put("key1", "value1");
    getBackend().put("key3", "value3");
    getBackend().put("key2", "value2");
    getBackend().put("key30", "value30");
    getBackend().put("key0", "value0");
    getBackend().put("key100", "value100");

    Iterable<Map.Entry<String, String>> range = kvStore.find(new FindByRange<>("key0", false, "key0", false));
    assertEquals(0, Iterables.size(range));

    range = kvStore.find(new FindByRange<>("key0", false, "key0", true));
    assertEquals(0, Iterables.size(range));
  }

  @Test
  public void testSortOrder() throws Exception {
    getBackend().put("key1", "value1");
    getBackend().put("key3", "value3");
    getBackend().put("key2", "value2");
    getBackend().put("key30", "value30");
    getBackend().put("key0", "value0");
    getBackend().put("key40", "value40");
    getBackend().put("key55", "value55");
    getBackend().put("key100", "value100");


    ImmutableMap<String, String> expected = ImmutableMap.of("key1", "value1", "key100", "value100");
    Iterable<Entry<String, String>> range = kvStore.find(new FindByRange<>("key0", false, "key100", true));
    assertRangeEquals(expected, range);

    expected = ImmutableMap.of("key100", "value100", "key2", "value2", "key3", "value3", "key30", "value30", "key40", "value40");
    range = kvStore.find(new FindByRange<>("key10", true, "key40", true));
    assertRangeEquals(expected, range);
  }

  public static void assertRangeEquals(ImmutableMap<String, String> expectedOrderedMap, Iterable<Entry<String, String>> actualIterable){
    Iterator<Entry<String, String>> iter = actualIterable.iterator();

    for(Entry<String, String> expected : expectedOrderedMap.entrySet()){
      assertTrue(iter.hasNext());
      Entry<String, String> actual = iter.next();
      assertEquals(actual, expected);
    }
  }
}
