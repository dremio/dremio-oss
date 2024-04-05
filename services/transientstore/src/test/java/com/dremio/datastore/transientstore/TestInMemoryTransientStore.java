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
package com.dremio.datastore.transientstore;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.datastore.format.Format;
import com.google.common.base.Strings;
import com.google.common.base.Ticker;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.Test;

/** Tests of InMemoryTransientStore. */
public class TestInMemoryTransientStore {
  /** Used to provide an alternative time source for Cache. */
  static class FakeTicker extends Ticker {
    private final AtomicLong nanos = new AtomicLong();

    /** Advances the ticker value by {@code time} in {@code timeUnit}. */
    public void advance(long time, TimeUnit timeUnit) {
      nanos.addAndGet(timeUnit.toNanos(time));
    }

    @Override
    public long read() {
      return nanos.get();
    }
  }

  // put() tests
  @Test
  public void testPutOneEntry() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    final Document<String, Integer> document = store.put("key", 1);
    assertTrue(store.contains("key"));
    assertEquals("key", document.getKey());
    assertEquals(Integer.valueOf(1), document.getValue());
    assertFalse(Strings.isNullOrEmpty(document.getTag()));
  }

  @Test
  public void testBasicCreate() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1, KVStore.PutOption.CREATE);
    assertTrue(store.contains("key"));
  }

  @Test
  public void testBasicCreateAndUpdate() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1, KVStore.PutOption.CREATE);
    Document<String, Integer> document = store.get("key");
    assertEquals(Integer.valueOf(1), document.getValue());
    store.put("key", 2, VersionOption.from(document));
    document = store.get("key");
    assertEquals(Integer.valueOf(2), document.getValue());
  }

  @Test
  public void testCreateAndMultipleUpdate() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1, KVStore.PutOption.CREATE);
    Document<String, Integer> document = store.get("key");
    assertEquals(Integer.valueOf(1), document.getValue());
    for (int i = 2; i < 100; i++) {
      store.put("key", i, VersionOption.from(document));
      document = store.get("key");
      assertEquals(Integer.valueOf(i), document.getValue());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDifferentTTLOption() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    // test put with different TTL value is rejected.
    store.put("key", 1, KVStore.PutOption.CREATE, VersionOption.getTTLOption(600));
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testCreateExistingKey() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1, KVStore.PutOption.CREATE);
    assertTrue(store.contains("key"));
    // create same entry again
    store.put("key", 1, KVStore.PutOption.CREATE);
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testUpdateNonExistingKey() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1, new ImmutableVersionOption.Builder().setTag("foo").build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOverrideFixedTTLOption() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    // test put with TTL value is rejected.
    store.put("key", 1, KVStore.PutOption.CREATE, VersionOption.getTTLOption(60));
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testUpdateExistingKeyTagMismatch() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1, KVStore.PutOption.CREATE);
    assertTrue(store.contains("key"));
    // create same entry again
    store.put("key", 1, new ImmutableVersionOption.Builder().setTag("foo").build());
  }

  // contains() tests
  @Test
  public void testKeyNotPresent() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    assertFalse(store.contains("key"));
  }

  // get() single key tests
  @Test
  public void testGetOneEntry() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1);
    final Document<String, Integer> document = store.get("key");
    assertEquals("key", document.getKey());
    assertEquals(Integer.valueOf(1), document.getValue());
  }

  // get(list) tests
  @Test
  public void testGetMultipleEntries() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    final Map<String, Integer> reference =
        Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("key1", 1),
                new AbstractMap.SimpleImmutableEntry<>("key2", 2),
                new AbstractMap.SimpleImmutableEntry<>("key3", 3))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Load the store
    reference.forEach(store::put);

    // Retrieve a list of keys
    final List<String> keyList = new ArrayList<>(reference.keySet());
    final Iterable<Document<String, Integer>> result = store.get(keyList);

    // Compare the results
    for (Document<String, Integer> entry : result) {
      assertTrue(reference.containsKey(entry.getKey()));
      assertEquals(reference.get(entry.getKey()), entry.getValue());
    }
    final long resultSize = StreamSupport.stream(result.spliterator(), false).count();
    assertEquals(reference.size(), resultSize);
  }

  // delete() tests
  @Test
  public void testDeleteOneEntry() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1);
    assertTrue(store.contains("key"));
    store.delete("key");
    assertFalse(store.contains("key"));
  }

  @Test
  public void testDeleteWithOption() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1);
    final Document<String, Integer> document = store.get("key");
    store.delete("key", VersionOption.from(document));
    assertFalse(store.contains("key"));
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testDeleteWithBadVersion() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1);
    assertTrue(store.contains("key"));
    store.get("key");
    store.delete("key", new ImmutableVersionOption.Builder().setTag("foo").build());
    assertTrue(store.contains("key"));
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testDeleteKeyAlreadyDeleted() {
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(60);
    store.put("key", 1);
    assertTrue(store.contains("key"));
    final Document<String, Integer> document = store.get("key");
    store.delete("key", VersionOption.from(document));
    assertFalse(store.contains("key"));
    store.delete("key", VersionOption.from(document));
  }

  // TTL tests
  @Test
  public void testEntryExpiresAfterTTL() {
    final FakeTicker ticker = new FakeTicker();
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(5, ticker);
    store.put("key", 1);
    assertTrue(store.contains("key"));
    ticker.advance(6, TimeUnit.SECONDS);
    assertFalse(store.contains("key"));
  }

  @Test
  public void testEntryExpiresAtTTLAfterAccess() {
    final FakeTicker ticker = new FakeTicker();
    final InMemoryTransientStore<String, Integer> store = new InMemoryTransientStore<>(5, ticker);

    store.put("key", 1);
    for (int i = 0; i < 7; i++) {
      ticker.advance(1, TimeUnit.SECONDS);
      assertNotNull(store.get("key"));
    }
    ticker.advance(6, TimeUnit.SECONDS);
    assertNull(store.get("key"));
  }

  // InMemoryTransientStoreProvider tests
  @Test
  public void testProvider() throws Exception {
    try (TransientStoreProvider storeProvider = new InMemoryTransientStoreProvider()) {
      storeProvider.start();
      final InMemoryTransientStore<String, byte[]> store =
          storeProvider.getStore(Format.ofString(), Format.ofBytes(), 1);
      final byte[] expected = new byte[] {(byte) 0xe0, (byte) 0x4f};
      store.put("key", expected);
      final Document<String, byte[]> actual = store.get("key");
      assertArrayEquals(expected, actual.getValue());
    }
  }
}
