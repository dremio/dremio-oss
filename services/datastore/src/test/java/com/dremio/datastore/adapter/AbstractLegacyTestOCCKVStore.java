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
package com.dremio.datastore.adapter;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.adapter.extractors.ProtostuffDummyObjVersionExtractor;
import com.dremio.datastore.adapter.stores.LegacyProtostuffOCCStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.generator.DataGenerator;
import com.dremio.datastore.generator.ProtostuffStoreGenerator;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

/** Tests OCCKVStore */
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public abstract class AbstractLegacyTestOCCKVStore<K, V> {

  private LegacyKVStore<K, V> kvStore;
  private LegacyKVStoreProvider provider;

  @Parameter public Class<TestLegacyStoreCreationFunction<K, V>> storeCreationFunction;

  @Parameter(1)
  public DataGenerator<K, V> gen;

  @Parameter(2)
  public VersionExtractor<V> versionExtractor;

  @Parameterized.Parameters(name = "Table: {0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(
        new Object[][] {
          {
            LegacyProtostuffOCCStore.class,
            new ProtostuffStoreGenerator(),
            new ProtostuffDummyObjVersionExtractor()
          }
        });
  }

  protected abstract LegacyKVStoreProvider createProvider() throws Exception;

  @Before
  public void init() throws Exception {
    provider = createProvider();
    provider.start();
    kvStore = provider.getStore(storeCreationFunction);
  }

  @After
  public void after() throws Exception {
    provider.close();
  }

  @Test
  public void testMissingPrevious() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    kvStore.put(key, value);

    // Attempt to extend a version that doesn't exist
    final K newKey = gen.newKey();
    assertThatThrownBy(() -> kvStore.put(newKey, value))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void testCreate() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    kvStore.put(key, value);
  }

  @Test
  public void testUpdate() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    // First update
    kvStore.put(key, value);
    final String tag1 = versionExtractor.getTag(value);
    assertFalse(Strings.isNullOrEmpty(tag1));

    // Second update.
    kvStore.put(key, value);
    final String tag2 = versionExtractor.getTag(value);
    assertFalse(Strings.isNullOrEmpty(tag2));
    assertNotEquals(tag1, tag2);

    // Third update.
    kvStore.put(key, value);
    final String tag3 = versionExtractor.getTag(value);
    assertFalse(Strings.isNullOrEmpty(tag3));
    assertNotEquals(tag1, tag3);
    assertNotEquals(tag2, tag3);
  }

  @Test
  public void testPrecommit() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    kvStore.put(key, value);
    final String tag = versionExtractor.getTag(value);
    assertFalse(Strings.isNullOrEmpty(tag));
    V updatedValue = kvStore.get(key);
    // Make sure the in-memory value and kvstore value match.
    assertEquals(value, updatedValue);
    assertEquals(tag, versionExtractor.getTag(updatedValue));
  }

  @Test
  public void testConcurrentUpdate() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    // Initial put.
    kvStore.put(key, value);
    final String initialTag = versionExtractor.getTag(value);

    // First update.
    kvStore.put(key, value);
    final String firstUpdateTag = versionExtractor.getTag(value);
    assertFalse(Strings.isNullOrEmpty(firstUpdateTag));
    assertNotEquals(initialTag, firstUpdateTag);

    // Attempt to update with value containing stale tag.
    boolean threw = false;
    try {
      versionExtractor.setTag(value, initialTag);
      kvStore.put(key, value);
    } catch (ConcurrentModificationException ex) {
      threw = true;
    }
    assertTrue(threw);

    // Ensure that value doesn't get mutated after a failed update attempt.
    assertEquals(firstUpdateTag, versionExtractor.getTag(kvStore.get(key)));
  }

  @Test
  public void testDelete() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    kvStore.put(key, value);
    kvStore.delete(key, versionExtractor.getTag(value));
    assertFalse(kvStore.contains(key));
  }

  @Test
  public void testDeleteBadVersion() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    // Initial put.
    kvStore.put(key, value);
    final String initialTag = versionExtractor.getTag(value);
    assertFalse(Strings.isNullOrEmpty(initialTag));

    // Update value.
    kvStore.put(key, value);
    final String updateTag = versionExtractor.getTag(value);
    assertFalse(Strings.isNullOrEmpty(updateTag));
    assertNotEquals(initialTag, updateTag);

    // Attempt to delete with previous tag.
    assertThatThrownBy(() -> kvStore.delete(key, initialTag))
        .isInstanceOf(ConcurrentModificationException.class);
  }
}
