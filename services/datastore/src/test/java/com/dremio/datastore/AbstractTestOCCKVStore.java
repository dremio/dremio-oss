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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;
import com.dremio.datastore.generator.DataGenerator;
import com.dremio.datastore.generator.ProtobufStoreGenerator;
import com.dremio.datastore.generator.ProtostuffStoreGenerator;
import com.dremio.datastore.generator.RawByteStoreGenerator;
import com.dremio.datastore.generator.StringStoreGenerator;
import com.dremio.datastore.generator.UUIDStoreGenerator;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;
import com.dremio.datastore.proto.Dummy;
import com.dremio.datastore.proto.DummyId;
import com.dremio.datastore.stores.ByteContainerStore;
import com.dremio.datastore.stores.ProtobufStore;
import com.dremio.datastore.stores.ProtostuffStore;
import com.dremio.datastore.stores.RawByteStore;
import com.dremio.datastore.stores.StringStore;
import com.dremio.datastore.stores.UUIDStore;
import com.google.common.base.Strings;

/**
 * Tests OCCKVStore
 */
@RunWith(Parameterized.class)
public abstract class AbstractTestOCCKVStore<K, V> {
  private static final String TAG_ASSERT_FAILURE_MSG = "All documents should have a non-null, non-empty tag";

  private KVStore<K, V> kvStore;
  private KVStoreProvider provider;

  // CHECKSTYLE:OFF VisibilityModifier
  @Parameterized.Parameter
  public Class<StoreCreationFunction<KVStore<K, V>>> storeCreationFunction;

  @Parameterized.Parameter(1)
  public DataGenerator<K, V> gen;

  // Used to determine if find is supported.
  @Parameterized.Parameter(2)
  public Class<K> keyClass;

  // CHECKSTYLE:ON VisibilityModifier
  @Parameterized.Parameters(name = "Table: {0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {UUIDStore.class, new UUIDStoreGenerator(), UUID.class},
      {ProtobufStore.class, new ProtobufStoreGenerator(), Dummy.DummyId.class},
      {ProtostuffStore.class, new ProtostuffStoreGenerator(), DummyId.class},
      //Variable-length
      {StringStore.class, new StringStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH), String.class},
      {RawByteStore.class, new RawByteStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH), String.class},
      {ByteContainerStore.class, new ByteContainerStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH),
        ByteContainerStoreGenerator.ByteContainer.class},
      //Fixed-length
      {StringStore.class, new StringStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH), String.class},
      {RawByteStore.class, new RawByteStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH), String.class},
      {ByteContainerStore.class, new ByteContainerStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH),
        ByteContainerStoreGenerator.ByteContainer.class}
    });
  }

  protected abstract KVStoreProvider createKVStoreProvider() throws Exception;

  @SuppressWarnings("resource")
  @Before
  public void before() throws Exception {
    provider = createKVStoreProvider();
    kvStore = provider.getStore(storeCreationFunction);
    gen.reset();
  }

  @After
  public void after() throws Exception {
    provider.close();
  }

  @Test(expected = NullPointerException.class)
  public void testShouldNotPutNulls() {
    final K key = gen.newKey();
    kvStore.put(key, null);
  }

  @Test
  public void testPutWithoutCreateOptionIsValid() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);
  }

  @Test
  public void testPutWithCreateOption() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(key, value, doc);
  }

  @Test
  public void testPutWithCreateOptionFailsToOverwrite() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    try {
      final V updateValue = gen.newVal();
      kvStore.put(key, updateValue, KVStore.PutOption.CREATE);
      fail("Should not be able to update with a CREATE option.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(doc, docAfterFailedPut);
    }
  }

  @Test
  public void testModificationWithoutOptionsAlwaysOverwrites() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key,updateValue);
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);
  }

  @Test
  public void testPutAfterDeleteWithoutOptions() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    kvStore.delete(key);
    final Document<K, V> nullDoc = kvStore.get(key);
    assertNull(nullDoc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key,updateValue);
    assertDocumentEquals(key, updateValue, updatedDoc);
  }

  @Test
  public void testPutAlwaysUsingOptionsShouldUpdate() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    final V updateValue2 = gen.newVal();
    final Document<K, V> updatedDoc2 = kvStore.put(key, updateValue2, VersionOption.from(updatedDoc));
    assertDocumentUpdated(key, updateValue2, updatedDoc.getTag(), updatedDoc2);
  }

  @Test
  public void testPutLateOptionAdoptionShouldStillUpdate() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    final V updateValue2 = gen.newVal();
    final Document<K, V> updatedDoc2 = kvStore.put(key, updateValue2, VersionOption.from(updatedDoc));
    assertDocumentUpdated(key, updateValue2, updatedDoc.getTag(), updatedDoc2);
  }

  @Test
  public void testPutLateOptionAdoptionShouldStillProtectFromCM() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    try{
      final V updateValue2 = gen.newVal();
      kvStore.put(key, updateValue2, VersionOption.from(doc));
      fail("Should not be able to update with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(updatedDoc, docAfterFailedPut);
    }
  }

  @Test
  public void testCreateWithManyOptionsFails() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    try {
      final V updateValue = gen.newVal();
      kvStore.put(key, updateValue, KVStore.PutOption.CREATE, VersionOption.from(doc));
      fail("If multiple VersionOption(s) are passed, the put should not be performed.");
    } catch (IllegalArgumentException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(doc, docAfterFailedPut);
    }
  }

  @Test
  public void testPutWithMultipleVersionsFails() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K ,V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    try {
      final V updateValue2 = gen.newVal();
      kvStore.put(key, updateValue2, VersionOption.from(doc), VersionOption.from(updatedDoc));
      fail("If multiple VersionOption(s) are passed, the put should not be performed.");
    } catch (IllegalArgumentException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(updatedDoc, docAfterFailedPut);
    }
  }

  @Test
  public void testPutWithOutdatedVersionFails() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> originalDoc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(key, value, originalDoc);

    final V updateValue = gen.newVal();
    final Document<K, V> lastGoodDoc = kvStore.put(key, updateValue, VersionOption.from(originalDoc));
    assertDocumentUpdated(key, updateValue, originalDoc.getTag(), lastGoodDoc);

    try {
      final V updateValue2 = gen.newVal();
      kvStore.put(key, updateValue2, VersionOption.from(originalDoc));
      fail("Should not be able to update with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(lastGoodDoc, docAfterFailedPut);
    }
  }

  @Test
  public void testDeleteWithoutTagAlwaysDeletes() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    kvStore.delete(key);
    assertFalse(kvStore.contains(key));
  }

  @Test
  public void testDeleteWithOutdatedVersionFails() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    try {
      kvStore.delete(key, VersionOption.from(doc));
      fail("Should not be able to delete a document with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedDelete = kvStore.get(key);
      assertDocumentEquals(updatedDoc, docAfterFailedDelete);
    }
  }

  @Test
  public void testDeleteWithLateOptionShouldStillProtectFromCM() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue);
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    try {
      kvStore.delete(key, VersionOption.from(doc));
      fail("Should not be able to delete a document with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedDelete = kvStore.get(key);
      assertDocumentEquals(updatedDoc, docAfterFailedDelete);
    }
  }

  @Test
  public void testDeleteWithLatestVersionShouldDelete() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    kvStore.delete(key, VersionOption.from(updatedDoc));
    assertFalse(kvStore.contains(key));
  }

  @Test
  public void testDeleteWithLateVersionAdoptionShouldDelete() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(key, value, doc);

    kvStore.delete(key, VersionOption.from(doc));
    assertFalse(kvStore.contains(key));
  }

  @Test
  public void testDeleteWithManyVersionsFails() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(key, updateValue, doc.getTag(), updatedDoc);

    try {
      kvStore.delete(key, VersionOption.from(doc), VersionOption.from(updatedDoc));
      fail("Should not be able to perform a delete with multiple VersionOption(s).");
    } catch (IllegalArgumentException ex) {
      final Document<K, V> docAfterFailedDelete = kvStore.get(key);
      assertDocumentEquals(updatedDoc, docAfterFailedDelete);
    }
  }

  /**
   * Helper method to check that expected document is identical to result document.
   *
   * @param expected expected document.
   * @param result result document.
   */
  private void assertDocumentEquals(Document<K, V> expected, Document<K, V> result) {
    gen.assertKeyEquals(expected.getKey(), result.getKey());
    gen.assertValueEquals(expected.getValue(), result.getValue());
    assertEquals(expected.getTag(), result.getTag());
  }

  /**
   * Helper method to check that expected key and expected value matches ket and value in the result document.
   * It also ensures that the tag in the document is not {@code null} or empty.
   *
   * @param expectedKey the expected key.
   * @param expectedValue the expected value.
   * @param result the result document.
   */
  private void assertDocumentEquals(K expectedKey, V expectedValue, Document<K, V> result) {
    gen.assertKeyEquals(expectedKey, result.getKey());
    gen.assertValueEquals(expectedValue, result.getValue());
    assertFalse(TAG_ASSERT_FAILURE_MSG, Strings.isNullOrEmpty(result.getTag()));
  }

  /**
   * Helper method to check that result document contains the expected key and expected value. And that
   * the tag in the result document differs from the provided previous tag.
   *
   * @param expectedKey the expected key.
   * @param expectedValue the expected value.
   * @param prevTag the previous tag.
   * @param result the result document.
   */
  private void assertDocumentUpdated(K expectedKey, V expectedValue, String prevTag, Document<K, V> result) {
    gen.assertKeyEquals(expectedKey, result.getKey());
    gen.assertValueEquals(expectedValue, result.getValue());
    assertFalse(TAG_ASSERT_FAILURE_MSG, Strings.isNullOrEmpty(result.getTag()));
    assertNotEquals(prevTag, result.getTag());
  }
}
