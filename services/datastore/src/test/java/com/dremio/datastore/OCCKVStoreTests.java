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

import java.util.ConcurrentModificationException;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.datastore.generator.DataGenerator;
import com.google.common.base.Strings;

/**
 * Static OCCKVStore tests so they may be used with an OCC only environment, as well as an indexed OCC environment.
 */
public final class OCCKVStoreTests {
  private static final String TAG_ASSERT_FAILURE_MSG = "All documents should have a non-null, non-empty tag";

  //@Test(expected = NullPointerException.class)
  public static <K, V> void testShouldNotPutNulls(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    kvStore.put(key, null);
  }

  public static <K, V> void testPutWithoutCreateOptionIsValid(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);
  }

  public static <K, V> void testPutWithCreateOption(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(gen, key, value, doc);
  }

  public static <K, V> void testPutWithCreateOptionFailsToOverwrite(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    try {
      final V updateValue = gen.newVal();
      kvStore.put(key, updateValue, KVStore.PutOption.CREATE);
      fail("Should not be able to update with a CREATE option.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(gen, doc, docAfterFailedPut);
    }
  }

  public static <K, V> void testModificationWithoutOptionsAlwaysOverwrites(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key,updateValue);
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);
  }

  public static <K, V> void testPutAfterDeleteWithoutOptions(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    kvStore.delete(key);
    final Document<K, V> nullDoc = kvStore.get(key);
    assertNull(nullDoc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key,updateValue);
    assertDocumentEquals(gen, key, updateValue, updatedDoc);
  }

  public static <K, V> void testPutAlwaysUsingOptionsShouldUpdate(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    final V updateValue2 = gen.newVal();
    final Document<K, V> updatedDoc2 = kvStore.put(key, updateValue2, VersionOption.from(updatedDoc));
    assertDocumentUpdated(gen, key, updateValue2, updatedDoc.getTag(), updatedDoc2);
  }

  public static <K, V> void testPutLateOptionAdoptionShouldStillUpdate(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    final V updateValue2 = gen.newVal();
    final Document<K, V> updatedDoc2 = kvStore.put(key, updateValue2, VersionOption.from(updatedDoc));
    assertDocumentUpdated(gen, key, updateValue2, updatedDoc.getTag(), updatedDoc2);
  }

  public static <K, V> void testPutLateOptionAdoptionShouldStillProtectFromCM(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    try{
      final V updateValue2 = gen.newVal();
      kvStore.put(key, updateValue2, VersionOption.from(doc));
      fail("Should not be able to update with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(gen, updatedDoc, docAfterFailedPut);
    }
  }

  public static <K, V> void testCreateWithManyOptionsFails(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    try {
      final V updateValue = gen.newVal();
      kvStore.put(key, updateValue, KVStore.PutOption.CREATE, VersionOption.from(doc));
      fail("If multiple VersionOption(s) are passed, the put should not be performed.");
    } catch (IllegalArgumentException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(gen, doc, docAfterFailedPut);
    }
  }

  public static <K, V> void testPutWithMultipleVersionsFails(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K ,V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    try {
      final V updateValue2 = gen.newVal();
      kvStore.put(key, updateValue2, VersionOption.from(doc), VersionOption.from(updatedDoc));
      fail("If multiple VersionOption(s) are passed, the put should not be performed.");
    } catch (IllegalArgumentException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(gen, updatedDoc, docAfterFailedPut);
    }
  }

  public static <K, V> void testPutWithOutdatedVersionFails(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> originalDoc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(gen, key, value, originalDoc);

    final V updateValue = gen.newVal();
    final Document<K, V> lastGoodDoc = kvStore.put(key, updateValue, VersionOption.from(originalDoc));
    assertDocumentUpdated(gen, key, updateValue, originalDoc.getTag(), lastGoodDoc);

    try {
      final V updateValue2 = gen.newVal();
      kvStore.put(key, updateValue2, VersionOption.from(originalDoc));
      fail("Should not be able to update with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedPut = kvStore.get(key);
      assertDocumentEquals(gen, lastGoodDoc, docAfterFailedPut);
    }
  }

  public static <K, V> void testDeleteWithoutTagAlwaysDeletes(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    kvStore.delete(key);
    assertFalse(kvStore.contains(key));
  }

  public static <K, V> void testDeleteWithOutdatedVersionFails(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    try {
      kvStore.delete(key, VersionOption.from(doc));
      fail("Should not be able to delete a document with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedDelete = kvStore.get(key);
      assertDocumentEquals(gen, updatedDoc, docAfterFailedDelete);
    }
  }

  public static <K, V> void testDeleteWithLateOptionShouldStillProtectFromCM(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue);
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    try {
      kvStore.delete(key, VersionOption.from(doc));
      fail("Should not be able to delete a document with an outdated tag.");
    } catch (ConcurrentModificationException ex) {
      final Document<K, V> docAfterFailedDelete = kvStore.get(key);
      assertDocumentEquals(gen, updatedDoc, docAfterFailedDelete);
    }
  }

  public static <K, V> void testDeleteWithLatestVersionShouldDelete(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    kvStore.delete(key, VersionOption.from(updatedDoc));
    assertFalse(kvStore.contains(key));
  }

  public static <K, V> void testDeleteWithLateVersionAdoptionShouldDelete(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value);
    assertDocumentEquals(gen, key, value, doc);

    kvStore.delete(key, VersionOption.from(doc));
    assertFalse(kvStore.contains(key));
  }

  public static <K, V> void testDeleteWithManyVersionsFails(KVStore<K, V> kvStore, DataGenerator<K, V> gen) {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final Document<K, V> doc = kvStore.put(key, value, KVStore.PutOption.CREATE);
    assertDocumentEquals(gen, key, value, doc);

    final V updateValue = gen.newVal();
    final Document<K, V> updatedDoc = kvStore.put(key, updateValue, VersionOption.from(doc));
    assertDocumentUpdated(gen, key, updateValue, doc.getTag(), updatedDoc);

    try {
      kvStore.delete(key, VersionOption.from(doc), VersionOption.from(updatedDoc));
      fail("Should not be able to perform a delete with multiple VersionOption(s).");
    } catch (IllegalArgumentException ex) {
      final Document<K, V> docAfterFailedDelete = kvStore.get(key);
      assertDocumentEquals(gen, updatedDoc, docAfterFailedDelete);
    }
  }

  /**
   * Helper method to check that expected document is identical to result document.
   *
   * @param expected expected document.
   * @param result result document.
   */
  private static <K, V> void assertDocumentEquals(DataGenerator<K, V> gen, Document<K, V> expected, Document<K, V> result) {
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
  private static <K, V> void assertDocumentEquals(DataGenerator<K, V> gen, K expectedKey, V expectedValue, Document<K, V> result) {
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
  private static <K, V> void assertDocumentUpdated(DataGenerator<K, V> gen, K expectedKey, V expectedValue, String prevTag, Document<K, V> result) {
    gen.assertKeyEquals(expectedKey, result.getKey());
    gen.assertValueEquals(expectedValue, result.getValue());
    assertFalse(TAG_ASSERT_FAILURE_MSG, Strings.isNullOrEmpty(result.getTag()));
    assertNotEquals(prevTag, result.getTag());
  }
}
