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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.parameterized.ParametersRunnerFactory;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;
import com.dremio.datastore.generator.ByteContainerStoreGenerator.ByteContainer;
import com.dremio.datastore.generator.DataGenerator;
import com.dremio.datastore.generator.Dataset;
import com.dremio.datastore.generator.DocumentDataset;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Test kvstore + key value serde storage.
 */
@RunWith(Parameterized.class)
public abstract class AbstractTestKVStore<K, V> {
  private static final String TAG_ASSERT_FAILURE_MSG = "All documents should have a non-null, non-empty tag";
  private KVStore<K, V> kvStore;
  private static final int SAMPLING_SIZE = 30;
  private static final ImmutableSet<Class<?>> KEY_DOES_NOT_SUPPORT_FIND =
    ImmutableSet.of(UUID.class, Dummy.DummyId.class, DummyId.class);
  private static final ImmutableSet<Class<?>> STORE_DOES_NOT_SUPPORT_NULL_FIELDS =
    ImmutableSet.of(UUID.class, String.class, ByteContainer.class, Dummy.DummyId.class);

  /**
   * Exempt the parameters from the visibility modifier check.
   * Normally, we would have a public constructor to create the parameterized tests.
   * However, this is an abstract class, so we could not use the constructor approach here
   * without putting a burden on all extenders. If we wanted to make these variables private, we could
   * write a custom {@link ParametersRunnerFactory} to style-soundly inject our parameters. That would however
   * be a non-trivial amount of development.
   */
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
    return Arrays.asList(new Object[][]{
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

  protected abstract KVStoreProvider initProvider() throws Exception;
  protected abstract void closeProvider() throws Exception;

  @Before
  public void init() throws Exception {
    final KVStoreProvider provider = initProvider();
    kvStore = provider.getStore(storeCreationFunction);
    gen.reset();
  }

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

  public void init(final KVStore<K, V> kvStore) {
    this.kvStore = kvStore;
  }

  protected KVStore<K, V> getStore() {
    return kvStore;
  }

  @Test
  public void testGet() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    kvStore.put(key, value);
    final Document<K, V> doc = kvStore.get(key);

    gen.assertKeyEquals(key, doc.getKey());
    gen.assertValueEquals(value, doc.getValue());
    assertFalse(TAG_ASSERT_FAILURE_MSG, Strings.isNullOrEmpty(doc.getTag()));
  }

  @Test
  public void testGetAll() {
    final DocumentDataset<K, V> data =  generateDataAndPopulateKVStore(SAMPLING_SIZE);

    final Iterable<Document<K, V>> actualResult = kvStore.find();

    assertResultsAreEqual(data.getAllDocuments(), actualResult, false);
  }

  @Test
  public void testGetMissingKey() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final K missingKey = gen.newKey();

    kvStore.put(key, value);

    assertNull(kvStore.get(missingKey));
  }

  @Test
  public void testGetMissingKeys() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final K missingKey1 = gen.newKey();
    final K missingKey2 = gen.newKey();

    kvStore.put(key, value);
    final Iterable<Document<K, V>> results = kvStore.get(ImmutableList.of(missingKey1, missingKey2));

    assertEquals(2, Iterables.size(results));
    assertTrue(StreamSupport.stream(results.spliterator(), false).allMatch(Objects::isNull));
  }

  @Test
  public void testGetWithKeys() {
    final DocumentDataset<K, V> data =  generateDataAndPopulateKVStore(SAMPLING_SIZE / 2);

    final Iterable<Document<K,V>> result = kvStore.get(data.getKeys());

    assertResultsAreEqual(data.getAllDocuments(), result, false);
  }

  @Test
  public void testGetWithKeysStateless() {
    final DocumentDataset<K, V> data =  generateDataAndPopulateKVStore(SAMPLING_SIZE / 2);

    final Iterable<Document<K,V>> firstResult = kvStore.get(data.getKeys());
    assertResultsAreEqual(data.getAllDocuments(), firstResult, false);

    // Delete the first entry then try to get it using the get(List<K>) operator.
    final K firstEntry = data.getDocument(0).getKey();
    kvStore.delete(firstEntry);

    final Iterable<Document<K,V>> resultWithDeletedKey = kvStore.get(ImmutableList.of(firstEntry));
    assertEquals(1, Iterables.size(resultWithDeletedKey));
    assertNull(Iterables.get(resultWithDeletedKey, 0));
  }

  @Test
  public void testContainsMissingKey() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final K missingKey = gen.newKey();

    kvStore.put(key, value);

    assertFalse(kvStore.contains(missingKey));
  }

  @Test
  public void testContains() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    kvStore.put(key, value);

    assertTrue(kvStore.contains(key));
  }

  @Test
  public void testPlainPutsShouldStillBeVersioned() {
    final K key = gen.newKey();
    final V value1 = gen.newVal();
    final V value2 = gen.newVal();

    kvStore.put(key, value1);
    final Document<K, V> doc1 = kvStore.get(key);

    assertNotNull(doc1.getTag());

    kvStore.put(key, value2);
    final Document<K, V> doc2 = kvStore.get(key);

    assertNotNull(doc2.getTag());
    assertNotEquals(doc1.getTag(), doc2.getTag());
  }

  @Test
  public void testPutCreateAndOverwrite() {
    // Initial State
    final K key = gen.newKey();
    final V value1 = gen.newVal();
    final V value2 = gen.newVal();

    kvStore.put(key, value1);
    final Document<K, V> doc1 = kvStore.get(key);

    gen.assertValueEquals(value1, doc1.getValue());
    gen.assertKeyEquals(key, doc1.getKey());
    assertFalse(TAG_ASSERT_FAILURE_MSG, Strings.isNullOrEmpty(doc1.getTag()));

    // Overwrite 1
    kvStore.put(key, value2);
    final Document<K, V> doc2 = kvStore.get(key);

    gen.assertValueEquals(value2, doc2.getValue());
    gen.assertKeyEquals(key, doc1.getKey());
    assertFalse(TAG_ASSERT_FAILURE_MSG, Strings.isNullOrEmpty(doc2.getTag()));

    // Overwrite 2
    kvStore.put(key, value1);
    final Document<K, V> doc3 = kvStore.get(key);

    gen.assertValueEquals(value1, doc3.getValue());
    gen.assertKeyEquals(key, doc3.getKey());
    assertFalse(TAG_ASSERT_FAILURE_MSG, Strings.isNullOrEmpty(doc3.getTag()));
  }

  @Test(expected = NullPointerException.class)
  public void testPutWithNullValue() {
    kvStore.put(gen.newKey(), null);
  }

  @Test
  public void testDeleteNonExistentKey() {
    final K key = gen.newKey();
    final K missingKey = gen.newKey();
    final V value = gen.newVal();

    kvStore.put(key, value);
    kvStore.delete(missingKey);

    // Just making sure no exception is thrown and that the backend is untouched
    gen.assertValueEquals(value, kvStore.get(key).getValue());
  }

  @Test
  public void testDeleteExistingValue() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    kvStore.put(key, value);
    kvStore.delete(key);

    assertNull(kvStore.get(key));
  }

  @Test
  public void testFindByExclusiveStartEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 2, false, false, false);
  }

  @Test
  public void testFindByExclusiveStartInclusiveEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 3, false, true, false);
  }

  @Test
  public void testFindByInclusiveStartExclusiveEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 3, true, false, false);
  }

  @Test
  public void testFindByInclusiveStartEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 2, true, true, false);
  }

  @Test
  public void testFindOneByExclusiveEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 0, 1, true, false, false);
  }

  @Test
  public void testExclusiveStartEndEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, false, false, false);
  }

  @Test
  public void testInclusiveStartExclusiveEndEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, true, false, false);
  }

  @Test
  public void testExclusiveStartInclusiveEndEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, false, true, false);
  }

  @Test
  public void testInclusiveStartEndEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, true, true, false);
  }

  @Test
  public void testSortOrderWithExclusiveStartEndFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 8, SAMPLING_SIZE - 1, false, true, true);
  }

  @Test
  public void testSortOrderWithInclusiveStartExclusiveEndFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 3 * 2, true, true, true);
  }

  @Test
  public void testSortOrderWithExclusiveStartInclusiveEndFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 8, SAMPLING_SIZE - 1, false, true, true);
  }

  @Test
  public void testSortOrderWithInclusiveStartEndFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 8, SAMPLING_SIZE - 1, false, true, true);
  }

  @Test
  public void testFindAllEntriesByNewKeyRange() {
    ignoreIfFindNotSupported();
    final int numKVPairs = 6;
    final int numKVPairsToLoad = numKVPairs - 1;
    final int loadedDataEndIndex = numKVPairsToLoad - 1;
    final int unusedKeyIndex = numKVPairs - 1;

    // Dataset returned is sorted
    final Dataset<K, V> dataset = gen.sortDataset(gen.makeDataset(numKVPairs));
    // Insert all but the last document.
    final DocumentDataset<K, V> documentDataset = insertDataIntoKVStore(dataset.getDatasetSlice(0, loadedDataEndIndex));
    // The unused key is greater than all previous keys.
    final K unusedKey = dataset.getKey(unusedKeyIndex);

    final FindByRange<K> range = makeRange(dataset.getKey(0), true, unusedKey, true);
    final Iterable<Document<K, V>> result = kvStore.find(range);

    assertResultsAreEqual(documentDataset.getAllDocuments(), result, false);
  }

  @Test
  public void testExclusiveStartEndNullRange() {
    ignoreIfFindNotSupported();
    final DocumentDataset<K, V> data = generateDataAndPopulateKVStore(6);

    final FindByRange<K> range = makeRange(null, false, null, false);
    final Iterable<Document<K, V>> result = kvStore.find(range);

    assertResultsAreEqual(data.getAllDocuments(), result, false);
  }

  @Test
  public void testExclusiveStartInclusiveEndNullRange() {
    ignoreIfFindNotSupported();
    final DocumentDataset<K, V> data = gen.sortDocumentDataset(generateDataAndPopulateKVStore(6));

    final FindByRange<K> range = makeRange(data.getDocument(0).getKey(), false, null, true);
    final Iterable<Document<K, V>> result = kvStore.find(range);

    assertResultsAreEqual(data.getDocumentDatasetSlice(1, 5), result, false);
  }

  @Test
  public void testInclusiveStartExclusiveEndNullRange() {
    ignoreIfFindNotSupported();
    final DocumentDataset<K, V> data = gen.sortDocumentDataset(generateDataAndPopulateKVStore(6));

    final FindByRange<K> range = makeRange(null, true, data.getDocument(2).getKey(), false);
    final Iterable<Document<K, V>> result = kvStore.find(range);

    assertResultsAreEqual(data.getDocumentDatasetSlice(0, 1), result, false);
  }

  @Test
  public void testInclusiveStartEndNullRange() {
    ignoreIfFindNotSupported();
    final DocumentDataset<K, V> data = gen.sortDocumentDataset(generateDataAndPopulateKVStore(6));

    final FindByRange<K> range = makeRange(null, true, null, true);
    final Iterable<Document<K, V>> result = kvStore.find(range);

    assertResultsAreEqual(data.getAllDocuments(), result, false);
  }

  @Test
  public void testNullableFields() {
    // This test is only ran against protostuff since its fields can be null if not set.
    ignoreIfNullFieldsNotSupported();
    final K key = gen.newKey();
    final V value = gen.newVal();
    final V valueWithNullFields = gen.newValWithNullFields();

    kvStore.put(key, value);
    gen.assertValueEquals(value, kvStore.get(key).getValue());

    kvStore.put(key, valueWithNullFields);
    gen.assertValueEquals(valueWithNullFields, kvStore.get(key).getValue());
  }

  /**
   * Utility method to temporarily disable test. Test must be re-enabled when ticket is resolved.
   *
   * @param ticket Jira ticket number, in the form of "DX-12345: Ticket title".
   * @param message the message explaining temporary test ignore.
   * @param classes key classes in which test should be ignored.
   */
  protected void temporarilyDisableTest(String ticket, String message, Set<Class<?>> classes){
    Assume.assumeFalse("[TEST DISABLED]" + ticket + " - " + message, classes.contains(keyClass));
  }

  /**
   * Method to ignore tests for data formats that do not support{@code null} fields, namely protobuf, UUID, String and bytes.
   */
  private void ignoreIfNullFieldsNotSupported() {
    Assume.assumeFalse("This store value format does not support null fields", STORE_DOES_NOT_SUPPORT_NULL_FIELDS.contains(keyClass));
  }

  /**
   * Method to ignore tests for data formats that do not support range queries, namely protobuf, protostuff and UUID.
   */
  private void ignoreIfFindNotSupported() {
    Assume.assumeFalse("This format does not support range queries", KEY_DOES_NOT_SUPPORT_FIND.contains(keyClass));
  }

  /**
   * Helper method to create build an immutable FindByRange instance.
   *
   * @param start          key to the start of the range.
   * @param inclusiveStart whether start is inclusive.
   * @param end            key to the end of the range.
   * @param inclusiveEnd   whether end is inclusive.
   * @return An immutable FindByRange instance.
   */

  private FindByRange<K> makeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd) {
    return new ImmutableFindByRange.Builder<K>()
      .setStart(start)
      .setIsStartInclusive(inclusiveStart)
      .setEnd(end)
      .setIsEndInclusive(inclusiveEnd)
      .build();
  }

  /**
   * Helper method to determine the expected size of a FindByRange query result set.
   *
   * @param startRange     start range index.
   * @param endRange       end range index.
   * @param startInclusive whether start is inclusive.
   * @param endInclusive   whether end is inclusive.
   * @return expected size of the resutl set.
   */
  private int expectedSizeHelper(int startRange, int endRange, boolean startInclusive, boolean endInclusive) {
    if (startInclusive && endInclusive) {
      return endRange - startRange + 1;
    } else if (startInclusive || endInclusive) {
      return endRange - startRange;
    } else {
      return endRange - startRange - 1;
    }
  }

  /**
   * Helper method to test FindByRange queries.
   *
   * @param samplingSize   size of the dataset to test.
   * @param startRange     start range index.
   * @param endRange       end range index.
   * @param startInclusive whether start is inclusive.
   * @param endInclusive   whether end is inclusive.
   * @param testSortOrder  boolean indicating whether test should take result set sort order into account.
   */
  private void testFindByRange(int samplingSize, int startRange, int endRange,
                               boolean startInclusive, boolean endInclusive, boolean testSortOrder) {
    final DocumentDataset<K, V> data = gen.sortDocumentDataset(generateDataAndPopulateKVStore(samplingSize));
    final int expectedSize = expectedSizeHelper(startRange, endRange, startInclusive, endInclusive);
    final int sliceStart = (startInclusive) ? startRange : startRange + 1;
    final int sliceEnd = (endInclusive) ? endRange : endRange - 1;

    final FindByRange<K> range = makeRange(data.getDocument(startRange).getKey(), startInclusive, data.getDocument(endRange).getKey(), endInclusive);
    final Iterable<Document<K, V>> result = kvStore.find(range);

    if (expectedSize <= 0 && (sliceStart > sliceEnd)) {
      // Testing empty ranges, result should be empty.
      assertEquals(0, Iterables.size(result));
    } else {
      assertResultsAreEqual(data.getDocumentDatasetSlice(sliceStart, sliceEnd), result, testSortOrder);
    }
  }

  /**
   * Helper method to check whether expected and actual documents have the same element in the same order.
   *
   * @param expectedDocuments Iterable of expected results.
   * @param actualDocuments   Iterable of actual results.
   */
  private void assertDocumentsAreEqual(Iterable<Document<K, V>> expectedDocuments, Iterable<Document<K, V>> actualDocuments) {
    assertSizeIsEqual(expectedDocuments, actualDocuments);

      final Iterator<Document<K, V>> iter = actualDocuments.iterator();
      for (final Document<K, V> expectedDocument : expectedDocuments) {
        assertTrue(iter.hasNext());
        final Document<K, V> actualDocument = iter.next();
        gen.assertKeyEquals(expectedDocument.getKey(), actualDocument.getKey());
        gen.assertValueEquals(expectedDocument.getValue(), actualDocument.getValue());
        assertEquals(expectedDocument.getTag(), actualDocument.getTag());
      }
  }

  /**
   * Helper method to check whether expected and actual results have the same element.
   *
   * @param expectedDocuments Iterable of expected results.
   * @param actualDocuments   Iterable of actual results.
   * @param checkSortOrder    whether to validate the order of the results.
   */
  private void assertResultsAreEqual(Iterable<Document<K, V>> expectedDocuments, Iterable<Document<K, V>> actualDocuments, boolean checkSortOrder) {
    assertSizeIsEqual(expectedDocuments, actualDocuments);

    if (checkSortOrder) {
      assertDocumentsAreEqual(expectedDocuments, actualDocuments);
    } else {
      assertDocumentsAreEqual(gen.sortDocuments(expectedDocuments), gen.sortDocuments(actualDocuments));
    }
  }

  /**
   * Helper method to check that result set has the expected size.
   *
   * @param expectedDocuments Iterable of expected results.
   * @param actualDocuments   Iterable of actual results.
   */
  private void assertSizeIsEqual(Iterable<Document<K, V>> expectedDocuments, Iterable<Document<K, V>> actualDocuments) {
    assertEquals(Iterables.size(expectedDocuments), Iterables.size(actualDocuments));
  }

  /**
   * Inserts the data into the KV Store.
   *
   * @param dataset data to be inserted.
   * @return DocumentDataset with the list of documents inserted into the KV Store.
   */
  private DocumentDataset<K, V> insertDataIntoKVStore(Iterable<Map.Entry<K, V>> dataset) {
    final ArrayList<Document<K, V>> documentList = new ArrayList<>();
    dataset.forEach(entry -> {
      kvStore.put(entry.getKey(), entry.getValue());
      documentList.add(kvStore.get(entry.getKey()));
    });
    return new DocumentDataset<>(documentList);
  }

  /**
   * Generates data and populates the KV Store with the generated data.
   *
   * @param numKVPairs number of unique keys and values to put into the store.
   * @return DocumentDataset with the list of documents inserted into the KV Store.
   */
  private DocumentDataset<K, V> generateDataAndPopulateKVStore(int numKVPairs) {
    final Dataset<K, V> data = gen.makeDataset(numKVPairs);
    return insertDataIntoKVStore(data.getDatasetSlice(0, numKVPairs - 1));
  }
}
