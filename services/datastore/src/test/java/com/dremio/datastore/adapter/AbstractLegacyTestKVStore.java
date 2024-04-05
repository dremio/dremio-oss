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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.datastore.adapter.stores.LegacyByteContainerStore;
import com.dremio.datastore.adapter.stores.LegacyProtobufStore;
import com.dremio.datastore.adapter.stores.LegacyProtostuffStore;
import com.dremio.datastore.adapter.stores.LegacyRawByteStore;
import com.dremio.datastore.adapter.stores.LegacyStringStore;
import com.dremio.datastore.adapter.stores.LegacyUUIDStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.format.visitor.SupportFindFormatVisitor;
import com.dremio.datastore.format.visitor.SupportNullFieldsFormatVisitor;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;
import com.dremio.datastore.generator.ByteContainerStoreGenerator.ByteContainer;
import com.dremio.datastore.generator.DataGenerator;
import com.dremio.datastore.generator.Dataset;
import com.dremio.datastore.generator.ProtobufStoreGenerator;
import com.dremio.datastore.generator.ProtostuffStoreGenerator;
import com.dremio.datastore.generator.RawByteStoreGenerator;
import com.dremio.datastore.generator.StringStoreGenerator;
import com.dremio.datastore.generator.UUIDStoreGenerator;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;
import com.dremio.datastore.proto.Dummy;
import com.dremio.datastore.proto.DummyId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.parameterized.ParametersRunnerFactory;

/**
 * Test kvstore + key value serde storage.
 *
 * <p>TODO: add tests that explore the limits of values.
 */
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public abstract class AbstractLegacyTestKVStore<K, V> {
  private static final int SAMPLING_SIZE = 30;
  private static final ImmutableSet<Class<?>> KEY_DOES_NOT_SUPPORT_FIND =
      ImmutableSet.of(UUID.class, Dummy.DummyId.class, DummyId.class);
  private static final ImmutableSet<Class<?>> STORE_DOES_NOT_SUPPORT_NULL_FIELDS =
      ImmutableSet.of(UUID.class, String.class, ByteContainer.class, Dummy.DummyId.class);

  private LegacyKVStore<K, V> kvStore;
  private LegacyKVStoreProvider provider;

  /**
   * Exempt the parameters from the visibility modifier check.
   *
   * <p>Normally, we would have a public constructor to create the parameterized tests. However,
   * this is an abstract class, so we could not use the constructor approach here without putting a
   * burden on all extenders. If we wanted to make these variables private, we could write a custom
   * {@link ParametersRunnerFactory} to style-soundly inject our parameters. That would however be a
   * non-trivial amount of development.
   */
  @Parameter public Class<TestLegacyStoreCreationFunction<K, V>> storeCreationFunction;

  @Parameter(1)
  public DataGenerator<K, V> gen;

  @Parameterized.Parameters(name = "Table: {0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(
        new Object[][] {
          {LegacyUUIDStore.class, new UUIDStoreGenerator()},
          {LegacyProtobufStore.class, new ProtobufStoreGenerator()},
          {LegacyProtostuffStore.class, new ProtostuffStoreGenerator()},
          // Variable-length
          {
            LegacyStringStore.class, new StringStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH)
          },
          {
            LegacyRawByteStore.class,
            new RawByteStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH)
          },
          {
            LegacyByteContainerStore.class,
            new ByteContainerStoreGenerator(UniqueSupplierOptions.VARIABLE_LENGTH)
          },
          // Fixed-length
          {LegacyStringStore.class, new StringStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH)},
          {LegacyRawByteStore.class, new RawByteStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH)},
          {
            LegacyByteContainerStore.class,
            new ByteContainerStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH)
          }
        });
  }

  @Before
  public void init() throws Exception {
    provider = createProvider();
    provider.start();
    kvStore = provider.getStore(storeCreationFunction);
    gen.reset();
  }

  protected abstract LegacyKVStoreProvider createProvider();

  @After
  public void tearDown() throws Exception {
    provider.close();
  }

  protected LegacyKVStore<K, V> getStore() {
    return kvStore;
  }

  @Test
  public void testEntries() {
    final Dataset<K, V> data = populateStoreWith(SAMPLING_SIZE);

    assertResultsEqual(data.getDatasetSlice(0, SAMPLING_SIZE - 1), kvStore.find(), false);
  }

  @Test
  public void testGetWithKeys() {
    final Dataset<K, V> data = populateStoreWith(SAMPLING_SIZE / 2);
    final List<V> result = kvStore.get(data.getKeys());

    assertEquals(data.getValues().size(), result.size());
    assertValuesEquals(data.getValues(), result);
  }

  @Test
  public void testGetWithKeysStateless() {
    final Dataset<K, V> data = populateStoreWith(SAMPLING_SIZE / 2);
    final List<V> firstResult = kvStore.get(data.getKeys());

    assertEquals(data.getValues().size(), firstResult.size());

    // Delete the first entry, then try to get(List<k>) with the first entry.
    final K firstEntry = data.getKey(0);
    kvStore.delete(firstEntry);

    final List<V> resultWithDeletedKey = kvStore.get(ImmutableList.of(firstEntry));
    assertEquals(1, resultWithDeletedKey.size());
    assertNull(resultWithDeletedKey.get(0));
  }

  @Test
  public void testGetWithMissingKey() {
    final K key = gen.newKey();
    final V value = gen.newVal();

    final K missingKey = gen.newKey();

    kvStore.put(key, value);
    assertNull(kvStore.get(missingKey));
  }

  @Test
  public void testGetWithMissingKeys() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    final int numMissingKeys = SAMPLING_SIZE / 2;

    final ImmutableList.Builder<K> missingKeysBuilder = new ImmutableList.Builder<>();
    final ArrayList<K> expected = new ArrayList<>();

    for (int i = 0; i < numMissingKeys; i++) {
      missingKeysBuilder.add(gen.newKey());
      expected.add(null);
    }

    kvStore.put(key, value);
    assertEquals(expected, kvStore.get(missingKeysBuilder.build()));
  }

  @Test
  public void testFindAll() {
    final int numPairs = 3;
    final int startSlice = 0;
    final int endSlice = numPairs - 1;

    final Dataset<K, V> data = populateStoreWith(numPairs);
    final Iterable<Entry<K, V>> entries = kvStore.find();
    assertEquals(Iterables.size(entries), numPairs);

    final Iterable<Entry<K, V>> expected = data.getDatasetSlice(startSlice, endSlice);
    assertResultsEqual(expected, entries, false);
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
  public void testPutAndUpdate() {
    final K key = gen.newKey();

    final V initialValue = gen.newVal();
    final V valueUpdate1 = gen.newVal();
    final V valueUpdate2 = gen.newVal();

    putAndValidate(key, initialValue);
    putAndValidate(key, valueUpdate1);
    putAndValidate(key, valueUpdate2);
  }

  @Test
  public void testPutWithNullValue() {
    final K key = gen.newKey();
    final V value = null;

    try {
      kvStore.put(key, value);
      fail("KVtore null value insertions should fail with NullPointerException");
    } catch (NullPointerException e) {
    }
  }

  @Test
  public void testDeleteMissingKey() {
    final K key = gen.newKey();
    final V value = gen.newVal();
    kvStore.put(key, value);

    final K misingKey = gen.newKey();

    kvStore.delete(misingKey);

    // Just making sure no exception is thrown and that the backend is untouched
    gen.assertValueEquals(value, kvStore.get(key));
  }

  @Test
  public void testDelete() {
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
  public void testFindByExclusiveStartRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 3, false, true, false);
  }

  @Test
  public void testFindByExclusiveEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 3, true, false, false);
  }

  @Test
  public void testFindOneByExclusiveEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 0, 1, true, false, false);
  }

  @Test
  public void testFindByInclusiveStartEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 2, true, true, false);
  }

  @Test
  public void testExclusiveEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, false, false, false);
  }

  @Test
  public void testInclusiveStartEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, true, false, false);
  }

  @Test
  public void testInclusiveEndEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, false, true, false);
  }

  @Test
  public void testInclusiveStartEndEmptyRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE / 3, 5, 5, true, true, false);
  }

  @Test
  public void testSortOrderWithInclusiveEndFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 8, SAMPLING_SIZE - 1, false, true, true);
  }

  @Test
  public void testSortOrderWithInclusiveStartFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 8, SAMPLING_SIZE - 1, true, false, true);
  }

  @Test
  public void testSortOrderWithInclusiveFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 2, true, true, true);
  }

  @Test
  public void testSortOrderWithExclusiveFindRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, 0, SAMPLING_SIZE / 2, false, false, true);
  }

  @Test
  public void testNullInclusiveStartRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, -1, SAMPLING_SIZE / 2, true, true, false);
  }

  @Test
  public void testNullExclusiveStartRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, -1, SAMPLING_SIZE / 2, false, true, false);
  }

  @Test
  public void testNullInclusiveEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, SAMPLING_SIZE / 2, -1, true, true, false);
  }

  @Test
  public void testNullExclusiveEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, SAMPLING_SIZE / 2, -1, true, false, false);
  }

  @Test
  public void testNullInclusiveStartEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, -1, -1, true, true, false);
  }

  @Test
  public void testNullExclusiveStartEndRange() {
    ignoreIfFindNotSupported();
    testFindByRange(SAMPLING_SIZE, -1, -1, false, false, false);
  }

  @Test
  public void testFindAllEntriesByNewKeyRange() {
    ignoreIfFindNotSupported();

    final int numKVPairs = 6;
    final int numKVPairsToLoad = 5;
    final int loadedDataEndIndex = 4;
    final int unusedKeyIndex = 5;

    // Dataset returned is sorted
    final Dataset<K, V> dataset = gen.sortDataset(gen.makeDataset(numKVPairs));

    // Load the first 5 values into the kvstore.
    for (int i = 0; i < numKVPairsToLoad; i++) {
      kvStore.put(dataset.getKey(i), dataset.getVal(i));
    }

    // The unused key is greater than all previous keys.
    final K unusedKey = dataset.getKey(unusedKeyIndex);
    final LegacyFindByRange<K> range =
        new LegacyFindByRange<>(dataset.getKey(0), true, unusedKey, true);
    final Iterable<Entry<K, V>> result = kvStore.find(range);
    assertResultsEqual(dataset.getDatasetSlice(0, loadedDataEndIndex), result, false);
  }

  @Test
  public void testNullableFields() {
    // This test is only ran against protostuff since its fields can be null if not set.
    ignoreIfNullFieldsNotSupported();
    final K key = gen.newKey();
    final V value = gen.newVal();
    putAndValidate(key, value);

    final V valueWithNullFields = gen.newValWithNullFields();
    putAndValidate(key, valueWithNullFields);
  }

  /**
   * Custom assert method to compare equality of two list of value objects.
   *
   * @param expected list of expected value objects.
   * @param actual list of actual expected objects.
   */
  private void assertValuesEquals(List<V> expected, List<V> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      gen.assertValueEquals(expected.get(i), actual.get(i));
    }
  }

  /**
   * Utility method to temporarily disable test. Test must be re-enabled when ticket is resolved.
   *
   * @param ticket Jira ticket number, in the form of "DX-12345: Ticket title".
   * @param message the message explaining temporary test ignore.
   * @param classes key classes in which test should be ignored.
   */
  protected void temporarilyDisableTest(String ticket, String message, Set<Class<?>> classes) {
    try {
      Assume.assumeFalse(
          "[TEST DISABLED]" + ticket + " - " + message,
          !Collections.disjoint(
              classes,
              storeCreationFunction.getDeclaredConstructor().newInstance().getKeyClasses()));
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      fail("TestStoreCreationFunction instantiation problem: " + e.getMessage());
    }
  }

  /**
   * Method to ignore tests for data formats that do not support{@code null} fields, namely
   * protobuf, UUID, String and bytes.
   */
  private void ignoreIfNullFieldsNotSupported() {
    try {
      Assume.assumeTrue(
          "This store value format does not support null fields",
          storeCreationFunction
              .getDeclaredConstructor()
              .newInstance()
              .getKeyFormat()
              .apply(new SupportNullFieldsFormatVisitor()));
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      fail("TestStoreCreationFunction instantiation problem: " + e.getMessage());
    }
  }

  /**
   * Method to ignore tests for data formats that do not support range queries, namely protobuf,
   * protostuff and UUID.
   */
  private void ignoreIfFindNotSupported() {
    try {
      Assume.assumeTrue(
          "This format does not support range queries",
          storeCreationFunction
              .getDeclaredConstructor()
              .newInstance()
              .getKeyFormat()
              .apply(new SupportFindFormatVisitor()));
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      fail("TestStoreCreationFunction instantiation problem: " + e.getMessage());
    }
  }

  /**
   * Helper method to obtain the end key from the testing dataset. Returns {@code null} if provided
   * endRange is negative.
   *
   * @param data testing dataset.
   * @param endRange end range index.
   * @return key K from the testing dataset. Returns {@code null} if provided endRange is negative.
   */
  private K getFindByRangeEndKey(Dataset<K, V> data, int endRange) {
    return (endRange < 0) ? null : data.getKey(endRange);
  }

  /**
   * Helper method to obtain the start key from the testing dataset. Returns {@code null} if
   * provided startRange is negative.
   *
   * @param data testing dataset.
   * @param startRange start range index.
   * @return key K from the testing dataset. Returns {@code null} if provided startRange is
   *     negative.
   */
  private K getFindByRangeStartKey(Dataset<K, V> data, int startRange) {
    return (startRange < 0) ? null : data.getKey(startRange);
  }

  /**
   * Helper method to obtain expected result of a FindByRange query from the testing dataset.
   * Returns an empty map if expected results is empty.
   *
   * @param samplingSize size of the testing dataset.
   * @param data testing dataset.
   * @param startRange the startRange index.
   * @param endRange the endRange index.
   * @param startInclusive whether start range is inclusive.
   * @param endInclusive whether end range is inclusive.
   * @return a map of type K, V. Returns an empty map if expected results is empty.
   */
  private Iterable<Entry<K, V>> getExpectedFindByRangeResult(
      int samplingSize,
      Dataset<K, V> data,
      int startRange,
      int endRange,
      boolean startInclusive,
      boolean endInclusive) {
    final int start = (startRange < 0) ? 0 : (startInclusive) ? startRange : startRange + 1;
    final int end = (endRange < 0) ? samplingSize - 1 : (endInclusive) ? endRange : endRange - 1;
    return (start > end) ? Collections.emptyList() : data.getDatasetSlice(start, end);
  }

  /**
   * Helper method to test FindByRange queries.
   *
   * @param samplingSize size of the dataset to test.
   * @param startRange start range index.
   * @param endRange end range index.
   * @param startInclusive whether start is inclusive.
   * @param endInclusive whether end is inclusive.
   * @param testSortOrder boolean indicating whether test should take result set sort order into
   *     account.
   */
  private void testFindByRange(
      int samplingSize,
      int startRange,
      int endRange,
      boolean startInclusive,
      boolean endInclusive,
      boolean testSortOrder) {
    final Dataset<K, V> data = populateStoreWith(samplingSize);
    final Iterable<Entry<K, V>> expected =
        getExpectedFindByRangeResult(
            samplingSize, data, startRange, endRange, startInclusive, endInclusive);

    LegacyFindByRange<K> range =
        new LegacyFindByRange<>(
            getFindByRangeStartKey(data, startRange), startInclusive,
            getFindByRangeEndKey(data, endRange), endInclusive);

    final Iterable<Entry<K, V>> result = kvStore.find(range);

    if (Iterables.isEmpty(expected)) {
      // Testing empty ranges, result should be empty.
      assertEquals(0, Iterables.size(result));
    } else {
      assertResultsEqual(expected, result, testSortOrder);
    }
  }

  /**
   * Helper method to check whether two Iterable of Entry(s) of key type K and value type V are
   * identical. If checkSortOrder is true, it checks whether the two Iterable(s) are equal without
   * sorting, otherwise, it sorts the two Iterables before checking for equality.
   *
   * @param expected expected Iterable
   * @param actual actual Iterable.
   * @param checkSortOrder whether to check for exact sort order.
   */
  private void assertResultsEqual(
      Iterable<Entry<K, V>> expected, Iterable<Entry<K, V>> actual, boolean checkSortOrder) {
    assertSizeEquals(expected, actual);

    if (checkSortOrder) {
      assertEntriesEqual(expected, actual);
    } else {
      assertEntriesEqual(gen.sortEntries(expected), gen.sortEntries(actual));
    }
  }

  /**
   * Helper method to check whether two Iterable of Entry(s) of key type K and value type V are
   * identical.
   *
   * @param expected expected Iterable.
   * @param actual actual Iterable.
   */
  private void assertEntriesEqual(Iterable<Entry<K, V>> expected, Iterable<Entry<K, V>> actual) {
    final Iterator<Entry<K, V>> iter = actual.iterator();
    for (final Entry<K, V> expectedEntry : expected) {
      assertTrue(iter.hasNext());
      final Entry<K, V> actualEntry = iter.next();
      gen.assertKeyEquals(expectedEntry.getKey(), actualEntry.getKey());
      gen.assertValueEquals(expectedEntry.getValue(), actualEntry.getValue());
    }
  }

  /**
   * Check that the size of the two Iterable(s) are equal.
   *
   * @param expected expected Iterable.
   * @param actual actual Iterable.
   */
  private void assertSizeEquals(Iterable<Entry<K, V>> expected, Iterable<Entry<K, V>> actual) {
    assertEquals(Iterables.size(expected), Iterables.size(actual));
  }

  /**
   * Helper method to put and check that value in the store is as expected.
   *
   * @param k key.
   * @param v value.
   */
  private void putAndValidate(K k, V v) {
    kvStore.put(k, v);
    gen.assertValueEquals(v, kvStore.get(k));
  }

  /**
   * Populates the kv store with data and returns the dataset.
   *
   * @param numKVPairs number of unique keys and values to put into the store.
   * @return dataset used.
   */
  private Dataset<K, V> populateStoreWith(int numKVPairs) {
    final Dataset<K, V> data = gen.makeDataset(numKVPairs);

    for (int i = 0; i < numKVPairs; i++) {
      kvStore.put(data.getKey(i), data.getVal(i));
    }
    return gen.sortDataset(data);
  }
}
