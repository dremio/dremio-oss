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
package com.dremio.exec.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.test.AllocatorRule;

public class ValueListWithBloomFilterTest {

  private static final String TEST_NAME = "TestName";
  private BufferAllocator testAllocator;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-valuelistfilter", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() {
    testAllocator.close();
  }

  @Test
  public void testInsertionInt() throws Exception {
    try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 4, false);
         AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable();
         ArrowBuf keyBuf = testAllocator.buffer(4)) {
      builder.setup();
      List<Integer> insertedVals = Arrays.asList(1, 2, 3, 10, 20, 30, 100, 200, 300, 90);
      insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
      builder.setName(TEST_NAME);
      builder.setFieldType(Types.MinorType.INT);

      ValueListFilter valueListFilter = builder.build();
      closer.add(valueListFilter);

      ValueListWithBloomFilter valueListWithBloomFilter = ValueListFilterBuilder.fromBufferWithBloomFilter(testAllocator.buffer(valueListFilter.buf().capacity() + ValueListFilter.BLOOM_FILTER_SIZE), valueListFilter);

      closer.add(valueListWithBloomFilter);

      ArrowBuf elements = valueListWithBloomFilter.valOnlyBuf();

      assertEquals(insertedVals.size(), valueListWithBloomFilter.getValueCount());
      List<Integer> storedVals = new ArrayList<>(valueListWithBloomFilter.getValueCount());
      IntStream.range(0, valueListWithBloomFilter.getValueCount()).forEach(i -> storedVals.add(elements.getInt(i * 4)));

      Collections.sort(insertedVals); // expect unique sorted
      assertEquals(insertedVals, storedVals);

      insertedVals.forEach(x -> assertTrue(valueListWithBloomFilter.mightBePresent(x)));
    }
  }

  @Test
  public void testInsertionLong() throws Exception {
    try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 8, false);
         AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable();
         ArrowBuf keyBuf = testAllocator.buffer(8)) {
      builder.setup();
      List<Long> insertedVals = Arrays.asList(1L, 2L, 3L, 123L, 132L, 12L, 11L, 41L, 90L, 37L);
      insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
      builder.setName(TEST_NAME);
      builder.setFieldType(Types.MinorType.BIGINT);

      ValueListFilter valueListFilter = builder.build();
      closer.add(valueListFilter);

      ValueListWithBloomFilter valueListWithBloomFilter = ValueListFilterBuilder.fromBufferWithBloomFilter(testAllocator.buffer(valueListFilter.buf().capacity() + ValueListFilter.BLOOM_FILTER_SIZE), valueListFilter);

      closer.add(valueListWithBloomFilter);

      ArrowBuf elements = valueListWithBloomFilter.valOnlyBuf();

      assertEquals(insertedVals.size(), valueListWithBloomFilter.getValueCount());
      List<Long> storedVals = new ArrayList<>(valueListWithBloomFilter.getValueCount());
      IntStream.range(0, valueListWithBloomFilter.getValueCount()).forEach(i -> storedVals.add(elements.getLong(i * 8)));

      Collections.sort(insertedVals); // expect unique sorted
      assertEquals(insertedVals, storedVals);

      insertedVals.forEach(x -> assertTrue(valueListWithBloomFilter.mightBePresent(x)));
    }
  }

  @Test
  public void testBloomFilterBuilder() throws Exception {
    try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 8, false, true);
         AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable();
         ArrowBuf keyBuf = testAllocator.buffer(8)) {
      builder.setup();
      List<Long> insertedVals = Arrays.asList(1L, 2L, 3L, 123L, 132L, 12L, 11L, 41L, 90L, 37L);
      insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
      builder.setName(TEST_NAME);
      builder.setFieldType(Types.MinorType.BIGINT);

      ValueListFilter valueListFilter = builder.build();
      closer.add(valueListFilter);
      ArrowBuf elements = valueListFilter.valOnlyBuf();

      assertEquals(insertedVals.size(), valueListFilter.getValueCount());
      List<Long> storedVals = new ArrayList<>(valueListFilter.getValueCount());
      IntStream.range(0, valueListFilter.getValueCount()).forEach(i -> storedVals.add(elements.getLong(i * 8)));

      Collections.sort(insertedVals); // expect unique sorted
      assertEquals(insertedVals, storedVals);

      insertedVals.forEach(x -> assertTrue(valueListFilter.mightBePresent(x)));
    }
  }
  private static ArrowBuf writeKey(ArrowBuf keyBuf, int val) {
    keyBuf.setInt(0, val);
    return keyBuf;
  }

  private static ArrowBuf writeKey(ArrowBuf keyBuf, long val) {
    keyBuf.setLong(0, val);
    return keyBuf;
  }
}
