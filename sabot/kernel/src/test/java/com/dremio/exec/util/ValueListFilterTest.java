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

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables.RollbackCloseable;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.test.AllocatorRule;

/**
 * Tests for {@link ValueListFilter} and {@link ValueListFilterBuilder}
 */
public class ValueListFilterTest {
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
    public void testInsertionWithDuplicates() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 4, false);
             RollbackCloseable closer = new RollbackCloseable();
             ArrowBuf keyBuf = testAllocator.buffer(4)) {
            builder.setup();
            List<Integer> insertedVals = new ArrayList<>(randomIntegers(100));
            insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
            insertedVals.stream().map(val -> builder.insert(writeKey(keyBuf, val))).forEach(Assert::assertFalse);
            builder.insertNull();
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.INT);

            ValueListFilter valueListFilter = builder.build();
            closer.add(valueListFilter);
            ArrowBuf elements = valueListFilter.valOnlyBuf();

            assertEquals(insertedVals.size(), valueListFilter.getValueCount());
            List<Integer> storedVals = new ArrayList<>(valueListFilter.getValueCount());
            IntStream.range(0, valueListFilter.getValueCount()).forEach(i -> storedVals.add(elements.getInt(i * 4)));

            Collections.sort(insertedVals); // expect unique sorted
            assertEquals(insertedVals, storedVals);
            assertTrue(valueListFilter.isContainsNull());
        }
    }

    @Test
    public void testHashCollisions() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 4, false);
             RollbackCloseable closer = new RollbackCloseable();
             ArrowBuf keyBuf = testAllocator.buffer(4)) {
            builder.setup();
            builder.overrideMaxBuckets(4);
            List<Integer> insertedVals = new ArrayList<>(randomIntegers(100));
            insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
            insertedVals.stream().map(val -> builder.insert(writeKey(keyBuf, val))).forEach(Assert::assertFalse);
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.INT);

            ValueListFilter valueListFilter = builder.build();
            closer.add(valueListFilter);
            ArrowBuf elements = valueListFilter.valOnlyBuf();

            List<Integer> storedVals = new ArrayList<>(valueListFilter.getValueCount());
            IntStream.range(0, valueListFilter.getValueCount()).forEach(i -> storedVals.add(elements.getInt(i * 4)));

            Collections.sort(insertedVals); // expect unique sorted
            assertEquals(insertedVals, storedVals);
            assertFalse(valueListFilter.isContainsNull());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCapacityOverflow() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 4, false);
             ArrowBuf keyBuf = testAllocator.buffer(4)) {
            builder.setup();
            Set<Integer> insertedVals = randomIntegers(110);
            insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCapacityOverflow2() throws Exception {
        // Simulate reused buffers by allocating pre-filled buffers.
        final BufferAllocator proxyAllocator = (BufferAllocator) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                new Class<?>[]{BufferAllocator.class},
                (object, method, args) -> {
                    try {
                        if (method.getName().equals("buffer")) {
                            ArrowBuf buf = (ArrowBuf) method.invoke(testAllocator, args);
                            for (int i = 0; i < buf.capacity(); i += 4) {
                                buf.setInt(i, Integer.MAX_VALUE);
                            }
                            return buf;
                        } else {
                            return method.invoke(testAllocator, args);
                        }
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(proxyAllocator, 100, (byte) 4, false);
             ArrowBuf keyBuf = testAllocator.buffer(4)) {
            builder.setup();
            builder.overrideMaxBuckets(1);
            Set<Integer> insertedVals = randomIntegers(110);
            insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
        }
    }

    @Test
    public void testMetaBytesAndBufferReinitialization() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 4, false);
             ArrowBuf keyBuf = testAllocator.buffer(4);
             RollbackCloseable closeables = new RollbackCloseable()) {
            builder.setup();
            builder.overrideMaxBuckets(4);
            List<Integer> insertedVals = new ArrayList<>(randomIntegers(100));
            insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
            insertedVals.stream().map(val -> builder.insert(writeKey(keyBuf, val))).forEach(Assert::assertFalse);
            builder.insertNull();
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.INT);

            ValueListFilter valueListFilter1 = builder.build();
            closeables.add(valueListFilter1);
            assertEquals(TEST_NAME, valueListFilter1.getName());
            assertEquals(Types.MinorType.INT, valueListFilter1.getFieldType());
            assertEquals(4, valueListFilter1.getBlockSize());

            assertTrue(valueListFilter1.isContainsNull());
            assertFalse(valueListFilter1.isBoolField());
            assertTrue(valueListFilter1.isFixedWidth());
            valueListFilter1.writeMetaToBuffer();

            ArrowBuf valueBuf = valueListFilter1.buf();
            ValueListFilter valueListFilter2 = new ValueListFilter(valueBuf);
            valueListFilter2.initializeMetaFromBuffer();
            assertEquals(TEST_NAME, valueListFilter2.getName());
            assertEquals(Types.MinorType.INT, valueListFilter2.getFieldType());
            assertEquals(4, valueListFilter2.getBlockSize());
            assertEquals(insertedVals.size(), valueListFilter2.getValueCount());

            assertTrue(valueListFilter2.isContainsNull());
            assertFalse(valueListFilter2.isBoolField());
            assertTrue(valueListFilter2.isFixedWidth());

            ArrowBuf elements = valueListFilter2.valOnlyBuf();

            List<Integer> storedVals = new ArrayList<>(valueListFilter2.getValueCount());
            IntStream.range(0, valueListFilter2.getValueCount()).forEach(i -> storedVals.add(elements.getInt(i * 4)));

            Collections.sort(insertedVals); // expect unique sorted
            assertEquals(insertedVals, storedVals);
        }
    }

    @Test
    public void testBigIntValueSet() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 8, false);
             RollbackCloseable closer = new RollbackCloseable();
             ArrowBuf keyBuf = testAllocator.buffer(8)) {
            builder.setup();
            List<Long> insertedVals = new ArrayList<>(randomLong(10));
            insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val)));
            insertedVals.stream().map(val -> builder.insert(writeKey(keyBuf, val))).forEach(Assert::assertFalse);
            builder.insertNull();
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
            assertTrue(valueListFilter.isContainsNull());
        }
    }

    @Test
    public void testVarcharValueSet() throws Exception {
        byte blockSize = 16;
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, blockSize, false);
             RollbackCloseable closer = new RollbackCloseable();
             ArrowBuf keyBuf = testAllocator.buffer(blockSize)) {
            builder.setup();

            List<String> insertedVals = new ArrayList<>(randomStrings(100));
            insertedVals.forEach(val -> builder.insert(writeKey(keyBuf, val, blockSize)));
            insertedVals.stream().map(val -> builder.insert(writeKey(keyBuf, val, blockSize))).forEach(Assert::assertFalse);

            builder.insertNull();
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.VARCHAR);
            builder.setFixedWidth(false);

            ValueListFilter valueListFilter = builder.build();
            closer.add(valueListFilter);
            ArrowBuf elements = valueListFilter.valOnlyBuf();

            assertEquals(insertedVals.size(), valueListFilter.getValueCount());
            List<String> storedVals = new ArrayList<>(valueListFilter.getValueCount());

            for (int i=0; i < valueListFilter.getValueCount() * blockSize; i+=blockSize) {
                byte[] val = new byte[elements.getByte(i)];
                elements.getBytes(i + blockSize - val.length, val);
                storedVals.add(new String(val, StandardCharsets.UTF_8));
            }

            Collections.sort(insertedVals); // expect unique sorted
            assertEquals(insertedVals, storedVals);
            assertTrue(valueListFilter.isContainsNull());
        }
    }

    private Set<String> randomStrings(final int size) {
        Random r = new Random(System.nanoTime());
        Set<String> strings = new HashSet<>();
        for (int i = 0; i < size; i++) {
            strings.add(UUID.randomUUID().toString().substring(0, r.nextInt(16)));
        }
        return strings;
    }

    @Test
    public void testCustomDecimal() {
        final byte precision = 38;
        final byte scale = 3;
        final byte blockSize = 16;

        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 1, blockSize, false);
             RollbackCloseable closer = new RollbackCloseable()) {
            builder.setup();
            builder.insertNull();
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.DECIMAL, precision, scale);
            builder.setFixedWidth(false);

            ValueListFilter valueListFilter = builder.build();
            closer.add(valueListFilter);

            ValueListFilter filter2 = ValueListFilterBuilder.fromBuffer(valueListFilter.buf());
            assertTrue(filter2.isContainsNull());
            assertEquals(TEST_NAME, filter2.getName());
            assertEquals(Types.MinorType.DECIMAL, filter2.getFieldType());
            assertEquals(precision, filter2.getPrecision());
            assertEquals(scale, filter2.getScale());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBooleanInNonBooleanValList() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 0, false)) {
            builder.setup();
            builder.insertBooleanVal(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonBooleanInBooleanValList() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 0, true);
             ArrowBuf keyBuf = testAllocator.buffer(8)) {
            builder.setup();
            keyBuf.setLong(0, 1111L);
            builder.insert(keyBuf);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testBooleanAllCombinationsPresent() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 0, true)) {
            builder.setup();
            builder.insertBooleanVal(true);
            builder.insertBooleanVal(false);
            builder.insertNull();
        }
    }

    @Test
    public void testBooleanInsertions1() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 0, true);
             RollbackCloseable closer = new RollbackCloseable()) {
            builder.setup();
            builder.insertBooleanVal(true);
            builder.insertBooleanVal(false);
            // not setting null

            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.BIT);

            ValueListFilter valueListFilter = builder.build();
            closer.add(valueListFilter);

            ValueListFilter copyFilter = new ValueListFilter(valueListFilter.buf());
            copyFilter.initializeMetaFromBuffer();
            assertTrue(copyFilter.isBoolField());
            assertTrue(copyFilter.isContainsFalse());
            assertTrue(copyFilter.isContainsTrue());
            assertFalse(copyFilter.isContainsNull());
        }
    }

    @Test
    public void testBooleanInsertions2() throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 0, true);
             RollbackCloseable closer = new RollbackCloseable()) {
            builder.setup();
            builder.insertBooleanVal(true);
            builder.insertBooleanVal(true);
            builder.insertNull();
            // not setting null

            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.BIT);

            ValueListFilter valueListFilter = builder.build();
            closer.add(valueListFilter);

            ValueListFilter copyFilter = new ValueListFilter(valueListFilter.buf());
            copyFilter.initializeMetaFromBuffer();
            assertTrue(copyFilter.isBoolField());
            assertTrue(copyFilter.isContainsTrue());
            assertTrue(copyFilter.isContainsNull());
            assertFalse(copyFilter.isContainsFalse());
        }
    }

    @Test
    public void testBooleanListMerge() throws Exception {
        try (RollbackCloseable closer = new RollbackCloseable()) {
            ValueListFilter b1 = newBooleanFilter(true, false, false);
            ValueListFilter b2 = newBooleanFilter(false, false, true);
            ValueListFilter b3 = newBooleanFilter(true, false, true);
            ValueListFilter b4 = newBooleanFilter(false, true, false);
            ValueListFilter b5 = newBooleanFilter(true, false, false);
            ValueListFilter merged = new ValueListFilter(testAllocator.buffer(ValueListFilter.META_SIZE));
            closer.addAll(b1, b2, b3, b4, b5, merged);

            ValueListFilter.merge(b1, b2, merged);
            assertFalse(merged.isContainsFalse());
            assertTrue(merged.isContainsNull());
            assertTrue(merged.isContainsTrue());

            ValueListFilter.merge(b2, b3, merged);
            assertTrue(merged.isContainsTrue());
            assertTrue(merged.isContainsNull());
            assertFalse(merged.isContainsFalse());

            ValueListFilter.merge(b4, b5, merged);
            assertTrue(merged.isContainsTrue());
            assertTrue(merged.isContainsFalse());
            assertFalse(merged.isContainsNull());
        }
    }

    @Test (expected = IllegalStateException.class)
    public void testBooleanListOverflowMerge() throws Exception {
        try (RollbackCloseable closer = new RollbackCloseable()) {
            ValueListFilter b1 = newBooleanFilter(true, true, false);
            ValueListFilter b2 = newBooleanFilter(false, false, true);
            ValueListFilter merged = new ValueListFilter(testAllocator.buffer(ValueListFilter.META_SIZE));
            closer.addAll(b1, b2, merged);
            ValueListFilter.merge(b1, b2, merged);
        }
    }

    private ValueListFilter newBooleanFilter(boolean containsTrue, boolean containsFalse, boolean containsNull) throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, 100, (byte) 0, true)) {
            builder.setup();
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.BIT);
            if (containsTrue) {
                builder.insertBooleanVal(true);
            }
            if (containsFalse) {
                builder.insertBooleanVal(false);
            }
            if (containsNull) {
                builder.insertNull();
            }
            return builder.build();
        }
    }

    @Test
    public void testMergeBigIntWithDuplicates() throws Exception {
        final Set<Long> set1 = randomLong(10);
        final Set<Long> set2 = randomLong(10);
        final Set<Long> common = randomLong(10);
        try (ValueListFilter valueListFilter1 = toValListFilterLong(set1, common);
             ValueListFilter valueListFilter2 = toValListFilterLong(set2, common);
             ValueListFilter mergedValList = new ValueListFilter(testAllocator.buffer(4096))) {

            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);

            assertEquals(valueListFilter1.getName(), mergedValList.getName());
            assertEquals(valueListFilter1.getBlockSize(), mergedValList.getBlockSize());
            assertFalse(mergedValList.isContainsNull());

            List<Long> expectedVals = Stream.concat(set1.stream(), Stream.concat(set2.stream(), common.stream()))
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(mergedValList.getValueCount(), expectedVals.size());

            List<Long> actualVals = new ArrayList<>(mergedValList.getValueCount());
            IntStream.range(0, mergedValList.getValueCount()).forEach(i -> actualVals.add(mergedValList.valOnlyBuf().getLong(i * 8)));
            assertEquals(expectedVals, actualVals);
        }
    }

    @Test
    public void testMergeVarcharWithDuplicates() throws Exception {
        final Set<String> set1 = randomStrings(10);
        final Set<String> set2 = randomStrings(10);
        final Set<String> common = randomStrings(10);
        try (ValueListFilter valueListFilter1 = toValListFilterString((byte)16, set1, common);
             ValueListFilter valueListFilter2 = toValListFilterString((byte)16, set2, common);
             ValueListFilter mergedValList = new ValueListFilter(testAllocator.buffer(4096))) {

            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);

            assertEquals(valueListFilter1.getName(), mergedValList.getName());
            assertEquals(valueListFilter1.getBlockSize(), mergedValList.getBlockSize());
            assertFalse(mergedValList.isContainsNull());

            List<String> expectedVals = Stream.concat(set1.stream(), Stream.concat(set2.stream(), common.stream()))
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(mergedValList.getValueCount(), expectedVals.size());

            List<String> actualVals = new ArrayList<>(mergedValList.getValueCount());
            for (int i=0; i < mergedValList.getValueCount() * 16; i+=16) {
                byte[] val = new byte[mergedValList.valOnlyBuf().getByte(i)];
                mergedValList.valOnlyBuf().getBytes(i + 16 - val.length, val);
                actualVals.add(new String(val, StandardCharsets.UTF_8));
            }
            assertEquals(expectedVals, actualVals);
        }
    }

    @Test
    public void testMergeContainsNull() throws Exception {
        try (ValueListFilter valueListFilter1 = toValListFilterLong(randomLong(10));
             ValueListFilter valueListFilter2 = toValListFilterLong(randomLong(10));
             ValueListFilter mergedValList = new ValueListFilter(testAllocator.buffer(1024))) {

            valueListFilter1.setContainsNull(true);
            valueListFilter2.setContainsNull(false);
            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);
            assertTrue(mergedValList.isContainsNull());

            valueListFilter1.setContainsNull(false);
            valueListFilter2.setContainsNull(false);
            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);
            assertFalse(mergedValList.isContainsNull());

            valueListFilter1.setContainsNull(false);
            valueListFilter2.setContainsNull(true);
            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);
            assertTrue(mergedValList.isContainsNull());

            valueListFilter1.setContainsNull(true);
            valueListFilter2.setContainsNull(true);
            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);
            assertTrue(mergedValList.isContainsNull());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testMergeValueOverflow1() throws Exception {
        try (ValueListFilter valueListFilter1 = toValListFilterLong(randomLong(65));
             ValueListFilter valueListFilter2 = toValListFilterLong(randomLong(65));
             ValueListFilter mergedValList = new ValueListFilter(testAllocator.buffer(1024))) {

            // Max entries possible are 124, inserting 130 distinct elements
            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeIncompatibleBlockSize() throws Exception {
        try (ValueListFilter valueListFilter1 = toValListFilterLong(randomLong(10));
             ValueListFilter valueListFilter2 = toValListFilterInt(randomIntegers(10));
             ValueListFilter mergedValList = new ValueListFilter(testAllocator.buffer(1024))) {

            // Max entries possible are 124, inserting 130 distinct elements
            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeIncompatibleFieldType() throws Exception {
        try (ValueListFilter valueListFilter1 = toValListFilterLong(randomLong(65));
             ValueListFilter valueListFilter2 = toValListFilterLong(randomLong(65));
             ValueListFilter mergedValList = new ValueListFilter(testAllocator.buffer(1024))) {

            valueListFilter1.setFieldType(Types.MinorType.DATEMILLI, (byte) 0, (byte) 0);
            // Max entries possible are 124, inserting 130 distinct elements
            ValueListFilter.merge(valueListFilter1, valueListFilter2, mergedValList);
        }
    }

    @SafeVarargs
    private final ValueListFilter toValListFilterLong(Set<Long>... vals) throws Exception {
        Set<Long> allValues = Arrays.stream(vals).flatMap(Set::stream).collect(Collectors.toSet());
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, allValues.size(), (byte) 8, false);
             ArrowBuf keyBuf = testAllocator.buffer(8)) {
            builder.setup();
            allValues.forEach(val -> builder.insert(writeKey(keyBuf, val)));
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.BIGINT);
            return builder.build();
        }
    }

    @SafeVarargs
    private final ValueListFilter toValListFilterString(byte blockSize, Set<String>... vals) throws Exception {
        Set<String> allValues = Arrays.stream(vals).flatMap(Set::stream).collect(Collectors.toSet());
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, allValues.size(), blockSize, false);
             ArrowBuf keyBuf = testAllocator.buffer(blockSize)) {
            builder.setup();
            allValues.forEach(val -> builder.insert(writeKey(keyBuf, val, blockSize)));
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.VARCHAR);
            builder.setFixedWidth(false);
            return builder.build();
        }
    }

    @SafeVarargs
    private final ValueListFilter toValListFilterInt(Set<Integer>... vals) throws Exception {
        Set<Integer> allValues = Arrays.stream(vals).flatMap(Set::stream).collect(Collectors.toSet());
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, allValues.size(), (byte) 4, false);
             ArrowBuf keyBuf = testAllocator.buffer(4)) {
            builder.setup();
            allValues.forEach(val -> builder.insert(writeKey(keyBuf, val)));
            builder.setName(TEST_NAME);
            builder.setFieldType(Types.MinorType.INT);
            return builder.build();
        }
    }

    // TODO: Tests for all other data types

    private static ArrowBuf writeKey(ArrowBuf keyBuf, int val) {
        keyBuf.setInt(0, val);
        return keyBuf;
    }

    private static ArrowBuf writeKey(ArrowBuf keyBuf, long val) {
        keyBuf.setLong(0, val);
        return keyBuf;
    }

    private static ArrowBuf writeKey(ArrowBuf keyBuf, String val, int blockSize) {
        keyBuf.setBytes(0, new byte[blockSize]);
        byte[] valBytes = val.getBytes(StandardCharsets.UTF_8);
        keyBuf.setByte(0, valBytes.length);
        keyBuf.setBytes(blockSize - valBytes.length, valBytes);
        return keyBuf;
    }

    private Set<Integer> randomIntegers(int count) {
        Random random = new Random(System.nanoTime());
        return IntStream.range(0, count).map(i -> random.nextInt()).boxed().collect(Collectors.toSet());
    }

    private Set<Long> randomLong(int count) {
        Random random = new Random(System.nanoTime());
        return IntStream.range(0, count).mapToLong(i -> random.nextLong()).boxed().collect(Collectors.toSet());
    }
}
