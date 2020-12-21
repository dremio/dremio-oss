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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.test.AllocatorRule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

/**
 * Tests for {@link BloomFilter}
 */
public class BloomFilterTest {
    private BufferAllocator bfTestAllocator;
    private final static String TEST_NAME = "20ed4177-87c7-91cc-c869-82b1d90cd300:frag:1:3";

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        bfTestAllocator = allocatorRule.newAllocator("test-bloomfilter", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        bfTestAllocator.close();
    }

    @Test
    public void testFilterInt() {
        /*
         * Create two distinct sets. Insert first to bloomfilter. Ensure no false negatives while matching first set and
         * tolerable false positives while matching second set.
         */
        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(4);
             final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 544)) {
            bloomFilter.setup();
            Set<Integer> keySet1 = randomIntegers(100);
            putAllIntKeys(bloomFilter, keyBuf, keySet1);

            Set<Integer> keySet2 = randomIntegers(99);
            keySet2.removeAll(keySet1); // ensure all are non-existing keys

            // Assert FPP < 5%
            int maxPermissibleErrors = (int) (0.05 * keySet2.size());
            long errCount = keySet2.stream().map(k -> writeKey(keyBuf, k)).filter(k -> bloomFilter.mightContain(k, 4)).count();
            assertTrue("False positivity is higher than expected. Total errors: " + errCount + ", max permissible: " + maxPermissibleErrors,
                    errCount <= maxPermissibleErrors);

            // Assert no false negatives
            keySet1.stream().map(k -> writeKey(keyBuf, k)).forEach(k -> assertTrue(bloomFilter.mightContain(k, 4)));
        }
    }

    @Test
    public void testFilterStrings() {
        /*
         * Create two distinct sets. Insert first to bloomfilter. Ensure no false negatives while matching first set and
         * tolerable false positives while matching second set.
         */
        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(36);
             final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 544)) {
            bloomFilter.setup();

            Set<String> keySet1 = randomStrings(100);
            putAllStringKeys(bloomFilter, keyBuf, keySet1);

            Set<String> keySet2 = randomStrings(1000);
            keySet2.removeAll(keySet1); // ensure all are non-existing keys

            // Assert FPP < 5%
            int maxPermissibleErrors = (int) (0.05 * keySet2.size());
            long errCount = keySet2.stream().map(k -> writeKey(keyBuf, k)).filter(key -> bloomFilter.mightContain(key, 36)).count();
            assertTrue(errCount <= maxPermissibleErrors);

            // Assert no false negatives
            keySet1.stream().map(k -> writeKey(keyBuf, k)).forEach(key -> assertTrue(bloomFilter.mightContain(key, 36)));
        }
    }

    @Test
    public void testDuplicateKeyIdentification() {
        // Bloomfilter.put(..) should return false if it finds a key repetitive. Try to re-insert keyset and ensure it returns false.
        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(4);
             final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 544)) {
            bloomFilter.setup();
            Set<Integer> keySet1 = randomIntegers(100);
            keySet1.stream().map(k -> writeKey(keyBuf, k)).forEach(key -> bloomFilter.put(key, 4));
            keySet1.stream().map(k -> writeKey(keyBuf, k)).forEach(key -> assertFalse(bloomFilter.put(key, 4)));
        }
    }

    @Test
    public void testMergeFilters() {
        // Create two keysets and two filters from each one respectively. Merged bloomfilter should match all entries from merged keyset.
        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(4);
             final BloomFilter bloomFilter1 = new BloomFilter(bfTestAllocator, TEST_NAME, 544);
             final BloomFilter bloomFilter2 = new BloomFilter(bfTestAllocator, TEST_NAME, 544)) {
            bloomFilter1.setup();
            bloomFilter2.setup();

            Set<Integer> keySet1 = randomIntegers(100);
            putAllIntKeys(bloomFilter1, keyBuf, keySet1);
            long initialNumBitsSet = bloomFilter1.getNumBitsSet();

            Set<Integer> keySet2 = randomIntegers(100);
            putAllIntKeys(bloomFilter2, keyBuf, keySet2);

            bloomFilter1.merge(bloomFilter2);
            long mergedNumBitsSet = bloomFilter1.getNumBitsSet();

            Set<Integer> allInsertedKeys = new HashSet<>();
            allInsertedKeys.addAll(keySet1);
            allInsertedKeys.addAll(keySet2);
            allInsertedKeys.stream().map(k -> writeKey(keyBuf, k)).forEach(key -> assertTrue(bloomFilter1.mightContain(key, 4)));

            assertTrue("Merged filter should have more bits set.", mergedNumBitsSet > initialNumBitsSet);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeInvalidSameFilter() {
        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(4);
             final BloomFilter bloomFilter1 = new BloomFilter(bfTestAllocator, TEST_NAME, 544)) {
            bloomFilter1.setup();

            Set<Integer> keySet1 = randomIntegers(100);
            putAllIntKeys(bloomFilter1, keyBuf, keySet1);

            bloomFilter1.merge(bloomFilter1);
            fail("Expected failure during bloomfilter merge");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeDifferentSizedFilters() {
        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(4);
             final BloomFilter bloomFilter1 = new BloomFilter(bfTestAllocator, TEST_NAME, 544);
             final BloomFilter bloomFilter2 = new BloomFilter(bfTestAllocator, TEST_NAME, 1056)) {
            bloomFilter1.setup();
            bloomFilter2.setup();

            Set<Integer> keySet1 = randomIntegers(100);
            putAllIntKeys(bloomFilter1, keyBuf, keySet1);

            Set<Integer> keySet2 = randomIntegers(100);
            putAllIntKeys(bloomFilter2, keyBuf, keySet2);

            bloomFilter1.merge(bloomFilter2);
            fail("Expected failure during bloomfilter merge");
        }
    }

    @Test
    public void testFppForOptimalInsertions() {
        final long twoMBWithMeta = (2 * 1024 * 1024);

        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(36);
             final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, twoMBWithMeta)) {
            bloomFilter.setup();

            int optimalNoOfInsertions = (int) bloomFilter.getOptimalInsertions();
            assertEquals(1_750_324, optimalNoOfInsertions);  // (-n * (log(2) * log(2))/log(p)) [n=size, p=err probability]

            Set<String> keySet1 = randomStrings(optimalNoOfInsertions);
            putAllStringKeys(bloomFilter, keyBuf, keySet1);

            Set<String> keySet2 = randomStrings(1_000_000);
            keySet2.removeAll(keySet1); // ensure all are non-existing keys

            // Assert FPP < 5%
            int maxPermissibleErrors = (int) (0.05 * keySet2.size());
            long errCount = keySet2.stream().map(k -> writeKey(keyBuf, k)).filter(key -> bloomFilter.mightContain(key, 36)).count();
            assertTrue("False positivity is higher than expected. Total errors: " + errCount, errCount <= maxPermissibleErrors);

            double actualFpp = (double) errCount / (double) keySet2.size();
            double estimatedFpp = bloomFilter.getExpectedFPP();
            assertTrue(String.format("Estimated FPP %f is far lower from the actual FPP %f .", estimatedFpp, actualFpp), (actualFpp - estimatedFpp) < 0.01);
        }
    }

    @Test
    public void testSerDe() {
        try (final ArrowBuf keyBuf = bfTestAllocator.buffer(36);
             final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 1024)) {
            bloomFilter.setup();
            Set<String> keySet1 = randomStrings(100);
            putAllStringKeys(bloomFilter, keyBuf, keySet1);

            ByteBuf deser = NettyArrowBuf.unwrapBuffer(bloomFilter.getDataBuffer());
            assertEquals("Reader index not set to zero", 0, deser.readerIndex());
            assertEquals("Writer index not set to capacity", deser.capacity(), deser.writerIndex());

            BloomFilter deserializedFilter = BloomFilter.prepareFrom(((NettyArrowBuf) deser).arrowBuf());
            assertEquals(bloomFilter.getNumBitsSet(), deserializedFilter.getNumBitsSet());
            assertEquals(bloomFilter.getName(), deserializedFilter.getName());
            assertEquals(bloomFilter.getSizeInBytes(), deserializedFilter.getSizeInBytes());

            // Run value test on deserialised version -
            Set<String> keySet2 = randomStrings(1000);
            keySet2.removeAll(keySet1); // ensure all are non-existing keys
            int maxPermissibleErrors = (int) (0.05 * keySet2.size());
            long errCount = keySet2.stream().map(k -> writeKey(keyBuf, k)).filter(key -> deserializedFilter.mightContain(key, 36)).count();
            assertTrue(errCount <= maxPermissibleErrors);

            // Assert no false negatives
            keySet1.stream().map(k -> writeKey(keyBuf, k)).forEach(key -> assertTrue(deserializedFilter.mightContain(key, 36)));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSizeSmallerThanMeta() {
        try (final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 24)) {
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSizeNotMultipleOf8() {
        try (final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 31)) {
        }
    }

    @Test
    public void testSetup() {
        try (final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 40)) {
            bloomFilter.setup();
            assertEquals(bloomFilter.getDataBuffer().capacity(), bloomFilter.getSizeInBytes());

            String expectedName = TEST_NAME.substring(TEST_NAME.length() - 24);
            assertEquals("BoomFilter.getName() is incorrect", expectedName, bloomFilter.getName());
            byte[] nameBytes = new byte[24];
            bloomFilter.getDataBuffer().getBytes(bloomFilter.getSizeInBytes() - 32, nameBytes);
            assertEquals("Name in meta bytes not set correctly.", expectedName, new String(nameBytes, StandardCharsets.UTF_8));

            assertEquals("Reader index not set correctly", 0, bloomFilter.getDataBuffer().readerIndex());
            assertEquals("Writer index not set correctly", bloomFilter.getSizeInBytes(), bloomFilter.getDataBuffer().writerIndex());

            for (long i = 0; i < bloomFilter.getSizeInBytes() - 32; i += 8) {
                long block = bloomFilter.getDataBuffer().getLong(i);
                assertEquals("Found unclean buffer state", 0L, block);
            }
        }
    }

    @Test
    public void testIsCrossingMaxFpp() {
        try (final BloomFilter bloomFilter = new BloomFilter(bfTestAllocator, TEST_NAME, 64);
             final ArrowBuf keyBuf = bfTestAllocator.buffer(36)) {
            bloomFilter.setup();

            for (int i = 0; i < 1_000_000; i++) {
                bloomFilter.put(writeKey(keyBuf, UUID.randomUUID().toString()), 36);
                if (bloomFilter.getExpectedFPP() > 0.05) {
                    break;
                }
            }

            assertTrue(bloomFilter.isCrossingMaxFPP());
        }
    }

    @Test
    public void testGetOptimalSize() {
        assertEquals(40, BloomFilter.getOptimalSize(1));
        assertEquals(40, BloomFilter.getOptimalSize(4));
        assertEquals(152, BloomFilter.getOptimalSize(100));
        assertEquals(1_232, BloomFilter.getOptimalSize(1_000));
        assertEquals(1_198_168, BloomFilter.getOptimalSize(1_000_000));
        assertEquals(1_198_132_336, BloomFilter.getOptimalSize(1_000_000_000));
    }

    @Test
    public void testClose() {
        try (final BloomFilter f1 = new BloomFilter(bfTestAllocator, TEST_NAME, 64)) {
            f1.setup();

            f1.getDataBuffer().retain();
            assertEquals(2, f1.getDataBuffer().refCnt());
            f1.close();
            assertEquals(1, f1.getDataBuffer().refCnt());
        }
    }

    private Set<Integer> randomIntegers(int count) {
        Random random = new Random(System.nanoTime());
        Set<Integer> randomIntegerSet = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            int newKey = random.nextInt();
            randomIntegerSet.add(newKey);
        }
        return randomIntegerSet;
    }

    private Set<String> randomStrings(int count) {
        Set<String> randomStringsSet = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            randomStringsSet.add(UUID.randomUUID().toString());
        }
        return randomStringsSet;
    }

    private static ArrowBuf writeKey(ArrowBuf keyBuf, int val) {
        keyBuf.writerIndex(0);
        keyBuf.writeInt(val);
        return keyBuf;
    }

    private static ArrowBuf writeKey(ArrowBuf keyBuf, String val) {
        keyBuf.writerIndex(0);
        keyBuf.writeBytes(val.getBytes(StandardCharsets.UTF_8));
        return keyBuf;
    }

    private static void putAllIntKeys(BloomFilter bloomFilter, ArrowBuf keyBuf, Set<Integer> keySet) {
        keySet.stream().map(k -> writeKey(keyBuf, k)).forEach(key -> bloomFilter.put(key, 4));
    }

    private static void putAllStringKeys(BloomFilter bloomFilter, ArrowBuf keyBuf, Set<String> keySet) {
        keySet.stream().map(k -> writeKey(keyBuf, k)).forEach(key -> bloomFilter.put(key, 36));
    }
}
