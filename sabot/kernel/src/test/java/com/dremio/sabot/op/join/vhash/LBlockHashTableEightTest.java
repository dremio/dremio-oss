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
package com.dremio.sabot.op.join.vhash;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.util.BloomFilter;
import com.dremio.sabot.op.common.ht2.HashComputation;
import com.dremio.test.AllocatorRule;
import com.koloboke.collect.hash.HashConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Tests for {@link LBlockHashTableEight} */
public class LBlockHashTableEightTest {
  private BufferAllocator testAllocator;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-LBlockHashTableEight", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() {
    testAllocator.close();
  }

  @Test
  public void testPrepareBloomFilter() throws Exception {
    List<AutoCloseable> closeables = new ArrayList<>();
    try (ArrowBuf keyBuf = testAllocator.buffer(9);
        LBlockHashTableEight table =
            new LBlockHashTableEight(HashConfig.getDefault(), testAllocator, 16)) {
      Set<Long> dataSet = generatedData(10);
      dataSet.stream().forEach(key -> table.insert(key, (int) HashComputation.computeHash(key)));

      final Optional<BloomFilter> bloomFilterOptional = table.prepareBloomFilter(false);
      assertTrue(bloomFilterOptional.isPresent());
      closeables.add(bloomFilterOptional.get());
      dataSet.stream()
          .forEach(
              key -> assertTrue(bloomFilterOptional.get().mightContain(writeKey(keyBuf, key), 9)));
      Set<Long> differentData = generatedData(100);
      long fpCount =
          differentData.stream()
              .filter(key -> !dataSet.contains(key))
              .filter(key -> bloomFilterOptional.get().mightContain(writeKey(keyBuf, key), 9))
              .count();
      assertTrue("False positive count is high - " + fpCount, fpCount < 5);

      // test null
      BloomFilter bloomFilter = bloomFilterOptional.get();
      assertFalse(bloomFilter.mightContain(writeNull(keyBuf), 9));
      table.insertNull();
      BloomFilter bloomFilter2 = table.prepareBloomFilter(false).get();
      closeables.add(bloomFilter2);
      assertTrue(bloomFilter2.mightContain(writeNull(keyBuf), 9));
    } finally {
      AutoCloseables.close(closeables);
    }
  }

  private static ArrowBuf writeNull(ArrowBuf keyBuf) {
    keyBuf.writerIndex(0);
    keyBuf.writeByte(0x00);
    keyBuf.writeLong(0);
    return keyBuf;
  }

  private static ArrowBuf writeKey(ArrowBuf keyBuf, long val) {
    keyBuf.writerIndex(0);
    keyBuf.writeByte(0x01);
    keyBuf.writeLong(val);
    return keyBuf;
  }

  private Set<Long> generatedData(int count) {
    Random random = new Random(System.nanoTime());
    Set<Long> randomLongSet = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      long newKey = random.nextLong();
      randomLongSet.add(newKey);
    }
    return randomLongSet;
  }
}
