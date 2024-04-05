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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

public class ArrowInPlaceMergeSorterTest {
  private BufferAllocator testAllocator = new RootAllocator();

  @Test
  public void testSortInts() {
    try (ArrowBuf baseValues = testAllocator.buffer(1024)) {
      Random random = new Random(System.nanoTime());
      Set<Integer> inputVals =
          IntStream.range(0, 100).mapToObj(i -> random.nextInt()).collect(Collectors.toSet());
      baseValues.writerIndex(0);

      inputVals.forEach(baseValues::writeInt);
      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, 4, ArrowCrossBufComparatorProvider.get4ByteNumComparator());
      sorter.sort(0, inputVals.size());

      IntStream.range(1, inputVals.size())
          .forEach(i -> assertTrue(baseValues.getInt(4 * (i - 1)) <= baseValues.getInt(4 * i)));
    }
  }

  @Test
  public void testSortFloat() {
    try (ArrowBuf baseValues = testAllocator.buffer(1024)) {
      Random random = new Random(System.nanoTime());
      Set<Float> inputVals =
          IntStream.range(0, 100).mapToObj(i -> random.nextFloat()).collect(Collectors.toSet());
      baseValues.writerIndex(0);

      inputVals.forEach(baseValues::writeFloat);
      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, 4, ArrowCrossBufComparatorProvider.get4ByteNumComparator());
      sorter.sort(0, inputVals.size());

      IntStream.range(1, inputVals.size())
          .forEach(i -> assertTrue(baseValues.getFloat(4 * (i - 1)) <= baseValues.getFloat(4 * i)));
    }
  }

  @Test
  public void testSortDouble() {
    try (ArrowBuf baseValues = testAllocator.buffer(1024)) {
      Random random = new Random(System.nanoTime());
      Set<Double> inputVals =
          IntStream.range(0, 100).mapToObj(i -> random.nextDouble()).collect(Collectors.toSet());
      baseValues.writerIndex(0);

      inputVals.forEach(baseValues::writeDouble);
      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, 8, ArrowCrossBufComparatorProvider.get8ByteNumComparator());
      sorter.sort(0, inputVals.size());

      IntStream.range(1, inputVals.size())
          .forEach(
              i -> assertTrue(baseValues.getDouble(8 * (i - 1)) <= baseValues.getDouble(8 * i)));
    }
  }

  @Test
  public void testSortShort() {
    try (ArrowBuf baseValues = testAllocator.buffer(1024)) {
      Random random = new Random(System.nanoTime());
      Set<Short> inputVals =
          IntStream.range(0, 100)
              .mapToObj(i -> (short) random.nextInt(Short.MAX_VALUE))
              .collect(Collectors.toSet());
      baseValues.writerIndex(0);

      inputVals.forEach(baseValues::writeShort);
      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, 2, ArrowCrossBufComparatorProvider.get2ByteNumComparator());
      sorter.sort(0, inputVals.size());

      IntStream.range(1, inputVals.size())
          .forEach(i -> assertTrue(baseValues.getShort(2 * (i - 1)) <= baseValues.getShort(2 * i)));
    }
  }

  @Test
  public void testBigInt() {
    try (ArrowBuf baseValues = testAllocator.buffer(2048)) {
      Random random = new Random(System.nanoTime());
      byte[] inputVal = new byte[16];

      List<BigInteger> expectedVals = new ArrayList<>(100);
      for (int i = 0; i < 100; i++) {
        random.nextBytes(inputVal);
        baseValues.setBytes(i * 16, inputVal);
        expectedVals.add(new BigInteger(inputVal));
      }
      Collections.sort(expectedVals);

      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, 16, ArrowCrossBufComparatorProvider.getCustomByteNumComparator(16));
      sorter.sort(0, expectedVals.size());
      List<BigInteger> actualVals = new ArrayList<>(100);
      for (int i = 0; i < 100; i++) {
        baseValues.getBytes(i * 16, inputVal);
        actualVals.add(new BigInteger(inputVal));
      }
      assertEquals(expectedVals, actualVals);
    }
  }

  @Test
  public void testSortDecimals() {
    try (ArrowBuf baseValues = testAllocator.buffer(2048)) {
      Random random = new Random(System.nanoTime());
      byte[] inputVal = new byte[16];

      List<BigDecimal> expectedVals = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
        random.nextBytes(inputVal);
        BigDecimal val = new BigDecimal(new BigInteger(inputVal), 2);
        expectedVals.add(val);
        baseValues.setBytes(i * 16, DecimalUtils.convertBigDecimalToArrowByteArray(val));
      }

      Collections.sort(expectedVals);

      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, 16, ArrowCrossBufComparatorProvider.getDecimalComparator(16));
      sorter.sort(0, expectedVals.size());
      List<BigDecimal> actualVals = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
        byte[] tmp = new byte[16];
        final BigDecimal bigDecimalVal =
            DecimalUtils.getBigDecimalFromLEBytes(baseValues.memoryAddress() + (i * 16), tmp, 2);
        actualVals.add(bigDecimalVal);
      }
      assertEquals(expectedVals, actualVals);
    }
  }

  @Test
  public void testSortString() {
    final int blockSize = 36;
    try (ArrowBuf baseValues = testAllocator.buffer(blockSize * 100)) {
      List<String> inputVals =
          IntStream.range(0, 100)
              .mapToObj(i -> UUID.randomUUID().toString())
              .collect(Collectors.toList());

      for (int i = 0; i < inputVals.size(); i++) {
        byte[] b = inputVals.get(i).getBytes(StandardCharsets.UTF_8);
        byte len = (byte) b.length;
        baseValues.setByte(i * blockSize, len);
        baseValues.setBytes((i * blockSize) + 1, b);
      }

      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, blockSize, new ValueListVarWidthFilterComparator(blockSize));
      sorter.sort(0, inputVals.size());

      for (int i = blockSize; i < blockSize * inputVals.size(); i += blockSize) {
        byte[] val1 = new byte[baseValues.getByte(i - blockSize)];
        baseValues.getBytes(i - blockSize + 1, val1);
        byte[] val2 = new byte[baseValues.getByte(i)];
        baseValues.getBytes(i + 1, val2);

        assertTrue(
            new String(val1, StandardCharsets.UTF_8)
                    .compareTo(new String(val2, StandardCharsets.UTF_8))
                <= 0);
      }
    }
  }

  @Test
  public void testSortSmallStrings() {
    final int blockSize = 16;
    try (ArrowBuf baseValues = testAllocator.buffer(blockSize * 100)) {
      List<String> inputVals =
          IntStream.range(0, 100)
              .mapToObj(i -> UUID.randomUUID().toString().substring(0, 10))
              .collect(Collectors.toList());

      for (int i = 0; i < inputVals.size(); i++) {
        byte[] b = inputVals.get(i).getBytes(StandardCharsets.UTF_8);
        byte len = (byte) b.length;
        baseValues.setByte(i * blockSize, len);
        baseValues.setBytes((i * blockSize) + (blockSize - len), b);
      }

      ArrowInPlaceMergeSorter sorter =
          new ArrowInPlaceMergeSorter(
              baseValues, blockSize, new ValueListVarWidthFilterComparator(blockSize));
      sorter.sort(0, inputVals.size());

      for (int i = blockSize; i < blockSize * inputVals.size(); i += blockSize) {
        int len1 = baseValues.getByte(i - blockSize);
        byte[] val1 = new byte[len1];
        baseValues.getBytes(i - len1, val1);

        int len2 = baseValues.getByte(i);
        byte[] val2 = new byte[len2];
        baseValues.getBytes(i + blockSize - len2, val2);

        assertTrue(
            new String(val1, StandardCharsets.UTF_8)
                    .compareTo(new String(val2, StandardCharsets.UTF_8))
                <= 0);
      }
    }
  }
}
