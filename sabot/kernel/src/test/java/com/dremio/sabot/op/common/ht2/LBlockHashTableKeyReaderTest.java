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

package com.dremio.sabot.op.common.ht2;

import static com.dremio.exec.util.TestdataSetupUtils.newHashMap;
import static com.dremio.exec.util.TestdataSetupUtils.randomStrings;
import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.ByteArrayUtil;
import com.dremio.exec.util.KeyFairSliceCalculator;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Tests for {@link HashTableKeyReader for LBlockHashTable} */
public class LBlockHashTableKeyReaderTest {
  private BufferAllocator allocator;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    allocator = allocatorRule.newAllocator("test-lbockhashtablekeyreadertest", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() {
    allocator.close();
  }

  @Test
  public void testSingleIntKeyLoad() throws Exception {
    List<Integer> keysToFeed = Arrays.asList(1111, 2222, null, -4444);
    try (FieldVector intField = new IntVector("intfield", allocator);
        ArrowBuf tableData = allocator.buffer(32)) {
      // Feed keys at the address
      long startAddress = tableData.memoryAddress();
      for (Integer key : keysToFeed) {
        // validity
        PlatformDependent.putInt(startAddress, key == null ? 0 : 1);
        // value
        PlatformDependent.putInt(startAddress + 4, key == null ? 0 : key);
        startAddress += 8;
      }

      PivotDef pivot = buildPivot(intField);
      List<String> fieldsToRead = Lists.newArrayList("intfield");
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        assertEquals("Invalid keybuf size", 5, reader.getKeyBufSize());
        assertFalse("Keys not trimmed", reader.isKeyTrimmedToFitSize());

        long tableFixedAddress = tableData.memoryAddress();
        for (Integer key : keysToFeed) {
          reader.loadNextKey(tableFixedAddress, 0);
          assertEquals(
              "validity byte mismatched",
              (byte) ((key == null) ? 0 : 1),
              reader.getKeyHolder().getByte(0));
          assertEquals("Key mismatched", (key == null) ? 0 : key, reader.getKeyHolder().getInt(1));
          tableFixedAddress += pivot.getBlockWidth();
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComplexKeyLoad1() throws Exception {
    final FieldType complexStruct =
        CompleteType.struct(CompleteType.INT.toField("int"))
            .toField("complexStruct")
            .getFieldType();
    try (FieldVector structfield =
        new StructVector("structfield", allocator, complexStruct, null)) {
      VectorPivotDef v =
          new VectorPivotDef(
              PivotBuilder.FieldType.FOUR_BYTE,
              0,
              0,
              0,
              new FieldVectorPair(structfield, structfield));

      PivotDef pivot = new PivotDef(4, 0, 0, ImmutableList.of(v));
      List<String> fieldsToRead = Lists.newArrayList("structfield");
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        // trigger exception
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComplexKeyLoad2() throws Exception {
    final FieldType complexStruct =
        CompleteType.struct(CompleteType.VARCHAR.toField("varchar"))
            .toField("complexStruct")
            .getFieldType();
    try (FieldVector structfield = new StructVector("structfield", allocator, complexStruct, null);
        ArrowBuf tableData = allocator.buffer(32)) {

      long[] tableFixedAddresses = {tableData.memoryAddress()};
      long[] tableVarAddresses = {tableData.memoryAddress()};
      VectorPivotDef v =
          new VectorPivotDef(
              PivotBuilder.FieldType.VARIABLE,
              0,
              0,
              0,
              new FieldVectorPair(structfield, structfield));

      PivotDef pivot = new PivotDef(-1, 1, 0, ImmutableList.of(v));
      List<String> fieldsToRead = Lists.newArrayList("structfield");
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        // trigger exception
      }
    }
  }

  @Test
  public void testSingleBooleanKeyLoad() throws Exception {
    List<Boolean> keysToFeed = Arrays.asList(true, false, null, true);
    try (FieldVector bitVector = new BitVector("bitField", allocator);
        ArrowBuf tableData = allocator.buffer(4 * 4)) {
      // Feed keys at the address
      long startAddress = tableData.memoryAddress();
      for (Boolean key : keysToFeed) {
        if (key == null) {
          PlatformDependent.putInt(startAddress, 0);
        } else {
          int val = 1; // validity
          val |= (key ? 1 : 0) << 1; // value
          PlatformDependent.putInt(startAddress, val);
        }
        startAddress += 4;
      }

      PivotDef pivot = buildPivot(bitVector);
      List<String> fieldsToRead = Lists.newArrayList("bitField");
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        assertEquals("Invalid keybuf size", 1, reader.getKeyBufSize());
        assertFalse("Keys not trimmed", reader.isKeyTrimmedToFitSize());

        long tableFixedAddress = tableData.memoryAddress();
        for (Boolean key : keysToFeed) {
          reader.loadNextKey(tableFixedAddress, 0);
          int val;
          if (key == null) {
            val = 0;
          } else {
            val = 1; // validity
            val |= (key ? 1 : 0) << 1; // value
            assertEquals(
                "validity and value bits mismatched", (byte) val, reader.getKeyHolder().getByte(0));
          }
          tableFixedAddress += pivot.getBlockWidth();
        }
      }
    }
  }

  @Test
  public void testCompositeKeyLoadReverseOrderDifferentTypes() throws Exception {
    // Composite key made of int, long and double. While reading, we expected a reverse order.
    List<Integer> intKeysToFeed = Arrays.asList(1111, null, 3333, -4444);
    List<Long> longKeysToFeed = Arrays.asList(101010L, 202020L, null, -404040L);
    List<Double> doubleKeysToFeed = Arrays.asList(1.02321, 4.12321, 6.11312, null);

    try (FieldVector intField = new IntVector("intfield", allocator);
        FieldVector longField = new BigIntVector("longfield", allocator);
        FieldVector doubleField = new Float8Vector("doublefield", allocator);
        // Block size = 4(validity) + 4(intkey) + 8(longkey) + 8(doublekey) = 24
        // Total space required = block size * num of entries in keyset = 24 * 4 = 96
        ArrowBuf tableData = allocator.buffer(96)) {

      // Feed keys at the address
      long startAddress = tableData.memoryAddress();
      for (int keyIdx = 0; keyIdx < intKeysToFeed.size(); keyIdx++) {
        int validity = 0;
        validity |= ((intKeysToFeed.get(keyIdx) == null) ? 0 : 1);
        validity |= ((longKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 1;
        validity |= ((doubleKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 2;
        PlatformDependent.putInt(startAddress, validity);
        PlatformDependent.putInt(
            startAddress + 4, intKeysToFeed.get(keyIdx) == null ? 0 : intKeysToFeed.get(keyIdx));
        PlatformDependent.putLong(
            startAddress + 8, longKeysToFeed.get(keyIdx) == null ? 0 : longKeysToFeed.get(keyIdx));
        copyDoubleToAddress(
            startAddress + 16,
            doubleKeysToFeed.get(keyIdx) == null ? 0 : doubleKeysToFeed.get(keyIdx));
        startAddress += 24;
      }

      Map<String, Integer> keySizeMap = newHashMap("intfield", 4, "longfield", 8, "doublefield", 8);
      KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(keySizeMap, 32);

      PivotDef pivot = buildPivot(intField, longField, doubleField);

      // Read and verify it is same as the ones fed earlier
      List<String> fieldsToRead =
          Lists.newArrayList("doublefield", "longfield", "intfield"); // order is reversed
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot);
          ArrowBuf expectedKey = allocator.buffer(21)) {

        ArrowBuf key = reader.getKeyHolder();
        assertEquals("Invalid keybuf size", 21, reader.getKeyBufSize());
        assertFalse("Keys trimmed", reader.isKeyTrimmedToFitSize());
        long tableFixedAddress = tableData.memoryAddress();
        for (int keyIdx = 0; keyIdx < intKeysToFeed.size(); keyIdx++) {
          int nextKeyPos = 0;
          reader.loadNextKey(tableFixedAddress, 0);
          tableFixedAddress += pivot.getBlockWidth();

          int validity = 0;
          validity |= ((doubleKeysToFeed.get(keyIdx) == null) ? 0 : 1);
          validity |= ((longKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 1;
          validity |= ((intKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 2;
          expectedKey.setByte(nextKeyPos, (byte) validity);
          nextKeyPos += 1;

          expectedKey.setDouble(
              nextKeyPos, doubleKeysToFeed.get(keyIdx) == null ? 0 : doubleKeysToFeed.get(keyIdx));
          nextKeyPos += keyFairSliceCalculator.getKeySlice("doublefield");

          expectedKey.setLong(
              nextKeyPos, longKeysToFeed.get(keyIdx) == null ? 0 : longKeysToFeed.get(keyIdx));
          nextKeyPos += keyFairSliceCalculator.getKeySlice("longfield");

          expectedKey.setInt(
              nextKeyPos, intKeysToFeed.get(keyIdx) == null ? 0 : intKeysToFeed.get(keyIdx));

          byte[] expectedBytes = new byte[21];
          expectedKey.getBytes(0, expectedBytes);
          byte[] actualBytes = new byte[21];
          key.getBytes(0, actualBytes);
          assertTrue("Key mismatched", Arrays.equals(expectedBytes, actualBytes));
        }
      }
    }
  }

  @Test
  public void testCompositeKeySubsetLoad() throws Exception {
    // Composite key made of int, long and double. Read only intfield and doublefield, dropping
    // longfield.
    List<Integer> intKeysToFeed = Arrays.asList(1111, 2222, null, -4444);
    List<Long> longKeysToFeed = Arrays.asList(101010L, 202020L, 303030L, null);
    List<Double> doubleKeysToFeed = Arrays.asList(1.02321, null, 6.11312, -1.23424);

    try (FieldVector intField = new IntVector("intfield", allocator);
        FieldVector longField = new BigIntVector("longfield", allocator);
        FieldVector doubleField = new Float8Vector("doublefield", allocator);
        // Block size = 4(validity) + 4(intkey) + 8(longkey) + 8(doublekey) = 24
        // Total space required = block size * num of entries in keyset = 24 * 4 = 96
        ArrowBuf tableData = allocator.buffer(96)) {

      // Feed keys at the address
      long startAddress = tableData.memoryAddress();
      for (int keyIdx = 0; keyIdx < intKeysToFeed.size(); keyIdx++) {
        int validity = 0;
        validity |= ((intKeysToFeed.get(keyIdx) == null) ? 0 : 1);
        validity |= ((longKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 1;
        validity |= ((doubleKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 2;
        PlatformDependent.putInt(startAddress, validity);
        PlatformDependent.putInt(
            startAddress + 4, intKeysToFeed.get(keyIdx) == null ? 0 : intKeysToFeed.get(keyIdx));
        PlatformDependent.putLong(
            startAddress + 8, longKeysToFeed.get(keyIdx) == null ? 0 : longKeysToFeed.get(keyIdx));
        copyDoubleToAddress(
            startAddress + 16,
            doubleKeysToFeed.get(keyIdx) == null ? 0 : doubleKeysToFeed.get(keyIdx));
        startAddress += 24;
      }

      PivotDef pivot = buildPivot(intField, longField, doubleField);

      // Read and verify it is same as the ones fed earlier
      List<String> fieldsToRead =
          Lists.newArrayList("intfield", "doublefield"); // dropping longfield
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        ArrowBuf key = reader.getKeyHolder();
        // Not expecting longfield value, hence total size is 1(validity) + 4(int) + 8(double) = 13
        assertEquals("Invalid keybuf size", 13, reader.getKeyBufSize());
        long tableFixedAddress = tableData.memoryAddress();
        for (int keyIdx = 0; keyIdx < intKeysToFeed.size(); keyIdx++) {
          reader.loadNextKey(tableFixedAddress, 0);
          tableFixedAddress += pivot.getBlockWidth();

          int validity = 0;
          validity |= ((intKeysToFeed.get(keyIdx) == null) ? 0 : 1);
          validity |= ((doubleKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 1;
          assertEquals("Validity mismatched", (byte) validity, key.getByte(0));
          assertEquals(
              "Int key mismatched",
              intKeysToFeed.get(keyIdx) == null ? 0 : intKeysToFeed.get(keyIdx).intValue(),
              key.getInt(1));
          assertEquals(
              "Double key mismatched",
              String.valueOf(
                  doubleKeysToFeed.get(keyIdx) == null ? 0 : doubleKeysToFeed.get(keyIdx)),
              String.valueOf(reader.getKeyHolder().getDouble(5)));
        }
      }
    }
  }

  @Test
  public void testSingleStringKeyLoad() throws Exception {
    List<String> keysToFeed =
        Arrays.asList(
            "61a20b7d-4f5c-493a-acaf-b6d4be4a094a",
            "short1",
            "8488fa8d-7f34-4152-97a8-c54292c74340",
            null,
            "535e0790-c851-4b1d-a0bb-74ed796825be");
    try (FieldVector stringField = new VarCharVector("stringfield", allocator);
        ArrowBuf tableData = setupTableData(keysToFeed);
        ArrowBuf fixedBlocks = allocator.buffer(8 * keysToFeed.size())) {

      long startAddress = fixedBlocks.memoryAddress();
      for (int i = 0; i < keysToFeed.size(); ++i, startAddress += 8) {
        PlatformDependent.putInt(startAddress, keysToFeed.get(i) == null ? 0 : 1);
      }

      PivotDef pivot = buildPivot(stringField);
      List<String> fieldsToRead = Lists.newArrayList("stringfield");
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        assertEquals("Invalid keybuf size", 32, reader.getKeyBufSize());
        assertTrue("Keys not trimmed", reader.isKeyTrimmedToFitSize());
        long tableVarAddress = tableData.memoryAddress();
        int currentVarOffset = 0;
        long tableFixedAddress = fixedBlocks.memoryAddress();
        byte[] keyBytes = new byte[32];
        for (String key : keysToFeed) {
          reader.loadNextKey(tableFixedAddress, tableVarAddress + currentVarOffset);
          tableFixedAddress += pivot.getBlockWidth();
          currentVarOffset += PlatformDependent.getInt(tableVarAddress + currentVarOffset) + 4;
          byte[] expectedKey = new byte[32];
          if (key != null) {
            expectedKey[0] = (byte) 1;
            byte[] stringBytes =
                key.length() <= 31
                    ? padZeroBytes(key.getBytes(StandardCharsets.UTF_8), 31)
                    : key.substring(0, 31).getBytes(StandardCharsets.UTF_8);
            System.arraycopy(stringBytes, 0, expectedKey, 1, 31);
          }
          reader.getKeyHolder().getBytes(0, keyBytes);
          assertTrue("Key mismatched", Arrays.equals(expectedKey, keyBytes));
        }
      }
    }
  }

  @Test
  public void testSingleStringWithLen() throws Exception {
    List<String> keysToFeed =
        Arrays.asList(
            "61a20b7d-4f5c-493a-acaf-b6d4be4a094a",
            "short1",
            "8488fa8d-7f34-4152-97a8-c54292c74340",
            null,
            "535e0790-c851-4b1d-a0bb-74ed796825be");
    try (FieldVector stringField = new VarCharVector("stringfield", allocator);
        ArrowBuf tableData = setupTableData(keysToFeed);
        ArrowBuf fixedBlocks = allocator.buffer(8 * keysToFeed.size())) {

      long startAddress = fixedBlocks.memoryAddress();
      for (int i = 0; i < keysToFeed.size(); ++i, startAddress += 8) {
        PlatformDependent.putInt(startAddress, keysToFeed.get(i) == null ? 0 : 1);
      }

      PivotDef pivot = buildPivot(stringField);
      List<String> fieldsToRead = Lists.newArrayList("stringfield");
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot, true)) {
        assertEquals("Invalid keybuf size", 32, reader.getKeyBufSize());
        assertTrue("Keys not trimmed", reader.isKeyTrimmedToFitSize());

        long tableVarAddress = tableData.memoryAddress();
        int currentVarOffset = 0;
        long tableFixedAddress = fixedBlocks.memoryAddress();
        byte[] keyBytes = new byte[32];
        for (String key : keysToFeed) {
          reader.loadNextKey(tableFixedAddress, tableVarAddress + currentVarOffset);
          tableFixedAddress += pivot.getBlockWidth();
          currentVarOffset += PlatformDependent.getInt(tableVarAddress + currentVarOffset) + 4;
          byte[] expectedKey = new byte[32];
          if (key != null) {
            expectedKey[0] = (byte) 1;
            byte keyLen = (byte) Math.min(key.getBytes(StandardCharsets.UTF_8).length, 30);
            expectedKey[1] = keyLen;
            byte[] stringBytes =
                key.length() <= 30
                    ? padZeroBytes(key.getBytes(StandardCharsets.UTF_8), 30)
                    : key.substring(0, 30).getBytes(StandardCharsets.UTF_8);
            System.arraycopy(stringBytes, 0, expectedKey, 2, 30);
          }
          reader.getKeyHolder().getBytes(0, keyBytes);
          assertTrue("Key mismatched", Arrays.equals(expectedKey, keyBytes));
        }
      }
    }
  }

  @Test
  public void testMultipleStringKeyLoadReverseOrder() throws Exception {
    List<String> keysToFeed1 = randomStrings(30);
    List<String> keysToFeed2 = randomStrings(30);
    List<String> keysToFeed3 = randomStrings(30);
    try (FieldVector stringField1 = new VarCharVector("stringfield1", allocator);
        FieldVector stringField2 = new VarCharVector("stringfield2", allocator);
        FieldVector stringField3 = new VarCharVector("stringfield3", allocator);
        ArrowBuf tableData = setupTableData(keysToFeed1, keysToFeed2, keysToFeed3);
        ArrowBuf fixedBlocks = allocator.buffer(8 * 30)) {

      long startAddress = fixedBlocks.memoryAddress();
      for (int i = 0; i < 30; ++i, startAddress += 8) {
        int validity = 0;
        validity |= ((keysToFeed1.get(i) == null) ? 0 : 1);
        validity |= ((keysToFeed2.get(i) == null) ? 0 : 1) << 1;
        validity |= ((keysToFeed3.get(i) == null) ? 0 : 1) << 2;
        PlatformDependent.putInt(startAddress, validity);
      }

      PivotDef pivot = buildPivot(stringField1, stringField2, stringField3);
      List<String> fieldsToRead =
          Lists.newArrayList("stringfield3", "stringfield2", "stringfield1");
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        assertEquals("Invalid keybuf size", 32, reader.getKeyBufSize());
        assertTrue("Keys not trimmed", reader.isKeyTrimmedToFitSize());

        long tableVarAddress = tableData.memoryAddress();
        int currentVarOffset = 0;
        long tableFixedAddress = fixedBlocks.memoryAddress();
        byte[] keyBytes = new byte[32];
        for (int i = 0; i < keysToFeed1.size(); i++) {
          reader.loadNextKey(tableFixedAddress, tableVarAddress + currentVarOffset);
          tableFixedAddress += pivot.getBlockWidth();
          currentVarOffset += PlatformDependent.getInt(tableVarAddress + currentVarOffset) + 4;
          reader.getKeyHolder().getBytes(0, keyBytes);
          int validity = 0;
          validity |= ((keysToFeed1.get(i) == null) ? 0 : 1) << 2;
          validity |= ((keysToFeed2.get(i) == null) ? 0 : 1) << 1;
          validity |= ((keysToFeed3.get(i) == null) ? 0 : 1);
          byte[] expectedKey =
              prepareSlicedKey(
                  (byte) validity,
                  keysToFeed3.get(i) != null
                      ? keysToFeed3.get(i).getBytes(StandardCharsets.UTF_8)
                      : new byte[10],
                  10,
                  keysToFeed2.get(i) != null
                      ? keysToFeed2.get(i).getBytes(StandardCharsets.UTF_8)
                      : new byte[10],
                  10,
                  keysToFeed1.get(i) != null
                      ? keysToFeed1.get(i).getBytes(StandardCharsets.UTF_8)
                      : new byte[11],
                  11,
                  32);
          assertTrue("Key mismatched", Arrays.equals(expectedKey, keyBytes));
        }
      }
    }
  }

  @Test
  public void testCompositeVarKeySubsetLoad() throws Exception {
    List<String> keysToFeed1 = randomStrings(30);
    List<String> keysToFeed2 = randomStrings(30);
    List<String> keysToFeed3 = randomStrings(30);
    try (FieldVector stringField1 = new VarCharVector("stringfield1", allocator);
        FieldVector stringField2 = new VarCharVector("stringfield2", allocator);
        FieldVector stringField3 = new VarCharVector("stringfield3", allocator);
        ArrowBuf tableData = setupTableData(keysToFeed1, keysToFeed2, keysToFeed3);
        ArrowBuf fixedBlocks = allocator.buffer(8 * 30)) {

      long startAddress = fixedBlocks.memoryAddress();
      for (int i = 0; i < 30; ++i, startAddress += 8) {
        int validity = 0;
        validity |= ((keysToFeed1.get(i) == null) ? 0 : 1);
        validity |= ((keysToFeed2.get(i) == null) ? 0 : 1) << 1;
        validity |= ((keysToFeed3.get(i) == null) ? 0 : 1) << 2;
        PlatformDependent.putInt(startAddress, validity);
      }

      PivotDef pivot = buildPivot(stringField1, stringField2, stringField3);
      List<String> fieldsToRead =
          Lists.newArrayList("stringfield1", "stringfield3"); // dropping stringfield2
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        assertEquals("Invalid keybuf size", 32, reader.getKeyBufSize());
        assertTrue("Keys not trimmed", reader.isKeyTrimmedToFitSize());

        long tableVarAddress = tableData.memoryAddress();
        int currentVarOffset = 0;
        long tableFixedAddress = fixedBlocks.memoryAddress();
        byte[] keyBytes = new byte[32];
        for (int i = 0; i < keysToFeed1.size(); i++) {
          reader.loadNextKey(tableFixedAddress, tableVarAddress + currentVarOffset);
          tableFixedAddress += pivot.getBlockWidth();
          currentVarOffset += PlatformDependent.getInt(tableVarAddress + currentVarOffset) + 4;
          reader.getKeyHolder().getBytes(0, keyBytes);
          int validity = 0;
          validity |= ((keysToFeed1.get(i) == null) ? 0 : 1);
          validity |= ((keysToFeed3.get(i) == null) ? 0 : 1) << 1;
          byte[] expectedKey =
              prepareSlicedKey(
                  (byte) validity,
                  keysToFeed1.get(i) != null
                      ? keysToFeed1.get(i).getBytes(StandardCharsets.UTF_8)
                      : new byte[15],
                  15,
                  new byte[0],
                  0,
                  keysToFeed3.get(i) != null
                      ? keysToFeed3.get(i).getBytes(StandardCharsets.UTF_8)
                      : new byte[16],
                  16,
                  32);
          assertTrue("Key mismatched", Arrays.equals(expectedKey, keyBytes));
        }
      }
    }
  }

  @Test
  public void testCompositeKeyMixedWidthCols() throws Exception {
    List<Integer> intKeysToFeed = Arrays.asList(1111, null, 3333, -4444);
    List<Long> longKeysToFeed = Arrays.asList(101010L, 202020L, null, -404040L);
    List<Double> doubleKeysToFeed = Arrays.asList(1.02321, 4.12321, 6.11312, null);
    List<String> stringKeysToFeed1 = randomStrings(4);
    List<String> stringKeysToFeed2 = randomStrings(4);
    List<Boolean> booleanKeysToFeed = Arrays.asList(false, true, false, null);

    try (FieldVector intField = new IntVector("intfield", allocator);
        FieldVector longField = new BigIntVector("longfield", allocator);
        FieldVector stringField1 = new VarCharVector("stringfield1", allocator);
        FieldVector booleanField = new BitVector("bitfield", allocator);
        FieldVector doubleField = new Float8Vector("doublefield", allocator);
        FieldVector stringField2 = new VarCharVector("stringfield2", allocator);
        ArrowBuf tableVarData = setupTableData(stringKeysToFeed1, stringKeysToFeed2);
        ArrowBuf tableFixedData = allocator.buffer(96)) {

      // Feed fixed keys at the address
      long startAddress = tableFixedData.memoryAddress();
      for (int keyIdx = 0; keyIdx < intKeysToFeed.size(); keyIdx++) {
        int validity = 0;
        validity |= ((intKeysToFeed.get(keyIdx) == null) ? 0 : 1);
        validity |= ((longKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 1;
        validity |= ((stringKeysToFeed1.get(keyIdx) == null) ? 0 : 1) << 2;
        if (booleanKeysToFeed.get(keyIdx) != null) {
          validity |= 1 << 3;
          validity |= (booleanKeysToFeed.get(keyIdx) ? 1 : 0) << 4;
        }
        validity |= ((doubleKeysToFeed.get(keyIdx) == null) ? 0 : 1) << 5;
        validity |= ((stringKeysToFeed2.get(keyIdx) == null) ? 0 : 1) << 6;
        PlatformDependent.putInt(startAddress, validity);
        PlatformDependent.putInt(
            startAddress + 4, intKeysToFeed.get(keyIdx) != null ? intKeysToFeed.get(keyIdx) : 0);
        PlatformDependent.putLong(
            startAddress + 8, longKeysToFeed.get(keyIdx) != null ? longKeysToFeed.get(keyIdx) : 0);
        copyDoubleToAddress(
            startAddress + 16,
            doubleKeysToFeed.get(keyIdx) != null ? doubleKeysToFeed.get(keyIdx) : 0);
        startAddress += 28;
      }

      PivotDef pivot =
          buildPivot(intField, longField, stringField1, booleanField, doubleField, stringField2);
      List<String> fieldsToRead =
          Lists.newArrayList(
              "stringfield2",
              "intfield",
              "bitfield",
              "stringfield1",
              "longfield"); // dropping doublefield
      try (HashTableKeyReader reader = newKeyReader(fieldsToRead, pivot)) {
        assertEquals("Invalid keybuf size", 32, reader.getKeyBufSize());
        assertTrue("Keys not trimmed", reader.isKeyTrimmedToFitSize());

        long tableVarAddress = tableVarData.memoryAddress();
        int currentVarOffset = 0;
        long tableFixedAddress = tableFixedData.memoryAddress();
        byte[] keyBytes = new byte[32];
        for (int i = 0; i < stringKeysToFeed1.size(); i++) {
          reader.loadNextKey(tableFixedAddress, tableVarAddress + currentVarOffset);
          tableFixedAddress += pivot.getBlockWidth();
          currentVarOffset += PlatformDependent.getInt(tableVarAddress + currentVarOffset) + 4;
          reader.getKeyHolder().getBytes(0, keyBytes);

          byte[] expectedKey = new byte[32];
          int validity = 0;
          validity |= ((stringKeysToFeed2.get(i) == null) ? 0 : 1);
          validity |= ((intKeysToFeed.get(i) == null) ? 0 : 1) << 1;
          if (booleanKeysToFeed.get(i) != null) {
            validity |= 1 << 2;
            validity |= (booleanKeysToFeed.get(i) ? 1 : 0) << 3;
          }
          validity |= ((stringKeysToFeed1.get(i) == null) ? 0 : 1) << 4;
          validity |= ((longKeysToFeed.get(i) == null) ? 0 : 1) << 5;
          expectedKey[0] = (byte) validity;
          System.arraycopy(
              (stringKeysToFeed2.get(i) != null
                  ? stringKeysToFeed2.get(i).getBytes(StandardCharsets.UTF_8)
                  : new byte[9]),
              0,
              expectedKey,
              1,
              9);
          System.arraycopy(
              intKeysToFeed.get(i) != null
                  ? ByteArrayUtil.toByta(intKeysToFeed.get(i))
                  : new byte[4],
              0,
              expectedKey,
              10,
              4);
          System.arraycopy(
              stringKeysToFeed1.get(i) != null
                  ? stringKeysToFeed1.get(i).getBytes(StandardCharsets.UTF_8)
                  : new byte[10],
              0,
              expectedKey,
              14,
              10);
          System.arraycopy(
              longKeysToFeed.get(i) != null
                  ? ByteArrayUtil.toByta(longKeysToFeed.get(i))
                  : new byte[8],
              0,
              expectedKey,
              24,
              8);

          assertTrue("Key mismatched", Arrays.equals(expectedKey, keyBytes));
        }
      }
    }
  }

  private ArrowBuf setupTableData(List<String>... colKeys) {
    checkArgument(!ArrayUtils.isEmpty(colKeys), "No column keys supplied.");
    int rowCount = colKeys[0].size();
    checkArgument(
        Arrays.stream(colKeys).allMatch(list -> list.size() == rowCount),
        "All input col key lists should be of same size.");
    int totalBufferSize =
        Arrays.stream(colKeys)
            .flatMap(
                keys ->
                    keys.stream()
                        .map(k -> k == null ? 4 : k.getBytes(StandardCharsets.UTF_8).length + 4))
            .reduce(Integer::sum)
            .map(totalBufSize -> totalBufSize + (4 * rowCount))
            .orElseThrow(() -> new IllegalArgumentException("Not able to compute buffer size"));
    ArrowBuf tableData = null;
    try {
      tableData = allocator.buffer(totalBufferSize);
      long blockOffset = 0;
      for (int i = 0; i < rowCount; i++) {
        int blockWidth = 0;

        long localOffset = blockOffset + 4;
        for (List<String> keysToFeed : colKeys) {
          String key = keysToFeed.get(i);
          byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : new byte[0];
          tableData.setInt(localOffset, keyBytes.length);
          tableData.setBytes(localOffset + 4, keyBytes);
          long cellWidth = keyBytes.length + 4;
          localOffset += cellWidth;
          blockWidth += cellWidth;
        }

        tableData.setInt(blockOffset, blockWidth);
        blockOffset += blockWidth + 4;
      }
      return tableData;
    } catch (RuntimeException e) {
      AutoCloseables.closeNoChecked(tableData);
      throw e;
    }
  }

  private byte[] prepareSlicedKey(
      byte validityByte,
      byte[] key1,
      int k1Slice,
      byte[] key2,
      int k2Slice,
      byte[] key3,
      int k3Slice,
      int totalSize) {
    byte[] slicedKey = new byte[totalSize];
    slicedKey[0] = validityByte;
    System.arraycopy(key1, 0, slicedKey, 1, k1Slice);
    System.arraycopy(key2, 0, slicedKey, k1Slice + 1, k2Slice);
    System.arraycopy(key3, 0, slicedKey, k1Slice + k2Slice + 1, k3Slice);
    return slicedKey;
  }

  private byte[] padZeroBytes(byte[] bytes, int expectedSize) {
    byte[] targetBytes = new byte[expectedSize];
    System.arraycopy(bytes, 0, targetBytes, (expectedSize - bytes.length), bytes.length);
    return targetBytes;
  }

  private void copyDoubleToAddress(long address, double val) {
    byte[] bytes = new byte[8];
    ByteBuffer.wrap(bytes).putDouble(val);
    PlatformDependent.putLong(address, ByteBuffer.wrap(bytes).getLong());
  }

  private HashTableKeyReader newKeyReader(List<String> fieldNames, PivotDef pivot) {
    return newKeyReader(fieldNames, pivot, false);
  }

  private HashTableKeyReader newKeyReader(
      List<String> fieldNames, PivotDef pivot, boolean setVarFieldLenInFirstByte) {
    HashTableKeyReader.Builder builder =
        new HashTableKeyReader.Builder()
            .setBufferAllocator(allocator)
            .setPivot(pivot)
            .setMaxKeySize(32)
            .setFieldsToRead(fieldNames)
            .setSetVarFieldLenInFirstByte(setVarFieldLenInFirstByte);
    return builder.build();
  }

  private PivotDef buildPivot(FieldVector... fields) {
    List<FieldVectorPair> fieldVectors =
        Arrays.stream(fields).map(f -> new FieldVectorPair(f, f)).collect(Collectors.toList());
    return PivotBuilder.getBlockDefinition(fieldVectors);
  }
}
