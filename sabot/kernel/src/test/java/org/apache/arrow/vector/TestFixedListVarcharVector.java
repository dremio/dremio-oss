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
package org.apache.arrow.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import com.google.common.base.Preconditions;


public class TestFixedListVarcharVector extends DremioTest {
  private BufferAllocator testAllocator;
  static final int TOTAL_ROWS = 2000;
  static final int MAX_VALUES_PER_BATCH = 2500;
  static final int MAX_LIST_AGG_SIZE = 256;
  static final String DELIMITER = ";;";
  static final String OVERFLOW_STRING = "\u2026"; // Unicode for ellipsis '…'

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-fixedlist-varchar-vector", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    testAllocator.close();
  }

  @Test
  public void TestBasic() {
    TestBasic(false, false, false);
    TestBasic(true, false, false);
    TestBasic(false, true, false);
    TestBasic(false, true, true);
    TestBasic(true, true, false);
    TestBasic(true, true, true);
  }

  private void TestBasic(boolean distinct, boolean orderby, boolean asc) {
    int batchSize = 100;

    FixedListVarcharVector flv = new FixedListVarcharVector("TestCompactionThreshold", testAllocator,
      batchSize, "", MAX_LIST_AGG_SIZE, distinct, orderby, asc, false, null);

    int validitySize = FixedListVarcharVector.getValidityBufferSize(batchSize);
    int dataSize = FixedListVarcharVector.getDataBufferSize(batchSize);

    ArrowBuf validityBuf = testAllocator.buffer(validitySize);
    ArrowBuf dataBuf = testAllocator.buffer(dataSize);
    flv.loadBuffers(batchSize, dataBuf, validityBuf);
    validityBuf.getReferenceManager().release();
    dataBuf.getReferenceManager().release();

    try (ArrowBuf sampleDataBuf = testAllocator.buffer(512)) {
      final String insString1 = "aaaaa";
      final String insString2 = "bbbbbbbbbb";
      sampleDataBuf.setBytes(0, insString1.getBytes());
      sampleDataBuf.setBytes(insString1.length(), insString2.getBytes());

      flv.addValueToRowGroup(0, 0, insString1.length(), sampleDataBuf);
      flv.addValueToRowGroup(1, insString1.length(), insString2.length(), sampleDataBuf);

      byte[] str1 = flv.get(0);
      byte[] str2 = flv.get(1);
      Preconditions.checkState(Arrays.equals(str1, insString1.getBytes()));
      Preconditions.checkState(Arrays.equals(str2, insString2.getBytes()));
    } finally {
      flv.close();
    }
  }

  @Test
  public void TestDistinctBasic() {
    // Input Provided:
    // 0: "AA", "AA", BB
    // 1: "AAA", "BBB", "AAA"
    // 2: "CC", "DDD"
    final int SMALL_MAX_VALUES_PER_BATCH = 25;
    final int SMALL_MAX_LIST_AGG_SIZE = 10;
    final ListVector tempSpace =  FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-distinct-basic", SMALL_MAX_VALUES_PER_BATCH, DELIMITER,
      SMALL_MAX_LIST_AGG_SIZE, true, false, false, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2 },
      new String[] { "AA", "AAA", "AA", "BBB", "BB", "AAA", "CC", "DDD" }, testAllocator);

    // ACT
    final IntArrayList[] rowGroups = flv.extractRowGroups();
    flv.distinct(rowGroups);

    // ASSERT
    // Expected output:
    // 0: "AA", 0xFFFF, BB
    // 1: "AAA", "BBB", 0xFFFF
    // 2: "CC", "DDD"
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "AAA"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(1, "BBB"),
      new RowGroupValuePair(0, "BB"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(2, "CC"),
      new RowGroupValuePair(2, "DDD"),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestMoveValuesAndFreeSpace() {
    VarCharVector v1 = new VarCharVector("TestCopyOutVarchar", testAllocator);
    VarCharVector v2 = new VarCharVector("TestCopyOutVarchar", testAllocator);
    VarCharVector v3 = new VarCharVector("TestCopyOutVarchar", testAllocator);
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, MAX_VALUES_PER_BATCH);
    FixedListVarcharVector flv = makeFlv("test-move-values-and-free-space", MAX_VALUES_PER_BATCH, DELIMITER,
      MAX_LIST_AGG_SIZE, false, false, false, false, tempSpace, testAllocator);
    FixedListVarcharVector newflv = makeFlv("test-move-values-and-free-space-new", MAX_VALUES_PER_BATCH, DELIMITER,
      MAX_LIST_AGG_SIZE, false, false, false, false, tempSpace, testAllocator);

    try (ArrowBuf sampleDataBuf = testAllocator.buffer(512)) {
      final String insString1 = "aaaaa";
      final String insString2 = "ありがと";
      final String insString3 = "12345";
      final String outputString = insString1 + DELIMITER + insString2 + DELIMITER + insString3;
      final String reverseOutputString = insString3 + DELIMITER + insString2 + DELIMITER + insString1;

      sampleDataBuf.setBytes(0, insString1.getBytes());
      sampleDataBuf.setBytes(insString1.length(), insString2.getBytes());
      sampleDataBuf.setBytes(insString1.length() + insString2.getBytes().length, insString3.getBytes());

      /** For rows 0 to TOTAL_ROWS/2 insert in the order string1, string2 and string3. */
      for (int row = 0; row < TOTAL_ROWS/2; row++) {
        flv.addValueToRowGroup(row, 0, insString1.length(), sampleDataBuf);
        flv.addValueToRowGroup(row, insString1.length(),  insString2.getBytes().length, sampleDataBuf);
        flv.addValueToRowGroup(row, insString1.length() + insString2.getBytes().length , insString3.length(), sampleDataBuf);
      }
      /** For rows TOTAL_ROWS/2 to TOTAL_ROWS insert in the order string3, string2 and string1. */
      for (int row = TOTAL_ROWS/2; row < TOTAL_ROWS; row++) {
        flv.addValueToRowGroup(row, insString1.length() + insString2.getBytes().length , insString3.length(), sampleDataBuf);
        flv.addValueToRowGroup(row, insString3.length(),  insString2.getBytes().length, sampleDataBuf);
        flv.addValueToRowGroup(row, 0, insString1.length(), sampleDataBuf);
      }

      v1.allocateNew(flv.getRequiredByteCapacity(), TOTAL_ROWS);
      flv.outputToVector(v1, 0, TOTAL_ROWS);

      /** Before moving the records to the new object verify all TOTAL_ROWS are filled. */
      for (int row = 0; row < TOTAL_ROWS; row++) {
        if (row < TOTAL_ROWS/2) {
          Assert.assertEquals(outputString /*expected*/, v1.getObject(row).toString() /*actual*/);
        } else {
          Assert.assertEquals(reverseOutputString /*expected*/, v1.getObject(row).toString() /*actual*/);
        }
      }

      flv.moveValuesAndFreeSpace(TOTAL_ROWS/2, 0, TOTAL_ROWS / 2, newflv);

      v2.allocateNew(flv.getRequiredByteCapacity(), TOTAL_ROWS / 2);
      v3.allocateNew(newflv.getRequiredByteCapacity(), TOTAL_ROWS / 2);

      flv.outputToVector(v2, 0, TOTAL_ROWS / 2);
      newflv.outputToVector(v3, 0, TOTAL_ROWS / 2);

      /** After moving the records both the objects will have TOTAL_ROWS/2 rows. */
      for (int row = 0; row < TOTAL_ROWS/2; row++) {
        Assert.assertEquals(outputString /*expected*/, v2.getObject(row).toString() /*actual*/);
      }
      for (int row = 0; row < TOTAL_ROWS/2; row++) {
        Assert.assertEquals(reverseOutputString /*expected*/, v3.getObject(row).toString() /*actual*/);
      }

    } finally {
      flv.close();
      newflv.close();
      v1.close();
      v2.close();
      v3.close();
      tempSpace.close();
    }
  }
  private static void populateStringsInArrowBuf(final String[] content, final ArrowBuf buf) {
    for (final String s : content) {
      buf.setBytes(buf.writerIndex(), s.getBytes());
      buf.writerIndex(buf.writerIndex() + s.length());
    }
  }

  private static void populateFixedListVarcharVector(final FixedListVarcharVector flv, final int[] groups, final String[] values,  final BufferAllocator allocator) {
    assert groups.length == values.length;

    try (final ArrowBuf dataBuf = allocator.buffer(512)) {
       populateStringsInArrowBuf(values, dataBuf);

       int head = 0;

       for (int i = 0; i < values.length; i++) {
         flv.addValueToRowGroup(groups[i], head, values[i].length(), dataBuf);
         head += values[i].length();
       }
    }
  }

  private static FixedListVarcharVector makeFlv(final String name,
                                                final int batchSize,
                                                final String delimiter,
                                                final int maxListAggSize,
                                                final boolean distinct,
                                                final boolean orderby,
                                                final boolean asc,
                                                final boolean allowOneOverflow,
                                                final ListVector tempSpace,
                                                final BufferAllocator allocator) {
    final FixedListVarcharVector flv = new FixedListVarcharVector(name, allocator,
      batchSize, delimiter,maxListAggSize, distinct, orderby, asc, allowOneOverflow, tempSpace);
    final int validitySize = FixedListVarcharVector.getValidityBufferSize(batchSize);
    final int dataSize = FixedListVarcharVector.getDataBufferSize(batchSize);

    final ArrowBuf validityBuf = allocator.buffer(validitySize);
    final ArrowBuf dataBuf = allocator.buffer(dataSize);

    flv.loadBuffers(batchSize, dataBuf, validityBuf);

    validityBuf.getReferenceManager().release();
    dataBuf.getReferenceManager().release();

    return flv;
  }

  private static class RowGroupValuePair {
    private final int rowGroup;
    private final String value;

    RowGroupValuePair(final int rowGroup, final String value) {
      this.rowGroup = rowGroup;
      this.value = value;
    }

    public int getRowGroup() {
      return rowGroup;
    }

    public String getValue() {
      return value;
    }
  }

  private static void assertVarCharVector(final String[] expected, final VarCharVector varCharVector) {
    assertEquals(expected.length, varCharVector.getLastSet() + 1);

    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(varCharVector.getObject(i) == null);
      } else {
        assertEquals(String.format("Comparing value at index #%d", i), expected[i], varCharVector.getObject(i).toString());
      }
    }
  }

  private static void assertFlv(final RowGroupValuePair[] expected, final FixedListVarcharVector flv) {
    assertEquals(expected.length, flv.size());

    for (int i = 0; i < expected.length; i++) {
      assertEquals(String.format("Comparing row group at index #%d", i), expected[i].getRowGroup(), flv.getGroupIndex(i));

      if (expected[i].getValue() != null) {
        assertEquals(String.format("Comparing value at index #%d", i), expected[i].getValue(), flv.getObject(i).toString());
      }
    }
  }

  @Test
  public void testOrderItemsInRowGroupsAsc() {
    // ARRANGE
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 20);
    final FixedListVarcharVector flv = makeFlv("test-order-items-in-row-groups-asc-flv", 64,
      DELIMITER, 20, false, true, true, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 2, 1 },
      new String[] { "AAA", "EE", "CCCC", "DDD", "BB" }, testAllocator);

    final IntArrayList[] rowGroups = new IntArrayList[3];
    rowGroups[0] = new IntArrayList(new int[] { 0, 2 });
    rowGroups[1] = new IntArrayList(new int[] { 1, 4 });
    rowGroups[2] = new IntArrayList(new int[] { 3 });

    // ACT
    flv.orderItemsInRowGroups(rowGroups);

    // ASSERT
    final long nonEmptyItemCount = Arrays.stream(rowGroups).filter(Objects::nonNull).count();

    assertEquals(3, nonEmptyItemCount);
    assertArrayEquals(new int[] { 0, 2 }, rowGroups[0].toIntArray());
    assertArrayEquals(new int[] { 4, 1 }, rowGroups[1].toIntArray());
    assertArrayEquals(new int[] { 3 }, rowGroups[2].toIntArray());

    // TEARDOWN
    flv.close();
    tempSpace.close();
  }

  @Test
  public void testOrderItemsInRowGroupsDesc() {
    // ARRANGE
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 20);
    final FixedListVarcharVector flv = makeFlv("test-order-items-in-row-groups-desc-flv", 64,
      DELIMITER, 20, false, true, false, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 0 }, new String[] { "AAA", "BB" }, testAllocator);

    final IntArrayList[] rowGroups = new IntArrayList[1];
    rowGroups[0] = new IntArrayList(new int[] { 0, 1 });

    // ACT
    flv.orderItemsInRowGroups(rowGroups);

    // ASSERT
    final long nonEmptyItemCount = Arrays.stream(rowGroups).filter(Objects::nonNull).count();

    assertEquals(1, nonEmptyItemCount);
    assertArrayEquals(new int[] { 1, 0 }, rowGroups[0].toIntArray());

    // TEARDOWN
    flv.close();
    tempSpace.close();
  }

  @Test
  public void testDeleteExcessItemsInGroups() {
    // ARRANGE
    // 0: "aaa", "bb", "cc"
    // 1: "AAAAAA", "BBB", "CCC"
    // 2: "CC", "DDD"
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 64);
    final FixedListVarcharVector flv = makeFlv("test-delete-excess-items-in-groups-flv", 64,
      DELIMITER, 12, false, true, true, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2 },
      new String[] { "aaa", "AAAAAA", "bb", "BBB", "cc", "CCC", "DD", "DDD" }, testAllocator);

    final IntArrayList[] rowGroups = new IntArrayList[3];
    rowGroups[0] = new IntArrayList(new int[] { 0, 2, 4});
    rowGroups[1] = new IntArrayList(new int[] { 1, 3, 5 });
    rowGroups[2] = new IntArrayList(new int[] { 6, 7 });

    // ACT
    flv.deleteExcessItemsInGroups(rowGroups, 0);

    // ASSERT
    final int deletedValue = FixedListVarcharVector.DELETED_VALUE_ROW_GROUP;
    assertFalse(flv.isNull(0));
    assertFalse(flv.isNull(1));
    assertFalse(flv.isNull(2));
    assertTrue(flv.isNull(3));
    assertTrue(flv.isNull(4));
    assertTrue(flv.isNull(5));
    assertFalse(flv.isNull(6));
    assertFalse(flv.isNull(7));
    assertTrue(flv.isOverflowSet(0));
    assertTrue(flv.isOverflowSet(1));
    assertFalse(flv.isOverflowSet(2));
    assertArrayEquals(new int[] { 0, 2 }, rowGroups[0].toIntArray());
    assertArrayEquals(new int[] { 1 }, rowGroups[1].toIntArray());
    assertArrayEquals(new int[] { 6, 7 }, rowGroups[2].toIntArray());

    // TEARDOWN
    flv.close();
    tempSpace.close();
  }

  @Test
  public void testExtractRowGroups() {
    // ARRANGE
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 64);
    final FixedListVarcharVector flv = makeFlv("test-extract-row-groups-flv", 64,
      DELIMITER, 20, false, true, true, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 2, 1 }, new String[] { "AAA", "EE", "CCCC", "DDD", "BB" }, testAllocator);

    // ACT
    final IntArrayList[] rowGroups = flv.extractRowGroups();

    // ASSERT
    final long nonEmptyItemCount = Arrays.stream(rowGroups).filter(Objects::nonNull).count();

    assertEquals(3, nonEmptyItemCount);
    assertArrayEquals(new int[] { 0, 2 }, rowGroups[0].toIntArray());
    assertArrayEquals(new int[] { 1, 4 }, rowGroups[1].toIntArray());
    assertArrayEquals(new int[] { 3 }, rowGroups[2].toIntArray());

    // TEARDOWN
    flv.close();
    tempSpace.close();
  }

  @Test
  public void testPhysicallyRearrangeValues() {
    // ARRANGE
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 64);

    final FixedListVarcharVector flv = makeFlv("test-physically-rearrange-values-flv", 64,
      DELIMITER, 20, false, true, true, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 2, 1 }, new String[] { "AAA", "EE", "CCCC", "DDD", "BB" }, testAllocator);

    final IntArrayList[] rowGroups = new IntArrayList[3];

    rowGroups[0] = new IntArrayList(new int[] { 2, 0 } );
    rowGroups[1] = new IntArrayList(new int[] { 4, 1 } );
    rowGroups[2] = new IntArrayList(new int[] { 3 } );

    // ACT
    flv.physicallyRearrangeValues(rowGroups);

    // ASSERT
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "CCCC"),
      new RowGroupValuePair(0, "AAA"),
      new RowGroupValuePair(1, "BB"),
      new RowGroupValuePair(1, "EE"),
      new RowGroupValuePair(2, "DDD"),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void testCompactWithOrderByLimitSizeAndPhysicalRearrangement() {
    // ARRANGE
    // Test row groups and values:
    // 0: "CC", "AA", "AA"
    // 1: "AAA", "BBB", "CCC"
    // 2: "DD", "DDD"
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 64);
    final FixedListVarcharVector flv = makeFlv("test-compact-with-order-by-limit-size-and-physical-rearrangement-asc",
      64, DELIMITER, 12, false, true, true, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2 },
      new String[] { "CC", "AAA", "AA", "BBB", "AA", "CCC", "DD", "DDD" }, testAllocator);

    // ACT
    flv.compact(true);

    // ASSERT
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "AAA"),
      new RowGroupValuePair(2, "DD"),
      new RowGroupValuePair(2, "DDD"),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void testCompactWithOrderByAndLimitSize() {
    // ARRANGE
    // Test row groups and values:
    // 0: "CC", "BB", "AA"
    // 1: "aa", "bbb", "ccc"
    // 2: "DD", "DDD"
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 10);
    final FixedListVarcharVector flv = makeFlv("test-compact-with-order-by-and-limit-size",
      64, DELIMITER, 12, false, true, true, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2 },
      new String[] { "CC", "aa", "BB", "bbb", "AA", "ccc", "DD", "DDD" }, testAllocator);

    // ACT
    flv.compact(false);

    // ASSERT
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(1, "aa"),
      new RowGroupValuePair(0, "BB"),
      new RowGroupValuePair(1, "bbb"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(2, "DD"),
      new RowGroupValuePair(2, "DDD"),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void testCompactWithDistinctOrderByLimitSizeAndPhysicalRearrangement() {
    // ARRANGE
    // Test row groups and values:
    // 0: "BB", "BB", "AA"
    // 1: "AAA", "BBB", "AAA"
    // 2: "DD", "DDD"
    // 3: "A", "A", "B"
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, 20);
    final FixedListVarcharVector flv = makeFlv("test-compact-with-distincy-orderby-limit-size-and-physical-rearrangement-asc",
      64, DELIMITER, 20, true, true, true, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2, 3, 3, 3 },
      new String[] { "BB", "AAA", "BB", "BBB", "AA", "AAA", "DD", "DDD", "A", "A", "B" }, testAllocator);

    // ACT
    flv.compact(true);

    // ASSERT
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "BB"),
      new RowGroupValuePair(1, "AAA"),
      new RowGroupValuePair(1, "BBB"),
      new RowGroupValuePair(2, "DD"),
      new RowGroupValuePair(2, "DDD"),
      new RowGroupValuePair(3, "A"),
      new RowGroupValuePair(3, "B"),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestLimitSizeBasic() {
    // Input Provided:
    // 0: "AA", "AA", "BB"
    // 1: "AAA", "BBB", "AAA"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD"
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_LIST_AGG_SIZE);
    final FixedListVarcharVector flv = makeFlv("test-limit-size-basic", MAX_VALUES_PER_BATCH, delimiter,
      SMALL_MAX_LIST_AGG_SIZE, false, false, false, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2, 3, 3 },
      new String[] { "AA", "AAA", "AA", "BBB", "BB", "AAA", "CC", "DDD", "CCC", "DDDD" }, testAllocator);

    // ACT
    final IntArrayList[] rowGroups = flv.extractRowGroups();
    flv.limitSize(rowGroups, 0);

    // ASSERT
    // Expected output:
    // 0: "AA", "AA", 0xFFFF
    // 1: "AAA", "BBB", 0xFFFF
    // 2: "CC", "DDD"
    // 3: "CCC", 0xFFFF
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "AAA"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "BBB"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(2, "CC"),
      new RowGroupValuePair(2, "DDD"),
      new RowGroupValuePair(3, "CCC"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestCompactWithDistinctAndLimitSize() {
    // Input Provided:
    // 0: "AA", "AA", "BB", "BB", "A", "B"
    // 1: "a", "b", "a", "b"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD"
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final int SMALL_MAX_VALUES_PER_BATCH = 64;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-compact-with-distinct-and-limit-size",
      SMALL_MAX_VALUES_PER_BATCH, delimiter, SMALL_MAX_LIST_AGG_SIZE, true, false, false, false,
      tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 1, 2, 3, 3, 0, 0, 0 },
      new String[] { "AA", "a", "AA", "b", "BB", "a", "CC", "b", "DDD", "CCC", "DDDD", "BB", "A", "B" }, testAllocator);

    // ACT
    flv.compact(false);

    // ASSERT
    // Expected output:
    // 0: "AA", 0xFFFF, "BB", 0xFFFF, "A", 0xFFFF
    // 1: "a", "b", 0xFFFF, 0xFFFF
    // 2: "CC", "DDD"
    // 3: "CCC", 0xFFFF
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "a"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(1, "b"),
      new RowGroupValuePair(0, "BB"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(2, "CC"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(2, "DDD"),
      new RowGroupValuePair(3, "CCC"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      new RowGroupValuePair(0, "A"),
      new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestCompactWithDistinctLimitSizeAndPhysicalRearrangement() {
    // Input Provided:
    // 0: "AA", "AA", "BB", "BB", "A", "B"
    // 1: "a", "b", "a", "b"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD"
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final int SMALL_MAX_VALUES_PER_BATCH = 64;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-compact-with-distinct-limit-size-and-physical-rearrangement",
      SMALL_MAX_VALUES_PER_BATCH, delimiter, SMALL_MAX_LIST_AGG_SIZE, true, false, false, false,
      tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 1, 2, 3, 3, 0, 0, 0 },
      new String[] { "AA", "a", "AA", "b", "BB", "a", "CC", "b", "DDD", "CCC", "DDDD", "BB", "A", "B" }, testAllocator);

    // ACT
    flv.compact(true);

    // ASSERT
    // Expected output:
    // 0: "AA", "BB", "A"
    // 1: "a", "b"
    // 2: "CC", "DDD"
    // 3: "CCC"
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "BB"),
      new RowGroupValuePair(0, "A"),
      new RowGroupValuePair(1, "a"),
      new RowGroupValuePair(1, "b"),
      new RowGroupValuePair(2, "CC"),
      new RowGroupValuePair(2, "DDD"),
      new RowGroupValuePair(3, "CCC"),
    };

    assertFlv(expected, flv);

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestOutputToVectorBasic() {
    // Input Provided:
    // 0: "AA", "AA", "BB"
    // 1: "AAA", "BBB", "AAA"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD"
    VarCharVector v1 = new VarCharVector("TestCopyOutVarchar", testAllocator);
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final int SMALL_MAX_VALUES_PER_BATCH = 16;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-limit-size-basic", SMALL_MAX_VALUES_PER_BATCH,
      delimiter, SMALL_MAX_LIST_AGG_SIZE, false, false, false, false,
      tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2, 3, 3 },
      new String[] { "AA", "AAA", "AA", "BBB", "BB", "AAA", "CC", "DDD", "CCC", "DDDD" }, testAllocator);

    // ACT
    flv.compact(true);
    v1.allocateNew(flv.getRequiredByteCapacity(), SMALL_MAX_VALUES_PER_BATCH);
    flv.outputToVector(v1, 0, 4);
    // ASSERT
    // Expected output:
    // 0: "AA","AA",...
    // 1: "AAA","BBB",...
    // 2: "CC","DDD"
    // 3: "CCC",...
    String[] expected = new String[] {
      "AA" + delimiter + "AA" + delimiter + OVERFLOW_STRING,
      "AAA" + delimiter + "BBB" + delimiter + OVERFLOW_STRING,
      "CC" + delimiter + "DDD",
      "CCC" + delimiter + OVERFLOW_STRING,
    };

    assertTrue(flv.delimterAndOverflowSize() == (3 * delimiter.length() + 3 * flv.getOverflowReserveSpace()));
    assertTrue(flv.getRequiredByteCapacity() == v1.getEndOffset(v1.getLastSet()));
    assertVarCharVector(expected, v1);

    // TEARDOWN
    tempSpace.close();
    flv.close();
    v1.close();
  }

  @Test
  public void TestAddValueToRowGroup() {
    // Input Provided:
    // 0: "AA", "AA", "BB"
    // 1: "aaa", "bbb", "aaa"
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final int SMALL_MAX_VALUES_PER_BATCH = 4;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-limit-size-basic", SMALL_MAX_VALUES_PER_BATCH,
      delimiter, SMALL_MAX_LIST_AGG_SIZE, false, false, false, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1 },
      new String[] { "AA", "aaa", "AA", "bbb", "BB", "aaa" }, testAllocator);

    // ACT
    flv.compact(true);

    // ASSERT
    // Expected output:
    // 0: "AA" ,"AA"
    // 1: "aaa", "bbb"
    final RowGroupValuePair[] expectedAfterCompact = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "aaa"),
      new RowGroupValuePair(1, "bbb"),
    };

    assertFlv(expectedAfterCompact, flv);

    // Add couple more values which should not be accepted.
    ArrowBuf sampleDataBuf = testAllocator.buffer(512);
    final String insString1 = "A";
    final String insString2 = "a";
    sampleDataBuf.setBytes(0, insString1.getBytes());
    sampleDataBuf.setBytes(insString1.length(), insString2.getBytes());

    flv.addValueToRowGroup(0, 0, insString1.length(), sampleDataBuf);
    flv.addValueToRowGroup(1, insString1.length(), insString2.length(), sampleDataBuf);

    // ASSERT
    // Expected output:
    // 0: "AA" ,"AA"
    // 1: "aaa", "bbb"
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "aaa"),
      new RowGroupValuePair(1, "bbb"),
    };

    // As orderby is not present, once overflow is set, no new elements are accepted.
    assertTrue(expected.length == flv.size());
    assertFlv(expected, flv);

    // TEARDOWN
    sampleDataBuf.getReferenceManager().release();
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestAddValueToRowGroupWithOrderby() {
    // Input Provided:
    // 0: "AA", "BB", "AA"
    // 1: "aaa", "bbb", "aaa"
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final int SMALL_MAX_VALUES_PER_BATCH = 4;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-limit-size-basic", SMALL_MAX_VALUES_PER_BATCH,
      delimiter, SMALL_MAX_LIST_AGG_SIZE, false, true, true, false,tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1 },
      new String[] { "AA", "aaa", "BB", "bbb", "AA", "aaa" }, testAllocator);

    flv.compact(true);

    final RowGroupValuePair[] expectedAfterCompact = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "aaa"),
      new RowGroupValuePair(1, "aaa"),
    };
    assertFlv(expectedAfterCompact, flv);

    // Add couple more values which should be accepted.
    ArrowBuf sampleDataBuf = testAllocator.buffer(512);
    final String insString1 = "A";
    final String insString2 = "a";
    sampleDataBuf.setBytes(0, insString1.getBytes());
    sampleDataBuf.setBytes(insString1.length(), insString2.getBytes());

    flv.addValueToRowGroup(0, 0, insString1.length(), sampleDataBuf);
    flv.addValueToRowGroup(1, insString1.length(), insString2.length(), sampleDataBuf);

    final RowGroupValuePair[] expectedBeforeSecondCompact = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "aaa"),
      new RowGroupValuePair(1, "aaa"),
      new RowGroupValuePair(0, "A"),
      new RowGroupValuePair(1, "a"),
    };

    // As orderby is present, even after overflow is set, new values are accepted.
    assertTrue(expectedBeforeSecondCompact.length == flv.size());
    assertFlv(expectedBeforeSecondCompact, flv);

    // ACT
    flv.compact(true);

    // ASSERT
    // Expected output:
    // 0: "A", "AA", "AA"
    // 1: "a" ,"aaa"
    final RowGroupValuePair[] expected = new RowGroupValuePair[] {
      new RowGroupValuePair(0, "A"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(0, "AA"),
      new RowGroupValuePair(1, "a"),
      new RowGroupValuePair(1, "aaa"),
    };

    // After second compact, only values that are required are arranged in order.
    assertTrue(expected.length == flv.size());
    assertFlv(expected, flv);

    // TEARDOWN
    sampleDataBuf.getReferenceManager().release();
    tempSpace.close();
    flv.close();
  }

  private ListVector makeAndPrepareListVector(final int numberOfGroups, final int numberOfValues) {
    ArrowBuf sampleDataBuf = testAllocator.buffer(512);
    final String insString1 = "aaa";
    final String insString2 = "bbb";
    sampleDataBuf.setBytes(0, insString1.getBytes());
    sampleDataBuf.setBytes(insString1.length(), insString2.getBytes());

    ListVector listVector = ListVector.empty("ListVector", testAllocator);
    UnionListWriter writer = listVector.getWriter();
    for (int i = 0; i < numberOfGroups; i++) {
      writer.setPosition(i);
      writer.startList();
      for (int j = 0; j < numberOfValues; j++) {
        if (i % 2 == 0) {
          writer.varChar().writeVarChar(0, insString1.length(), sampleDataBuf);
        } else {
          writer.varChar().writeVarChar(insString1.length(), insString1.length() + insString2.length(), sampleDataBuf);
        }
      }
      writer.endList();
    }
    listVector.setValueCount(numberOfGroups);

    sampleDataBuf.getReferenceManager().release();
    return listVector;
  }

  @Test
  public void TestAddListVectorToRowGroup() {
    int batchSize = 100;
    final String delimiter = ",";
    VarCharVector v1 = new VarCharVector("TestCopyOutVarchar", testAllocator);

    FixedListVarcharVector flv = new FixedListVarcharVector("TestCompactionThreshold", testAllocator,
      batchSize, delimiter, MAX_LIST_AGG_SIZE, false, false, false, false, null);

    int validitySize = FixedListVarcharVector.getValidityBufferSize(batchSize);
    int dataSize = FixedListVarcharVector.getDataBufferSize(batchSize);

    ArrowBuf validityBuf = testAllocator.buffer(validitySize);
    ArrowBuf dataBuf = testAllocator.buffer(dataSize);
    flv.loadBuffers(batchSize, dataBuf, validityBuf);
    validityBuf.getReferenceManager().release();
    dataBuf.getReferenceManager().release();

    ListVector listVector = makeAndPrepareListVector(2, 2);

    flv.addListVectorToRowGroup(0, listVector, 0);
    flv.addListVectorToRowGroup(1, listVector, 1);

    v1.allocateNew(flv.getRequiredByteCapacity(), TOTAL_ROWS);

    flv.outputToVector(v1, 0, 2);

    String[] expected = new String[] {
      "aaa" + delimiter + "aaa",
      "bbb" + delimiter + "bbb",
    };

    assertVarCharVector(expected, v1);

    listVector.close();
    flv.close();
    v1.close();
  }

  /**
   * The vector may have missing entries, this function returns the number of present items
   */
  private static int countNonNullEntries(final ListVector vector) {
    int nonNullCount = 0;

    for (int i = 0; i < vector.getValueCount(); i++) {
      if (!vector.isNull(i)) {
        nonNullCount++;
      }
    }

    return nonNullCount;
  }

  private static void assertListVector(final String[][] expected, final ListVector vector) {
    final int expectedNonNullCount = (int) Arrays.stream(expected).filter(Objects::nonNull).count();
    final int actualNonNullCount = countNonNullEntries(vector);

    assertEquals("Counts of non-null lists in the ListVector", expectedNonNullCount, actualNonNullCount);

    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        continue;
      }

      assertEquals(String.format("List length at position %d", i), expected[i].length, vector.getObject(i).size());

      for (int j = 0; j < expected[i].length; j++) {
        assertEquals(String.format("Comparing items in list %d at index %d", i, j), expected[i][j], vector.getObject(i).get(j).toString());
      }
    }
  }

  @Test
  public void TestWriteRowGroupValues() {
    // ARRANGE
    final int VALUES_PER_BATCH = 64;
    final int MAX_LIST_AGG_SIZE = 1024 * 1024; // A high limit because this test isn't concerned with overflow behavior
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-limit-size-basic", VALUES_PER_BATCH, DELIMITER,
      MAX_LIST_AGG_SIZE, false, false, false, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 5, 5, 3, 3 },
      new String[] { "AA", "AAA", "AA", "BBB", "BB", "AAA", "CC", "DDD", "CCC", "DDDD" }, testAllocator);
    final ListVector outputVector = ListVector.empty("test-output-vector", testAllocator);
    final UnionListWriter writer = outputVector.getWriter();
    final IntArrayList valueIndices = new IntArrayList(new int[]{ 0, 4, 6, 7 });

    // ACT
    flv.writeRowGroupValues(writer, valueIndices);
    outputVector.setValueCount(flv.size());

    // ASSERT
    final String[][] expectedValues = {
      { "AA", "BB", "CC", "DDD" }
    };

    assertListVector(expectedValues, outputVector);

    // TEARDOWN
    outputVector.close();
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestOutputToListVector() {
    // ARRANGE
    final int VALUES_PER_BATCH = 64;
    final int MAX_LIST_AGG_SIZE = 1024 * 1024; // A high limit because this test isn't concerned with overflow behavior
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-output-to-list-vector", VALUES_PER_BATCH,
      DELIMITER, MAX_LIST_AGG_SIZE, false, false, false, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 3, 3, 2, 2 },
      new String[] { "AA", "AAA", "AA", "BBB", "BB", "AAA", "CC", "DDD", "CCC", "DDDD" }, testAllocator);
    final BufferAllocator listVectorAllocator = testAllocator.newChildAllocator("test-output-vector-allocator", 0, Long.MAX_VALUE);
    final ListVector outputVector = ListVector.empty("test-output-vector", listVectorAllocator);

    // ACT
    flv.outputToListVector(outputVector, 0, 4);

    // ASSERT
    // Expected output:
    // 0: AA, AA, BB
    // 1: AAA, BBB, AAA
    // 2: CCC, DDDD
    // 3: CC, DDD
    final String[][] expectedValues = {
      { "AA", "AA", "BB"},
      { "AAA", "BBB", "AAA"},
      { "CCC", "DDDD" },
      { "CC", "DDD" }
    };

    assertListVector(expectedValues, outputVector);

    // TEARDOWN
    outputVector.clear();
    tempSpace.close();
    flv.close();
    listVectorAllocator.close();
  }

  private void TestAllowOverflow(boolean allowOneOverflow) {
    // Input Provided:
    // 0: "AA", "AA", "BB", "EE
    // 1: "AAA", "BBB", "AAA", "BBB"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD", "E"
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, MAX_VALUES_PER_BATCH);
    final FixedListVarcharVector flv = makeFlv("test-allow-overflow", MAX_VALUES_PER_BATCH, delimiter,
      SMALL_MAX_LIST_AGG_SIZE, false, false, false, allowOneOverflow, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2, 3, 3, 0, 1, 3 },
      new String[] { "AA", "AAA", "AA", "BBB", "BB", "AAA", "CC", "DDD", "CCC", "DDDD", "EE", "BBB", "E" }, testAllocator);

    // ACT
    flv.compact(false);

    if (allowOneOverflow) {
      // ASSERT
      // Expected output:
      // 0: "AA", "AA", "BB", 0xFFFF
      // 1: "AAA", "BBB", "AAA", 0xFFFF
      // 2: "CC", "DDD"
      // 3: "CCC", "DDDD", 0xFFFF
      final RowGroupValuePair[] expected = new RowGroupValuePair[] {
        new RowGroupValuePair(0, "AA"),
        new RowGroupValuePair(1, "AAA"),
        new RowGroupValuePair(0, "AA"),
        new RowGroupValuePair(1, "BBB"),
        new RowGroupValuePair(0, "BB"),
        new RowGroupValuePair(1, "AAA"),
        new RowGroupValuePair(2, "CC"),
        new RowGroupValuePair(2, "DDD"),
        new RowGroupValuePair(3, "CCC"),
        new RowGroupValuePair(3, "DDDD"),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      };

      assertFlv(expected, flv);
    } else {
      // ASSERT
      // Expected output:
      // 0: "AA", "AA", 0xFFFF, 0xFFFF
      // 1: "AAA", "BBB", 0xFFFF, 0xFFFF
      // 2: "CC", "DDD"
      // 3: "CCC", 0xFFFF, 0xFFFF
      final RowGroupValuePair[] expected = new RowGroupValuePair[] {
        new RowGroupValuePair(0, "AA"),
        new RowGroupValuePair(1, "AAA"),
        new RowGroupValuePair(0, "AA"),
        new RowGroupValuePair(1, "BBB"),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
        new RowGroupValuePair(2, "CC"),
        new RowGroupValuePair(2, "DDD"),
        new RowGroupValuePair(3, "CCC"),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
        new RowGroupValuePair(FixedListVarcharVector.DELETED_VALUE_ROW_GROUP, null),
      };

      assertFlv(expected, flv);
    }

    // TEARDOWN
    tempSpace.close();
    flv.close();
  }

  @Test
  public void TestAllowOverflow() {
    TestAllowOverflow(false);
    TestAllowOverflow(true);
  }

  private void addListVectorToFlv(final ListVector listVector, FixedListVarcharVector flv) {
    for (int i = 0; i < listVector.getValueCount(); i++) {
      if (!listVector.isNull(i)) {
        flv.addListVectorToRowGroup(i, listVector, i);
      }
    }
  }

  @Test
  public void TestFlvWithAndWithoutAllowOneOverFlow() {
    /**
     *  The test does the following.
     *  1. Insert values into flv which has allowOneOverFlow set to true.
     *  2. Compact and output to ListVector.
     *  3. Verify the ListVector output which would have an extra value.
     *  4. Use the ListVector as input to populate the contents of flv and compact. This flv would have allowOneOverFlow as false.
     *  5. Verify the VarCharVector output which would have just the required values.
     */

    // Input Provided:
    // 0: "AA", "AA", "BB", "EE
    // 1: "AAA", "BBB", "AAA", "BBB"
    // 2: null
    // 3: "CC", "DDD"
    // 4: null
    // 5: "CCC", "DDDD"
    final int SMALL_MAX_LIST_AGG_SIZE = 11;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, MAX_VALUES_PER_BATCH);
    final BufferAllocator listVectorAllocator = testAllocator.newChildAllocator("test-flv-with-and-without-allow-one-over-flow-buffer-allocator",
      0, Long.MAX_VALUE);
    final ListVector listVector = ListVector.empty("test-flv-with-and-without-allow-one-over-flow-list-vector", listVectorAllocator);
    final FixedListVarcharVector flv = makeFlv("test-flv-with-and-without-allow-one-over-flow-flv", MAX_VALUES_PER_BATCH, delimiter,
      SMALL_MAX_LIST_AGG_SIZE, false, false, false, true, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 1, 0, 1, 0, 1, 2, 2, 3, 3, 0, 1 },
      new String[] { "AA", "AAA", "AA", "BBB", "BB", "AAA", "CC", "DDD", "CCC", "DDDD", "EE", "BBB" }, testAllocator);

    // ACT
    flv.compact(true);
    flv.outputToListVector(listVector, 0, 4);

    // ASSERT
    // Expected output:
    // 0: "AA", "AA", "BB"
    // 1: "AAA", "BBB", "AAA"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD"
    final String[][] expectedValues = {
      { "AA", "AA", "BB"},
      { "AAA", "BBB", "AAA"},
      { "CC", "DDD" },
      { "CCC", "DDDD" }
    };

    assertListVector(expectedValues, listVector);
    assertTrue(flv.isOverflowSet(0));
    assertTrue(flv.isOverflowSet(1));
    assertFalse(flv.isOverflowSet(3));
    assertFalse(flv.isOverflowSet(5));

    final FixedListVarcharVector newFlv = makeFlv("test-flv-with-and-without-allow-one-over-flow-newflv",
      MAX_VALUES_PER_BATCH, delimiter, SMALL_MAX_LIST_AGG_SIZE, false, false, false,
      false, tempSpace, testAllocator);
    VarCharVector v1 = new VarCharVector("TestCopyOutVarchar", testAllocator);
    v1.allocateNew(flv.getRequiredByteCapacity(), TOTAL_ROWS);

    // INPUT ListVector
    // 0: "AA", "AA", "BB"
    // 1: "AAA", "BBB", "AAA"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD"
    addListVectorToFlv(listVector, newFlv);

    // ACT
    newFlv.compact(true);
    newFlv.outputToVector(v1, 0, 4);

    // ASSERT
    // Expected output:
    // 0: "AA","AA",...
    // 1: "AAA","BBB",...
    // 2: "CC", "DDD"
    // 3: "CCC",...
    String[] newExpected = new String[] {
      "AA" + delimiter + "AA" + delimiter + OVERFLOW_STRING,
      "AAA" + delimiter + "BBB" + delimiter + OVERFLOW_STRING,
      "CC" + delimiter + "DDD",
      "CCC" + delimiter + OVERFLOW_STRING
    };

    assertVarCharVector(newExpected, v1);
    assertTrue(newFlv.isOverflowSet(0));
    assertTrue(newFlv.isOverflowSet(1));
    assertFalse(newFlv.isOverflowSet(2));
    assertTrue(newFlv.isOverflowSet(3));

    flv.close();
    newFlv.close();
    v1.close();
    listVector.close();
    listVectorAllocator.close();
    tempSpace.close();
  }

  @Test
  public void TestAdjustMaxListAggSize() {
    // Input Provided:
    // 0: "AA", "AA", "BB"
    // 1: "AAA", "BBB", "AAA"
    // 2: "CC", "DDD"
    // 3: "CCC", "DDDD"
    final int SMALL_MAX_LIST_AGG_SIZE = 10;
    final String delimiter = ",";
    final ListVector tempSpace = FixedListVarcharVector.allocListVector(testAllocator, SMALL_MAX_LIST_AGG_SIZE);
    final BufferAllocator listVectorAllocator = testAllocator.newChildAllocator("test-adjust-max-listagg-size-allocator",
      0, Long.MAX_VALUE);
    final ListVector listVector = ListVector.empty("test-adjust-max-listagg-size-list-vector", listVectorAllocator);
    final FixedListVarcharVector flv = makeFlv("test-large-input-value", MAX_VALUES_PER_BATCH, delimiter,
      SMALL_MAX_LIST_AGG_SIZE, false, false, false, false, tempSpace, testAllocator);
    populateFixedListVarcharVector(flv, new int[] { 0, 0, 0, 1, 1, 1 },
      new String[] { "123456789012345", "123", "12345678901234567890123456789",
        "123", "123456789012345", "12345678901234567890123456789" }, testAllocator);

    // ACT
    flv.compact(true);
    flv.outputToListVector(listVector, 0, 2);

    // ASSERT
    // Expected output:
    // 0: "123456789012345"
    // 1: "123"
    String[][] expectedValues =  {
      { "123456789012345" },
      { "123" }
    };

    assertListVector(expectedValues, listVector);

    // TEARDOWN
    tempSpace.close();
    flv.close();
    listVector.close();
    listVectorAllocator.close();
  }
}
