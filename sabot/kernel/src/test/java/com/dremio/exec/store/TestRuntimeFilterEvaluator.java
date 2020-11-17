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
package com.dremio.exec.store;

import static org.mockito.Mockito.spy;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.dfs.implicit.TwosComplementValuePair;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.DecimalUtils;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.test.AllocatorRule;

/**
 * Tests for {@link RuntimeFilterEvaluator}
 */
public class TestRuntimeFilterEvaluator {
  private final static String TEST_NAME = "20ed4177-87c7-91cc-c869-82b1d90cd300:frag:1:3";

  private BufferAllocator testAllocator;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-runtime_filter_evaluator", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() {
    testAllocator.close();
  }

  private void testRuntimeFilter(ArrowBuf keyBuf, int keyLength, String partitionColumn, NameValuePair nameValuePair) {
    testRuntimeFilter(keyBuf, keyLength, Collections.singletonList(partitionColumn), Collections.singletonList(nameValuePair));
  }

  private void testRuntimeFilter(ArrowBuf keyBuf, int keyLength, List<String> partitionCols, List<NameValuePair<?>> nameValuePairs) {
    try (BloomFilter bloomFilter = new BloomFilter(testAllocator, TEST_NAME, 512)) {
      bloomFilter.setup();
      bloomFilter.put(keyBuf, keyLength);

      // create mocks
      OpProfileDef prof = new OpProfileDef(1, 1, 1);
      final OperatorStats stats = new OperatorStats(prof, testAllocator);

      List<NameValuePair<?>> pairs = new ArrayList<>();
      for (NameValuePair<?> nameValuePair : nameValuePairs) {
        NameValuePair<?> pair = spy(nameValuePair);
        pairs.add(pair);
      }

      // dummy runtimeFilter
      CompositeColumnFilter partitionColumnFilter = new CompositeColumnFilter.Builder()
        .setColumnsList(partitionCols)
        .setBloomFilter(bloomFilter)
        .setFilterType(CompositeColumnFilter.RuntimeFilterType.BLOOM_FILTER)
        .build();
      RuntimeFilter runtimeFilter = new RuntimeFilter(partitionColumnFilter, null, "");

      // bloomfilter contains the key, can't skip the partition
      RuntimeFilterEvaluator filterEvaluator = new RuntimeFilterEvaluator(testAllocator, stats, runtimeFilter);
      Assert.assertFalse(filterEvaluator.canBeSkipped(null, nameValuePairs));
      Assert.assertEquals(0L, stats.getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));

      /*
       * The following assertion where skipPartition is true (in this test and subsequent tests) assumes bloom filter returns
       * negative. Since there is a possibility of bloom filter giving false positive, if we ever encounter this part to sporadically fail, we
       * should comment it out.
       */
      // clear the bloom filter
      // bloom filter doesn't contain the partition values, can skip the partition
      bloomFilter.getDataBuffer().setZero(0, bloomFilter.getDataBuffer().capacity());
      Assert.assertTrue(filterEvaluator.canBeSkipped(null, nameValuePairs));
      Assert.assertEquals(1L, stats.getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
    }
  }

  @Test
  public void testRuntimeFilterWithSingleIntColumn() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(5)) {
      String partitionColumn = "intcol";

      Integer partitionValue = 1234567; // partition value in partition column
      keyBuf.setInt(1, partitionValue);
      keyBuf.setByte(0, 1);
      testRuntimeFilter(keyBuf, 5, partitionColumn, new ConstantColumnPopulators.IntNameValuePair(partitionColumn, partitionValue));

      partitionValue = null; // partition value in partition column
      keyBuf.setBytes(0, new byte[5]);
      testRuntimeFilter(keyBuf, 5, partitionColumn, new ConstantColumnPopulators.IntNameValuePair(partitionColumn, partitionValue));
    }
  }


  @Test
  public void testRuntimeFilterWithSingleDoubleColumn() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(9)) {
      String partitionColumn = "doublecol";

      Double partitionValue = 123.45; // partition value in partition column
      keyBuf.setDouble(1, partitionValue);
      keyBuf.setByte(0, 1);
      testRuntimeFilter(keyBuf, 9, partitionColumn, new ConstantColumnPopulators.Float8NameValuePair(partitionColumn, partitionValue));

      partitionValue = null; // partition value in partition column
      keyBuf.setBytes(0, new byte[9]);
      testRuntimeFilter(keyBuf, 9, partitionColumn, new ConstantColumnPopulators.Float8NameValuePair(partitionColumn, partitionValue));

    }
  }

  @Test
  public void testRuntimeFilterWithSingleBooleanColumn() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(1)) {
      String partitionColumn = "booleancol";

      Boolean partitionValue = true; // partition value in partition column
      keyBuf.setByte(0, 3/*11b*/);
      testRuntimeFilter(keyBuf, 1, partitionColumn, new ConstantColumnPopulators.BitNameValuePair(partitionColumn,partitionValue));

      partitionValue = null; // partition value in partition column
      keyBuf.setByte(0, 0);
      testRuntimeFilter(keyBuf, 1, partitionColumn, new ConstantColumnPopulators.BitNameValuePair(partitionColumn,partitionValue));

    }
  }

  @Test
  public void testRuntimeFilterWithSingleVarcharColumn() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(32)) {
      String partitionColumn = "varcharcol";

      String partitionValue = "abc"; // partition value in partition column
      keyBuf.setByte(0, 1);
      keyBuf.setBytes(1, new byte[28]);
      keyBuf.setBytes(29, partitionValue.getBytes(StandardCharsets.UTF_8));
      testRuntimeFilter(keyBuf, 32, partitionColumn, new ConstantColumnPopulators.VarCharNameValuePair(partitionColumn, partitionValue));

      partitionValue = null; // partition value in partition column
      keyBuf.setBytes(0, new byte[32]);
      testRuntimeFilter(keyBuf, 32, partitionColumn, new ConstantColumnPopulators.VarCharNameValuePair(partitionColumn, partitionValue));

      partitionValue = "abcedfghijklmnopqrstuvwxyz"; // partition value in partition column
      keyBuf.setByte(0, 1);
      keyBuf.setBytes(1, new byte[5]);
      keyBuf.setBytes(6, partitionValue.getBytes(StandardCharsets.UTF_8));
      testRuntimeFilter(keyBuf, 32, partitionColumn, new ConstantColumnPopulators.VarCharNameValuePair(partitionColumn, partitionValue));
    }
  }

  @Test
  public void testRuntimeFilterWithSingleDecimalColumn() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(17);
         ArrowBuf decimalBuf = testAllocator.buffer(16)) {
      String partitionColumn = "decimalcol";

      BigDecimal bigDecimal = BigDecimal.valueOf(1234, 2);
      byte[] partitionValue = DecimalUtils.convertBigDecimalToArrowByteArray(bigDecimal);
      keyBuf.setByte(0, 1);
      keyBuf.setBytes(1, partitionValue);
      TwosComplementValuePair pair = new TwosComplementValuePair(testAllocator, Field.nullable(partitionColumn, new ArrowType.Decimal(38, 2)), bigDecimal.unscaledValue().toByteArray());
      testRuntimeFilter(keyBuf, 17, partitionColumn, pair);
      pair.getBuf().close();

      decimalBuf.setBytes(0, partitionValue);
      DecimalHolder holder = new DecimalHolder();
      holder.buffer = decimalBuf;
      testRuntimeFilter(keyBuf, 17, partitionColumn, new ConstantColumnPopulators.DecimalNameValuePair(partitionColumn, holder));
    }
  }

  @Test
  public void testRuntimeFilterWithTwoVarcharColumns() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(32)) {
      String partitionCol1 = "varcharcol1";
      String partitionCol2 = "varcharcol2";

      keyBuf.setByte(0, 3 /*11b*/);
      keyBuf.setBytes(1, "ABCDEFGHIJKLMNO".getBytes(StandardCharsets.UTF_8));
      keyBuf.setBytes(16, new byte[13]);
      keyBuf.setBytes(29, "XYZ".getBytes(StandardCharsets.UTF_8));

      NameValuePair<?> pair1 = new ConstantColumnPopulators.VarCharNameValuePair(partitionCol1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
      NameValuePair<?> pair2 = new ConstantColumnPopulators.VarCharNameValuePair(partitionCol2, "XYZ");
      testRuntimeFilter(keyBuf, 32, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));

      pair2 = new ConstantColumnPopulators.VarCharNameValuePair(partitionCol2, null);
      keyBuf.setByte(0, 1);
      keyBuf.setBytes(16, new byte[16]);
      testRuntimeFilter(keyBuf, 32, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));
    }
  }

  @Test
  public void testRuntimeFilterWithLongAndIntColumns() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(13)) {
      String partitionCol1 = "intcol";
      String partitionCol2 = "longcol";

      Integer partitionValue1 = 1234;
      Long partitionValue2 = 5678l;

      NameValuePair<?> pair1 = new ConstantColumnPopulators.IntNameValuePair(partitionCol1, partitionValue1);
      NameValuePair<?> pair2 = new ConstantColumnPopulators.BigIntNameValuePair(partitionCol2, partitionValue2);
      keyBuf.setByte(0, 3/*11b*/);
      keyBuf.setInt(1, partitionValue1);
      keyBuf.setLong(5, partitionValue2);
      testRuntimeFilter(keyBuf, 13, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));

      partitionValue2 = null;
      pair2 = new ConstantColumnPopulators.BigIntNameValuePair(partitionCol2, partitionValue2);
      keyBuf.setByte(0, 1);
      keyBuf.setBytes(5, new byte[8]);
      testRuntimeFilter(keyBuf, 13, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));
    }
  }

  @Test
  public void testRuntimeFilterWithIntAndVarcharColumns() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(32)) {
      String partitionCol1 = "intcol";
      String partitionCol2 = "varcharcol";

      int partitionValue1 = 1234;
      String partitionValue2 = "abc";

      NameValuePair<?> pair1 = new ConstantColumnPopulators.IntNameValuePair(partitionCol1, partitionValue1);
      NameValuePair<?> pair2 = new ConstantColumnPopulators.VarCharNameValuePair(partitionCol2, partitionValue2);
      keyBuf.setByte(0, 3/*11b*/);
      keyBuf.setInt(1, partitionValue1);
      keyBuf.setBytes(5, new byte[24]);
      keyBuf.setBytes(29, partitionValue2.getBytes(StandardCharsets.UTF_8));
      testRuntimeFilter(keyBuf, 32, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));

      partitionValue2 = null;
      pair2 = new ConstantColumnPopulators.VarCharNameValuePair(partitionCol2, partitionValue2);
      keyBuf.setByte(0, 1);
      keyBuf.setBytes(5, new byte[27]);
      testRuntimeFilter(keyBuf, 32, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));
    }
  }

  @Test
  public void testRuntimeFilterWithIntAndBooleanColumns() throws Exception {
    try (ArrowBuf keyBuf = testAllocator.buffer(5)) {
      String partitionCol1 = "intcol";
      String partitionCol2 = "bitcol";

      Integer partitionValue1 = 1234;
      Boolean partitionValue2 = false;

      NameValuePair<?> pair1 = new ConstantColumnPopulators.IntNameValuePair(partitionCol1, partitionValue1);
      NameValuePair<?> pair2 = new ConstantColumnPopulators.BitNameValuePair(partitionCol2, partitionValue2);
      keyBuf.setByte(0, 3/*011b*/);
      keyBuf.setInt(1, partitionValue1);
      testRuntimeFilter(keyBuf, 5, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));

      partitionValue2 = null;
      pair2 = new ConstantColumnPopulators.BitNameValuePair(partitionCol2, partitionValue2);
      keyBuf.setByte(0, 1);
      testRuntimeFilter(keyBuf, 5, Arrays.asList(partitionCol1, partitionCol2), Arrays.asList(pair1, pair2));
    }
  }
}
