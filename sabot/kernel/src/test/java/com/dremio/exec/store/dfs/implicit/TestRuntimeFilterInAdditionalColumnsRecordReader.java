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
package com.dremio.exec.store.dfs.implicit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.DecimalUtils;
import com.dremio.sabot.BaseTestWithAllocator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.OutputMutator;

public class TestRuntimeFilterInAdditionalColumnsRecordReader extends BaseTestWithAllocator {

  private final static String TEST_NAME = "20ed4177-87c7-91cc-c869-82b1d90cd300:frag:1:3";


  private void testRuntimeFilter(ArrowBuf keyBuf, int keyLength, String partitionColumn, NameValuePair nameValuePair) throws Exception {
    testRuntimeFilter(keyBuf, keyLength, Collections.singletonList(partitionColumn), Collections.singletonList(nameValuePair));
  }

  private void testRuntimeFilter(ArrowBuf keyBuf, int keyLength, List<String> partitionCols, List<NameValuePair> nameValuePairs) throws Exception {
    try (BloomFilter bloomFilter = new BloomFilter(allocator, TEST_NAME, 512)) {
      bloomFilter.setup();
      bloomFilter.put(keyBuf, keyLength);

      // create mocks
      OperatorContext context = mock(OperatorContext.class);
      OperatorStats stats = mock(OperatorStats.class);
      when(context.getStats()).thenReturn(stats);
      RecordReader recordReader = mock(RecordReader.class);

      List<NameValuePair<?>> pairs = new ArrayList<>();
      for (NameValuePair<?> nameValuePair : nameValuePairs) {
        NameValuePair<?> pair = spy(nameValuePair);
        when(pair.createPopulator()).thenReturn(mock(AdditionalColumnsRecordReader.Populator.class));
        pairs.add(pair);
      }
      OutputMutator outputMutator = mock(OutputMutator.class);

      // create AdditionalColumnsRecordReader
      AdditionalColumnsRecordReader additionalColumnsRecordReader = new AdditionalColumnsRecordReader(context, recordReader, pairs, allocator);
      additionalColumnsRecordReader.setup(outputMutator);

      // dummy runtimeFilter
      CompositeColumnFilter partitionColumnFilter = new CompositeColumnFilter.Builder()
        .setColumnsList(partitionCols)
        .setBloomFilter(bloomFilter)
        .setFilterType(CompositeColumnFilter.RuntimeFilterType.BLOOM_FILTER)
        .build();
      RuntimeFilter runtimeFilter = new RuntimeFilter(partitionColumnFilter, null, "");

      // bloomfilter contains the key, can't skip the partition
      additionalColumnsRecordReader.addRuntimeFilter(runtimeFilter);
      Assert.assertFalse(additionalColumnsRecordReader.skipPartition());

      /*
       * The following assertion where skipPartition is true (in this test and subsequent tests) assumes bloom filter returns
       * negative. Since there is a possibility of bloom filter giving false positive, if we ever encounter this part to sporadically fail, we
       * should comment it out.
       */
      // clear the bloom filter
      // bloom filter doesn't contain the partition values, can skip the partition
      unsetAllBits(bloomFilter);
      additionalColumnsRecordReader.addRuntimeFilter(runtimeFilter);
      Assert.assertTrue(additionalColumnsRecordReader.skipPartition());

      additionalColumnsRecordReader.close();
    }
  }

  @Test
  public void testRuntimeFilterWithSingleIntColumn() throws Exception {
    try (ArrowBuf keyBuf = allocator.buffer(5)) {
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
    try (ArrowBuf keyBuf = allocator.buffer(9)) {
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
    try (ArrowBuf keyBuf = allocator.buffer(1)) {
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
    try (ArrowBuf keyBuf = allocator.buffer(32)) {
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
    try (ArrowBuf keyBuf = allocator.buffer(17);
         ArrowBuf decimalBuf = allocator.buffer(16)) {
      String partitionColumn = "decimalcol";

      BigDecimal bigDecimal = BigDecimal.valueOf(1234, 2);
      byte[] partitionValue = DecimalUtils.convertBigDecimalToArrowByteArray(bigDecimal);
      keyBuf.setByte(0, 1);
      keyBuf.setBytes(1, partitionValue);
      TwosComplementValuePair pair = new TwosComplementValuePair(allocator, Field.nullable(partitionColumn, new ArrowType.Decimal(38, 2)), bigDecimal.unscaledValue().toByteArray());
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
    try (ArrowBuf keyBuf = allocator.buffer(32)) {
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
    try (ArrowBuf keyBuf = allocator.buffer(13)) {
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
    try (ArrowBuf keyBuf = allocator.buffer(32)) {
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
    try (ArrowBuf keyBuf = allocator.buffer(5)) {
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

  private void unsetAllBits(BloomFilter bloomFilter) {
    ArrowBuf buf = bloomFilter.getDataBuffer();
    for (long i = 0; i < buf.capacity(); i+=8) {
      buf.writerIndex(i);
      buf.writeLong(0);
    }
  }
}
