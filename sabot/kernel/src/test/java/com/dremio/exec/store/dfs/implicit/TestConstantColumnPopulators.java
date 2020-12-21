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

import java.math.BigDecimal;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.DecimalUtility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.Lists;

/**
 * Tests for the constant column populators
 */
public class TestConstantColumnPopulators {
  @ClassRule
  public static final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  private static BufferAllocator bufferAllocator;
  private static OutputMutator outputMutator;

  @BeforeClass
  public static void setUp() {
    bufferAllocator = allocatorRule.newAllocator("test-constant-columns", 0, 2200000);
    outputMutator = new TestOutputMutator(bufferAllocator);
    outputMutator.addField(CompleteType.BIT.toField("bitCol"), BitVector.class);
    outputMutator.addField(CompleteType.INT.toField("intCol"), IntVector.class);
    outputMutator.addField(CompleteType.BIGINT.toField("bigintCol"), BigIntVector.class);
    outputMutator.addField(CompleteType.FLOAT.toField("floatCol"), Float4Vector.class);
    outputMutator.addField(CompleteType.DOUBLE.toField("doubleCol"), Float8Vector.class);
    outputMutator.addField(CompleteType.TIME.toField("timeCol"), TimeMilliVector.class);
    outputMutator.addField(CompleteType.DATE.toField("dateCol"), DateMilliVector.class);
    outputMutator.addField(CompleteType.TIMESTAMP.toField("timestampCol"), TimeStampMilliVector.class);
    outputMutator.addField(CompleteType.VARBINARY.toField("varbinaryCol"), VarBinaryVector.class);
    outputMutator.addField(CompleteType.VARCHAR.toField("varcharCol"), VarCharVector.class);
    outputMutator.addField(new Field("decimalCol", true, new ArrowType.Decimal(10, 2), null), DecimalVector.class);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(bufferAllocator);
  }

  private void verifyNonNullColumn(NameValuePair nameValuePair, VerifyValue verifyValue) throws Exception {
    AdditionalColumnsRecordReader.Populator populator = nameValuePair.createPopulator();
    populator.setup(outputMutator);
    int[] recordCounts = new int[] {8000, 10, 12000, 0};
    ValueVector valueVector = outputMutator.getVector(nameValuePair.getName());
    for(int i = 0; i < recordCounts.length; i++) {
      valueVector.reset();
      int recordCount = recordCounts[i];
      populator.allocate();
      populator.populate(recordCount);
      Assert.assertEquals(recordCount, valueVector.getValueCount());
      Assert.assertEquals(0, valueVector.getNullCount());
      for (int j = 0; j < valueVector.getValueCount(); j++) {
        Assert.assertTrue("Iteration " + i + ", Mismatch at " + j, verifyValue.checkValue(valueVector, i));
      }
      valueVector.close();
    }
    populator.close();
  }

  @Test
  public void testIntColumn() throws Exception {
    ConstantColumnPopulators.IntNameValuePair nameValuePair = new ConstantColumnPopulators.IntNameValuePair("intCol", 10);
    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        IntVector vector = (IntVector)valueVector;
        return (vector.get(i) == nameValuePair.getValue().intValue());
      }
    });
  }

  @Test
  public void testBigIntColumn() throws Exception {
    ConstantColumnPopulators.BigIntNameValuePair nameValuePair = new ConstantColumnPopulators.BigIntNameValuePair("bigintCol", 1000L);
    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        BigIntVector vector = (BigIntVector)valueVector;
        return (vector.get(i) == nameValuePair.getValue().longValue());
      }
    });
  }

  @Test
  public void testFloatColumn() throws Exception {
    NameValuePair nameValuePair = new ConstantColumnPopulators.Float4NameValuePair("floatCol", 1.2f);
    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        Float4Vector vector = (Float4Vector)valueVector;
        return (vector.get(i) == (float)nameValuePair.getValue());
      }
    });
  }

  @Test
  public void testDoubleColumn() throws Exception {
    NameValuePair nameValuePair = new ConstantColumnPopulators.Float8NameValuePair("doubleCol", 300.12d);
    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        Float8Vector vector = (Float8Vector)valueVector;
        return (vector.get(i) == (double)nameValuePair.getValue());
      }
    });
  }

  @Test
  public void testTimeColumn() throws Exception {
    NameValuePair nameValuePair = new ConstantColumnPopulators.TimeMilliNameValuePair("timeCol", 3_000);
    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        TimeMilliVector vector = (TimeMilliVector)valueVector;
        return (vector.get(i) == (int)nameValuePair.getValue());
      }
    });
  }

  @Test
  public void testTimestampColumn() throws Exception {
    NameValuePair nameValuePair = new ConstantColumnPopulators.TimeStampMilliNameValuePair("timestampCol", 3600000L);
    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        TimeStampMilliVector vector = (TimeStampMilliVector)valueVector;
        return (vector.get(i) == (long)nameValuePair.getValue());
      }
    });
  }

  @Test
  public void testDateColumn() throws Exception {
    NameValuePair nameValuePair = new ConstantColumnPopulators.DateMilliNameValuePair("dateCol", 3600000L);
    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        DateMilliVector vector = (DateMilliVector)valueVector;
        return (vector.get(i) == (long)nameValuePair.getValue());
      }
    });
  }

  @Test
  public void testBitColumn() throws Exception {
    ConstantColumnPopulators.BitNameValuePair nameValuePair = new ConstantColumnPopulators.BitNameValuePair("bitCol", true);
    int expectedValue = nameValuePair.getValue() ? 1 : 0;

    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        BitVector vector = (BitVector)valueVector;
        return (vector.get(i) == expectedValue);
      }
    });
  }

  private boolean compareBytes(byte[] buf1, byte[] buf2) {
    if (buf1.length != buf2.length) {
      return false;
    }

    for(int i = 0; i < buf1.length; i++) {
      if (buf1[i] != buf2[i]) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testVarBinaryColumn() throws Exception {
    byte[] bytes = new byte[] {'H', 'e', 'l', 'l', 'o'};
    NameValuePair nameValuePair = new ConstantColumnPopulators.VarBinaryNameValuePair("varbinaryCol", bytes);

    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        VarBinaryVector vector = (VarBinaryVector)valueVector;
        return compareBytes(vector.get(i), bytes);
      }
    });
  }

  @Test
  public void testVarCharColumn() throws Exception {
    String expectedValue = new String("hello");
    ConstantColumnPopulators.VarCharNameValuePair nameValuePair = new ConstantColumnPopulators.VarCharNameValuePair("varcharCol", expectedValue);

    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        VarCharVector vector = (VarCharVector)valueVector;
        return compareBytes(vector.get(i), expectedValue.getBytes());
      }
    });
  }

  private boolean compareArrowBufs(ArrowBuf buf1, ArrowBuf buf2) {
    if (buf1.capacity() != buf2.capacity()) {
      return false;
    }

    for(int i = 0; i < buf1.capacity(); i++) {
      if (buf1.getByte(i) != buf2.getByte(i)) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testDecimalColumn() throws Exception {
    BigDecimal bigDecimal = new BigDecimal("12.3");
    ArrowBuf arrowBuf = bufferAllocator.buffer(16);
    DecimalUtility.writeBigDecimalToArrowBuf(bigDecimal, arrowBuf, 0, DecimalVector.TYPE_WIDTH);
    DecimalHolder holder = new DecimalHolder();
    holder.precision = 10;
    holder.scale = 2;
    holder.buffer = arrowBuf;
    holder.start = 0;
    NameValuePair nameValuePair = new ConstantColumnPopulators.DecimalNameValuePair("decimalCol", holder);

    verifyNonNullColumn(nameValuePair, new VerifyValue() {
      @Override
      public boolean checkValue(ValueVector valueVector, int i) {
        DecimalVector vector = (DecimalVector)valueVector;
        return compareArrowBufs(vector.get(i), arrowBuf);
      }
    });

    arrowBuf.release();
  }

  private void verifyNullColumn(NameValuePair nameValuePair) throws Exception {
    AdditionalColumnsRecordReader.Populator populator = nameValuePair.createPopulator();
    populator.setup(outputMutator);
    int[] recordCounts = new int[] {8000, 10, 12000, 0};

    ValueVector valueVector = outputMutator.getVector(nameValuePair.getName());
    for(int i = 0; i < recordCounts.length; i++) {
      valueVector.reset();
      int recordCount = recordCounts[i];
      populator.allocate();
      populator.populate(recordCount);
      Assert.assertEquals(recordCount, valueVector.getValueCount());
      Assert.assertEquals(recordCount, valueVector.getNullCount());
      valueVector.close();
    }
    populator.close();
  }

  private void verifyAllNullColumns(List<NameValuePair> nameValuePairs) throws Exception {
    for(NameValuePair nameValuePair : nameValuePairs) {
      verifyNullColumn(nameValuePair);
    }
  }

  @Test
  public void testNullColumns() throws Exception {
    NameValuePair intNameValuePair = new ConstantColumnPopulators.IntNameValuePair("intCol", null);
    NameValuePair bigIntNameValuePair = new ConstantColumnPopulators.BigIntNameValuePair("bigintCol", null);
    NameValuePair floatValuePair = new ConstantColumnPopulators.Float4NameValuePair("floatCol", null);
    NameValuePair doubleValuePair = new ConstantColumnPopulators.Float8NameValuePair("doubleCol", null);
    NameValuePair timeValuePair = new ConstantColumnPopulators.TimeMilliNameValuePair("timeCol", null);
    NameValuePair timestampValuePair = new ConstantColumnPopulators.TimeStampMilliNameValuePair("timestampCol", null);
    NameValuePair dateValuePair = new ConstantColumnPopulators.DateMilliNameValuePair("dateCol", null);
    NameValuePair bitValuePair = new ConstantColumnPopulators.BitNameValuePair("bitCol", null);
    NameValuePair varBinaryValuePair = new ConstantColumnPopulators.VarBinaryNameValuePair("varbinaryCol", null);
    NameValuePair varcharValuePair = new ConstantColumnPopulators.VarCharNameValuePair("varcharCol", null);
    NameValuePair decimalValuePair = new ConstantColumnPopulators.DecimalNameValuePair("decimalCol", null);

    verifyAllNullColumns(Lists.newArrayList(intNameValuePair, bigIntNameValuePair, floatValuePair, doubleValuePair, timeValuePair,
      timestampValuePair, dateValuePair, bitValuePair, varBinaryValuePair, varcharValuePair, decimalValuePair));
  }

  interface VerifyValue {
    boolean checkValue(ValueVector valueVector, int i);
  }
}
