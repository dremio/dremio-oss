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
package com.dremio.dac.explore.model;

import static com.dremio.exec.record.RecordBatchHolder.newRecordBatchHolder;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.common.AutoCloseables;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataFragmentWrapper;
import com.dremio.dac.model.job.JobDataFragmentWrapper.JobDataFragmentSerializer;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.record.VectorContainer;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobDataFragmentImpl;
import com.dremio.service.jobs.RecordBatches;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Unittests for {@link com.dremio.dac.model.job.JobDataFragmentWrapper}. Currently it only tests the serialization aspects such as
 * <ul>
 *   <li>able to serialize all types of ValueVectors</li>
 *   <li>truncate large cell values</li>
 *   <li>providing URL to fetch the complete cell value for truncated cell values in results</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class TestData extends DremioTest {
  private static final JobId TEST_JOB_ID = new JobId("testJobId");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    SimpleModule module = new SimpleModule();
    module.addSerializer(JobDataFragmentWrapper.class, new JobDataFragmentSerializer());
    OBJECT_MAPPER.registerModule(module);
  }

  @ClassRule
  public static final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private static BufferAllocator allocator;
  private static ArrowBuf tempBuf;

  private static String defaultMaxCellLength;

  private static final String BIGINT_COL = "nBigIntCol";
  private static final String VARCHAR_COL = "nVarCharCol";
  private static final String LIST_COL = "nListCol";
  private static final String MAP_COL = "nMapCol";
  private static final String UNION_COL = "nUnionCol";

  @Parameterized.Parameters(name = "numbers as strings = {0}")
  public static Collection<Object> data() {
    return asList(new Object[] { true, false, null });
  }

  @BeforeClass
  public static void setMaxCellLength() {
    allocator = allocatorRule.newAllocator("test-data", 0, Long.MAX_VALUE);
    tempBuf = allocator.buffer(1024);
    // For testing purposes set the value to 30
    defaultMaxCellLength = System.getProperty(JobData.MAX_CELL_SIZE_KEY);
    System.setProperty(JobData.MAX_CELL_SIZE_KEY, "30");
  }

  @AfterClass
  public static void resetMaxCellLength() {
    if (defaultMaxCellLength != null) {
      System.setProperty(JobData.MAX_CELL_SIZE_KEY, defaultMaxCellLength);
    }
    allocator.close();
  }

  private final boolean convertNumbersToStrings;

  public TestData(Boolean convertNumbersToStrings) {
    this.convertNumbersToStrings = convertNumbersToStrings == null ? false : convertNumbersToStrings.booleanValue();
    if (convertNumbersToStrings == null) {
      OBJECT_MAPPER.setConfig(OBJECT_MAPPER.getSerializationConfig()
        .withoutAttribute(DataJsonOutput.DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_ATTRIBUTE));
    } else {
      OBJECT_MAPPER.setConfig(OBJECT_MAPPER.getSerializationConfig()
        .withAttribute(DataJsonOutput.DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_ATTRIBUTE, convertNumbersToStrings.booleanValue()));
    }
  }

  @Test
  public void serializeBoolean() throws Exception {
    helper(testBitVector(0, 0), "colBit", false, false);
  }

  @Test
  public void serializeTinyInt() throws Exception {
    helper(testTinyIntVector(), "colTinyInt", false, false);
  }

  @Test
  public void serializeSmallInt() throws Exception {
    helper(testSmallIntVector(), "colSmallInt", false, false);
  }

  @Test
  public void serializeInt() throws Exception {
    helper(testIntVector(), "colInt", false, false);
  }

  @Test
  public void serializeBigInt() throws Exception {
    helper(testBigIntVector(), "colBigInt", false, false);
  }

  @Test
  public void serializeFloat4() throws Exception {
    helper(testFloat4Vector(), "colFloat4", false, false);
  }

  @Test
  public void serializeFloat8() throws Exception {
    helper(testFloat8Vector(), "colFloat8", false, false);
  }

  @Test
  public void serializeDate() throws Exception {
    helper(testDateMilliVector(0, 0), "colDate", false, false);
  }

  @Test
  public void serializeTime() throws Exception {
    helper(testTimeVector(0, 0), "colTime", false, false);
  }

  @Test
  public void serializeTimeStamp() throws Exception {
    helper(testTimeStampVector(0, 0), "colTimeStamp", false, false);
  }

  @Test
  public void serializeIntervalYear() throws Exception {
    helper(testIntervalYearVector(0, 0), "colIntervalYear", false, false);
  }

  @Test
  public void serializeIntervalDay() throws Exception {
    helper(testIntervalDayVector(0, 0), "colIntervalDay", false, false);
  }

  @Test
  public void serializeDecimal() throws Exception {
    helper(testDecimalVector(), "colDecimal", false, false);
  }

  @Test
  public void serializeVarChar() throws Exception {
    helper(testVarCharVector(0, 0), "colVarChar", true, false);
  }

  @Test
  public void serializeVarBinary() throws Exception {
    helper(testVarBinaryVector(0, 0), "colVarBinary", true, false);
  }

  @Test
  public void serializeMap() throws Exception {
    helper(testMapVector(0, 0), "colMap", true, false);
  }

  @Test
  public void serializeList() throws Exception {
    helper(testListVector(0, 0), "colList", true, false);
  }

  @Test
  public void serializeUnion() throws Exception {
    helper(testUnionVector(0, 0), "colUnion", true, true);
  }

  @Test
  public void serializeAllTypesAtOnce() throws Exception {
    List<Pair<? extends ValueVector, ResultVerifier>> pairs = Lists.newArrayList();
    pairs.add(testBitVector(0, 0));
    pairs.add(testTinyIntVector());
    pairs.add(testSmallIntVector());
    pairs.add(testIntVector());
    pairs.add(testBigIntVector());
    pairs.add(testFloat4Vector());
    pairs.add(testFloat8Vector());
    pairs.add(testDateMilliVector(0, 0));
    pairs.add(testTimeVector(0, 0));
    pairs.add(testTimeStampVector(0, 0));
    pairs.add(testIntervalYearVector(0, 0));
    pairs.add(testIntervalDayVector(0, 0));
    pairs.add(testDecimalVector());
    pairs.add(testVarCharVector(0, 0));
    pairs.add(testVarBinaryVector(0, 0));
    pairs.add(testMapVector(0, 0));
    pairs.add(testListVector(0, 0));
    pairs.add(testUnionVector(0, 0));

    List<ValueVector> vvs = Lists.newArrayList(
        Iterables.transform(pairs, new Function<Pair<? extends ValueVector, ResultVerifier>, ValueVector>() {
          @Nullable
          @Override
          public ValueVector apply(@Nullable Pair<? extends ValueVector, ResultVerifier> input) {
            return input.getKey();
          }
        })
    );

    try(com.dremio.dac.model.job.JobDataFragment dataInput = createDataObject(vvs.toArray(new ValueVector[pairs.size()]))) {
      DataPOJO dataOutput = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(dataInput), DataPOJO.class);
      assertEquals(dataInput.getColumns().toString(), dataOutput.getColumns().toString());
      assertEquals(dataInput.getReturnedRowCount(), dataOutput.getReturnedRowCount());

      for(Pair<? extends ValueVector, ResultVerifier> pair : pairs) {
        pair.getValue().verify(dataOutput);
      }
    }
  }

  private static void helper(Pair<? extends ValueVector, ResultVerifier> testPair, String colName, boolean potentialTruncation, boolean isUnion) throws Exception {
    try(JobDataFragment dataInput = createDataObject(testPair.getKey())) {
      DataPOJO dataOutput = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(dataInput), DataPOJO.class);
      assertEquals(dataInput.getColumns().toString(), dataOutput.getColumns().toString());
      assertEquals(dataInput.getReturnedRowCount(), dataOutput.getReturnedRowCount());

      for(int i=0; i<dataOutput.getReturnedRowCount(); i++) {
        if (!isUnion) {
          assertNull(dataOutput.extractType(colName, i));
        }

        if (!potentialTruncation) {
          assertNull(dataOutput.extractUrl(colName, i));
        }
      }

      testPair.getValue().verify(dataOutput);
    }
  }

  private static com.dremio.dac.model.job.JobDataFragment createDataObject(ValueVector... vv) {
    RecordBatchData batch = createRecordBatch(vv);

    return new JobDataFragmentWrapper(0,  new JobDataFragmentImpl(
        new RecordBatches(asList(newRecordBatchHolder(batch, 0, batch.getRecordCount()))), 0, TEST_JOB_ID));
  }


  private static RecordBatchData createRecordBatch(ValueVector... vv) {
    VectorContainer container = new VectorContainer();
    container.addCollection(asList(vv));
    container.setRecordCount(5);
    container.buildSchema(SelectionVectorMode.NONE);

    return new RecordBatchData(container, allocator);
  }

  interface ResultVerifier {
    void verify(DataPOJO output);
  }

  private static Pair<BitVector, ResultVerifier> testBitVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    BitVector colBitV = new BitVector("colBit", allocator);
    colBitV.allocateNew(5);
    colBitV.set(0, 1);
    colBitV.set(1, 0);
    colBitV.setNull(2);
    colBitV.set(3, 1);
    colBitV.set(4, 1);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertTrue((Boolean)output.extractValue("colBit", index++));
        assertFalse((Boolean)output.extractValue("colBit", index++));
        assertNull(output.extractValue("colBit", index++));
        assertTrue((Boolean)output.extractValue("colBit", index++));
        assertTrue((Boolean)output.extractValue("colBit", index));
      }
    };

    return Pair.of(colBitV, verifier);
  }

  private Object getExpectedNumber(Number value) {
    if (value == null) {
      return null;
    }
    if (convertNumbersToStrings) {
      return value.toString();
    }
    return value;
  }

  private <T extends Number> void verifyIntValues(List<T> values, DataPOJO output, String colName) {
    for (int i = 0; i < values.size(); i++) {
      T value = values.get(i);

      if (value == null) {
        assertNull(output.extractValue(colName, i));
      } else {
        // always compare as strings, as there could be type mismatches. For example, if we send (byte)0 or 0L, returned
        // type would be integer
        assertEquals(value.toString(), output.extractValue(colName, i).toString());
      }
    }
  }

  private <T extends Number> void verifyDoubleValues(List<T> values, DataPOJO output, String colName, double accuracy) {
    for (int i = 0; i < values.size(); i++) {
      T value = values.get(i);

      if (value == null) {
        assertNull(output.extractValue(colName, i));
      } else {
        verifyDoubleValue(value, output, colName, i, accuracy);
      }
    }
  }

  private <T extends Number> void verifyDoubleValue(T expectedValue, DataPOJO output, String colName, int index, double accuracy) {
    final Object actualValue = output.extractValue(colName, index);
    if (convertNumbersToStrings) {
      assertEquals(expectedValue.toString(), actualValue.toString());
    } else {
      assertEquals(expectedValue.doubleValue(), ((Double)actualValue).doubleValue(), accuracy);
    }
  }

  private Pair<TinyIntVector, ResultVerifier> testTinyIntVector() {
    final String colName = "colTinyInt";
    final List<Byte> values =  asList((byte)0, (byte)-1, (byte)1, null, (byte)-54);

    TinyIntVector valuesVector = new TinyIntVector(colName, allocator);
    valuesVector.allocateNew(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        valuesVector.setNull(i);
      } else {
        valuesVector.set(i, values.get(i));
      }
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        verifyIntValues(values, output, colName);
      }
    };

    return Pair.of(valuesVector, verifier);
  }

  private Pair<SmallIntVector, ResultVerifier> testSmallIntVector() {
    final String colName = "colSmallInt";
    final List<Integer> values =  asList(20, null, -2000, 32700, 0);

    SmallIntVector valuesVector = new SmallIntVector(colName, allocator);
    valuesVector.allocateNew(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        valuesVector.setNull(i);
      } else {
        valuesVector.set(i, values.get(i));
      }
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        verifyIntValues(values, output, colName);
      }
    };

    return Pair.of(valuesVector, verifier);
  }

  private Pair<IntVector, ResultVerifier> testIntVector() {
    final String colName = "colInt";
    final List<Integer> values =  asList(20, 50, -2000, 327345, null);

    IntVector valuesVector = new IntVector(colName, allocator);
    valuesVector.allocateNew(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        valuesVector.setNull(i);
      } else {
        valuesVector.set(i, values.get(i));
      }
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        verifyIntValues(values, output, colName);
      }
    };

    return Pair.of(valuesVector, verifier);
  }

  private Pair<BigIntVector, ResultVerifier> testBigIntVector() {
    final String colName = "colBigInt";
    final List<Long> values = asList(null, 50L, -2000L, 327345234234L, 0L);

    BigIntVector valuesVector = new BigIntVector(colName, allocator);
    valuesVector.allocateNew(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        valuesVector.setNull(i);
      } else {
        valuesVector.set(i, values.get(i));
      }
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        verifyIntValues(values, output, colName);
      }
    };

    return Pair.of(valuesVector, verifier);
  }

  private Pair<Float4Vector, ResultVerifier> testFloat4Vector() {
    final String colName = "colFloat4";
    final List<Float> values = asList(20.0f, 50.023f, -238423f, null, 0f);

    Float4Vector valuesVector = new Float4Vector(colName, allocator);
    valuesVector.allocateNew(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        valuesVector.setNull(i);
      } else {
        valuesVector.set(i, values.get(i));
      }
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        verifyDoubleValues(values, output, colName, 0.01f);
      }
    };

    return Pair.of(valuesVector, verifier);
  }

  private Pair<Float8Vector, ResultVerifier> testFloat8Vector() {
    final String colName = "colFloat8";
    final List<Double> values = asList(20.2345234d, 503453.023d, -238423.3453453d, 3273452.345324563245d, null);
    Float8Vector valuesVector = new Float8Vector(colName, allocator);
    valuesVector.allocateNew(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        valuesVector.setNull(i);
      } else {
        valuesVector.set(i, values.get(i));
      }
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        verifyDoubleValues(values, output, colName, 0.01f);
      }
    };

    return Pair.of(valuesVector, verifier);
  }

  private static Pair<DateMilliVector, ResultVerifier> testDateMilliVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    DateMilliVector colDateV = new DateMilliVector("colDate", allocator);
    colDateV.allocateNew(5);
    colDateV.set(0, 234);
    colDateV.set(1, -2342);
    colDateV.setNull(2);
    colDateV.set(3, 384928359245L);
    colDateV.set(4, 2342893433L);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals("1970-01-01", output.extractValue("colDate", index++));
        assertEquals("1969-12-31", output.extractValue("colDate", index++));
        assertNull(output.extractValue("colDate", index++));
        assertEquals("1982-03-14", output.extractValue("colDate", index++));
        assertEquals("1970-01-28", output.extractValue("colDate", index++));
      }
    };

    return Pair.of(colDateV, verifier);
  }

  private static Pair<TimeMilliVector, ResultVerifier> testTimeVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    TimeMilliVector colTimeV = new TimeMilliVector("colTime", allocator);
    colTimeV.allocateNew(5);
    colTimeV.set(0, 23423234);
    colTimeV.set(1, -234223);
    colTimeV.set(2, 34534345);
    colTimeV.setNull(3);
    colTimeV.set(4, 23434);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals("06:30:23", output.extractValue("colTime", index++));
        assertEquals("23:56:05", output.extractValue("colTime", index++));
        assertEquals("09:35:34", output.extractValue("colTime", index++));
        assertNull(output.extractValue("colTime", index++));
        assertEquals("00:00:23", output.extractValue("colTime", index++));
      }
    };

    return Pair.of(colTimeV, verifier);
  }

  private static Pair<TimeStampMilliVector, ResultVerifier> testTimeStampVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    TimeStampMilliVector colTimeStampV = new TimeStampMilliVector("colTimeStamp", allocator);
    colTimeStampV.allocateNew(5);
    colTimeStampV.set(0, 23423234);
    colTimeStampV.set(1, -234223);
    colTimeStampV.setNull(2);
    colTimeStampV.set(3, 384928359237L);
    colTimeStampV.set(4, 234289342983294234L);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals("1970-01-01 06:30:23.234", output.extractValue("colTimeStamp", index++));
        assertEquals("1969-12-31 23:56:05.777", output.extractValue("colTimeStamp", index++));
        assertNull(output.extractValue("colTimeStamp", index++));
        assertEquals("1982-03-14 04:32:39.237", output.extractValue("colTimeStamp", index++));
        assertEquals("7426303-09-23 10:54:54.234", output.extractValue("colTimeStamp", index++));
      }
    };

    return Pair.of(colTimeStampV, verifier);
  }

  private static Pair<IntervalYearVector, ResultVerifier> testIntervalYearVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    IntervalYearVector colIntervalYearV = new IntervalYearVector("colIntervalYear", allocator);
    colIntervalYearV.allocateNew(5);
    colIntervalYearV.set(0, 2342);
    colIntervalYearV.set(1, -234);
    colIntervalYearV.set(2, 34545);
    colIntervalYearV.set(3, 38);
    colIntervalYearV.setNull(4);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals("+195-02", output.extractValue("colIntervalYear", index++));
        assertEquals("-019-06", output.extractValue("colIntervalYear", index++));
        assertEquals("+2878-09", output.extractValue("colIntervalYear", index++));
        assertEquals("+003-02", output.extractValue("colIntervalYear", index++));
        assertNull(output.extractValue("colIntervalYear", index++));
      }
    };

    return Pair.of(colIntervalYearV, verifier);
  }

  private static Pair<IntervalDayVector, ResultVerifier> testIntervalDayVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    IntervalDayVector colIntervalDayV = new IntervalDayVector("colIntervalDay", allocator);
    colIntervalDayV.allocateNew(5);
    colIntervalDayV.setNull(0);
    colIntervalDayV.set(1, 1, -300, 23423);
    colIntervalDayV.set(2, 1, 23424, 234234);
    colIntervalDayV.set(3, 1, 234, 2323);
    colIntervalDayV.set(4, 1, 987, 343);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertNull(output.extractValue("colIntervalDay", index++));
        assertEquals("-299 23:59:36.577", output.extractValue("colIntervalDay", index++));
        assertEquals("+23424 00:03:54.234", output.extractValue("colIntervalDay", index++));
        assertEquals("+234 00:00:02.323", output.extractValue("colIntervalDay", index++));
        assertEquals("+987 00:00:00.343", output.extractValue("colIntervalDay", index++));
      }
    };

    return Pair.of(colIntervalDayV, verifier);
  }

  private Pair<DecimalVector, ResultVerifier> testDecimalVector() {
    final int scale = 10;
    final String colName = "colDecimal";
    final List<BigDecimal> values =  asList(
      "0.0002503",
      "2524324.034534",
      null,
      "2523423423424234243234.235",
      "2523423423424234243234.0123456789"
    ).stream()
      .map(decimalString -> decimalString == null ? null : new BigDecimal(decimalString).setScale(scale))
      .collect(Collectors.toList());

    DecimalVector valuesVector = new DecimalVector(colName, allocator, 10, scale);
    valuesVector.allocateNew(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        valuesVector.setNull(i);
      } else {
        DecimalUtility.writeBigDecimalToArrowBuf(values.get(i), tempBuf, 0, DecimalVector.TYPE_WIDTH);
        valuesVector.set(i, tempBuf);
      }
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        for (int i = 0; i < values.size(); i++) {
          BigDecimal value = values.get(i);
          if (value == null) {
            assertNull(output.extractValue(colName, i));
          } else {
            if (convertNumbersToStrings) {
              assertEquals(value.toPlainString(), output.extractValue(colName, i));
            } else {
              assertEquals(value.doubleValue(), ((Double)output.extractValue(colName, i)).doubleValue(), 0.000f);
            }
          }
        }
      }
    };

    return Pair.of(valuesVector, verifier);
  }

  private static Pair<VarCharVector, ResultVerifier> testVarCharVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    VarCharVector colVarCharV = new VarCharVector("colVarChar", allocator);
    colVarCharV.allocateNew(500, 5);
    colVarCharV.set(0, "value1".getBytes());
    colVarCharV.set(1,
        "long long long long long long long long long long long long long long long long value".getBytes()
    );
    colVarCharV.set(2, "long long long long value".getBytes());
    colVarCharV.setNull(3);
    colVarCharV.set(4, "l".getBytes());

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        int uIndex = startIndexInJob;
        assertEquals("value1", output.extractValue("colVarChar", index));
        assertNull(output.extractUrl("colVarChar", index++));
        uIndex++;

        assertEquals("long long long long long long ", output.extractValue("colVarChar", index));
        assertEquals(cellUrl(uIndex++, "colVarChar"), output.extractUrl("colVarChar", index++));

        assertEquals("long long long long value", output.extractValue("colVarChar", index));
        assertNull(output.extractUrl("colVarChar", index++));

        assertNull(output.extractValue("colVarChar", index));
        assertNull(output.extractUrl("colVarChar", index++));

        assertEquals("l", output.extractValue("colVarChar", index));
        assertNull(output.extractUrl("colVarChar", index++));
      }
    };

    return Pair.of(colVarCharV, verifier);
  }

  private static Pair<VarBinaryVector, ResultVerifier> testVarBinaryVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    VarBinaryVector colVarBinaryV = new VarBinaryVector("colVarBinary", allocator);
    colVarBinaryV.allocateNew(500, 5);
    colVarBinaryV.set(0, "value1".getBytes());
    colVarBinaryV.set(1,
        "long long long long long long long long long long long long long long long value".getBytes()
    );
    colVarBinaryV.set(2, "long long long long value".getBytes());
    colVarBinaryV.setNull(3);
    colVarBinaryV.set(4, "l".getBytes());

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        int uIndex = startIndexInJob;
        assertEquals("dmFsdWUx", output.extractValue("colVarBinary", index));
        assertNull(output.extractUrl("colVarBinary", index++));
        uIndex++;

        assertEquals("bG9uZyBsb25nIGxvbmcgbG9uZyBsb25nIGxvbmcg", output.extractValue("colVarBinary", index));
        assertEquals(cellUrl(uIndex++, "colVarBinary"), output.extractUrl("colVarBinary", index++));

        assertEquals("bG9uZyBsb25nIGxvbmcgbG9uZyB2YWx1ZQ==", output.extractValue("colVarBinary", index));
        assertNull(output.extractUrl("colVarBinary", index++));
        uIndex++;

        assertNull(output.extractValue("colVarBinary", index));
        assertNull(output.extractUrl("colVarBinary", index++));
        uIndex++;

        assertEquals("bA==", output.extractValue("colVarBinary", index));
        assertNull(output.extractUrl("colVarBinary", index++));
        uIndex++;
      }
    };

    return Pair.of(colVarBinaryV, verifier);
  }

  private static void writeVarChar(VarCharWriter writer, String value) {
    byte[] varCharVal = value.getBytes();
    tempBuf.setBytes(0, varCharVal);
    writer.writeVarChar(0, varCharVal.length, tempBuf);
  }

  private static Pair<NonNullableStructVector, ResultVerifier> testMapVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NonNullableStructVector colStructV = new NonNullableStructVector("colMap", allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null),null);

    ComplexWriterImpl structWriter = new ComplexWriterImpl("colMap", colStructV);

    // colMap contains the following records:
    // { bigint: 23, nVarCharCol: 'value', nListCol: [1970-01-01, 1970-01-03, 1969-12-31], nUnionCol: 2 }
    // { bigint: 223, nVarCharCol: 'long long value', nListCol: [1969-12-29, 1969-12-29], nUnionCol: 'long long value' }
    // { bigint: 54645, nMap: { a: 1 } }
    // { }
    // { bigint: 234543 }

    StructWriter structWr = structWriter.rootAsStruct();

    structWr.setPosition(0);
    structWr.start();
    structWr.bigInt(BIGINT_COL).writeBigInt(23);
    writeVarChar(structWr.varChar(VARCHAR_COL), "value");
    structWr.list(LIST_COL).startList();
    structWr.list(LIST_COL).dateMilli().writeDateMilli(2312L);
    structWr.list(LIST_COL).dateMilli().writeDateMilli(234823492L);
    structWr.list(LIST_COL).dateMilli().writeDateMilli(-2382437L);
    structWr.list(LIST_COL).endList();
    structWr.integer(UNION_COL).writeInt(2);
    structWr.end();

    structWr.setPosition(1);
    structWr.start();
    structWr.bigInt(BIGINT_COL).writeBigInt(223);
    writeVarChar(structWr.varChar(VARCHAR_COL), "long long value");
    structWr.list(LIST_COL).startList();
    structWr.list(LIST_COL).dateMilli().writeDateMilli(-234238942L);
    structWr.list(LIST_COL).dateMilli().writeDateMilli(-234238942L);
    structWr.list(LIST_COL).endList();
    writeVarChar(structWr.varChar(UNION_COL), "long long value");
    structWr.end();

    structWr.setPosition(2);
    structWr.start();
    structWr.bigInt(BIGINT_COL).writeBigInt(54645L);
    structWr.struct(MAP_COL).start();
    structWr.struct(MAP_COL).integer("a").writeInt(1);
    structWr.struct(MAP_COL).end();
    structWr.end();

    structWr.setPosition(4);
    structWr.start();
    structWr.bigInt(BIGINT_COL).writeBigInt(234543L);
    structWr.end();

    structWriter.setValueCount(5);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        int uIndex = startIndexInJob;
        assertEquals("{colMap={nBigIntCol=23, nVarCharCol=value, nListCol=[1970-01-01, 1970-01-03, 1969-12-31]}}", output.extractValue("colMap", index).toString());
        assertEquals(cellUrl(uIndex++, "colMap"), output.extractUrl("colMap", index++));

        assertEquals("{colMap={nBigIntCol=223, nVarCharCol=long long value, nListCol=[1969-12-29]}}", output.extractValue("colMap", index).toString());
        assertEquals(cellUrl(uIndex++, "colMap"), output.extractUrl("colMap", index++));

        assertEquals("{colMap={nBigIntCol=54645, nUnionCol=null, nMapCol={a=1}}}", output.extractValue("colMap", index).toString());
        assertNull(output.extractUrl("colMap", index++));

        assertEquals("{}", output.extractValue("colMap", index).toString());
        assertNull(output.extractUrl("colMap", index++));

        assertEquals("{colMap={nBigIntCol=234543, nUnionCol=null}}", output.extractValue("colMap", index).toString());
        assertNull(output.extractUrl("colMap", index++));
      }
    };

    return Pair.of(colStructV, verifier);
  }

  private static Pair<ListVector, ResultVerifier> testListVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    ListVector colListV = new ListVector("colList", allocator, null);

    colListV.allocateNew();
    UnionListWriter listWriter = new UnionListWriter(colListV);
    for(int i=0; i<5; i++) {
      listWriter.setPosition(i);
      listWriter.startList();
      for(int j=0; j<5; j++) {
        byte[] varCharVal = String.format("item %d-%d", i, j).getBytes();
        tempBuf.setBytes(0, varCharVal);
        listWriter.writeVarChar(0, varCharVal.length, tempBuf);
      }
      listWriter.endList();
    }

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        int uIndex = startIndexInJob;
        assertEquals("[item 0-0, item 0-1, item 0-2, item 0]", output.extractValue("colList", index).toString());
        assertEquals(cellUrl(uIndex++, "colList"), output.extractUrl("colList", index++));

        assertEquals("[item 1-0, item 1-1, item 1-2, item 1]", output.extractValue("colList", index).toString());
        assertEquals(cellUrl(uIndex++, "colList"), output.extractUrl("colList", index++));

        assertEquals("[item 2-0, item 2-1, item 2-2, item 2]", output.extractValue("colList", index).toString());
        assertEquals(cellUrl(uIndex++, "colList"), output.extractUrl("colList", index++));

        assertEquals("[item 3-0, item 3-1, item 3-2, item 3]", output.extractValue("colList", index).toString());
        assertEquals(cellUrl(uIndex++, "colList"), output.extractUrl("colList", index++));

        assertEquals("[item 4-0, item 4-1, item 4-2, item 4]", output.extractValue("colList", index).toString());
        assertEquals(cellUrl(uIndex++, "colList"), output.extractUrl("colList", index));
      }
    };

    return Pair.of(colListV, verifier);
  }

  private Pair<UnionVector, ResultVerifier> testUnionVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    UnionVector colUnionV = new UnionVector("colUnion", allocator, null);

    UnionWriter unionWriter = new UnionWriter(colUnionV);
    unionWriter.allocate();
    for (int i = 0; i < 4; i++) {
      unionWriter.setPosition(i);
      if (i % 2 == 0) {
        unionWriter.writeInt(i);
      } else {
        unionWriter.writeFloat4(i);
      }
    }
    unionWriter.setPosition(4);
    byte[] varCharVal = "union varchar union varchar union varchar".getBytes();
    tempBuf.setBytes(0, varCharVal);
    unionWriter.writeVarChar(0, varCharVal.length, tempBuf);

    colUnionV.setValueCount(5);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        int uIndex = startIndexInJob;
        assertEquals(DataType.INTEGER, output.extractType("colUnion", index));
        assertEquals(getExpectedNumber(0), output.extractValue("colUnion", index));
        assertNull(output.extractUrl("colUnion", index++));
        uIndex++;

        assertEquals(DataType.FLOAT, output.extractType("colUnion", index));
        verifyDoubleValue(1d, output, "colUnion", index, 0.01f);
        assertNull(output.extractUrl("colUnion", index++));
        uIndex++;

        assertEquals(DataType.INTEGER, output.extractType("colUnion", index));
        assertEquals(getExpectedNumber(index), output.extractValue("colUnion", index));
        assertNull(output.extractUrl("colUnion", index++));
        uIndex++;

        assertEquals(DataType.FLOAT, output.extractType("colUnion", index));
        verifyDoubleValue(3d, output, "colUnion", index, 0.01f);
        assertNull(output.extractUrl("colUnion", index++));
        uIndex++;

        assertEquals(DataType.TEXT, output.extractType("colUnion", index));
        assertEquals("union varchar union varchar un", output.extractValue("colUnion", index));
        assertEquals(cellUrl(uIndex++, "colUnion"), output.extractUrl("colUnion", index++));
      }
    };

    return Pair.of(colUnionV, verifier);
  }

  private static String cellUrl(int r, String col) {
    return String.format("/job/%s/r/%d/c/%s", TEST_JOB_ID.getId(), r, col);
  }

  @Test
  public void testDataSerialization() throws Exception {
    Pair<? extends ValueVector, ResultVerifier> varChar1 = testVarCharVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> varChar2 = testVarCharVector(5, 5);
    Pair<? extends ValueVector, ResultVerifier> varChar3 = testVarCharVector(10, 10);
    Pair<? extends ValueVector, ResultVerifier> date1 = testDateMilliVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> date2 = testDateMilliVector(5, 5);
    Pair<? extends ValueVector, ResultVerifier> date3 = testDateMilliVector(10, 10);

    RecordBatchData batch1 = createRecordBatch(varChar1.getKey(), date1.getKey());
    RecordBatchData batch2 = createRecordBatch(varChar2.getKey(), date2.getKey());
    RecordBatchData batch3 = createRecordBatch(varChar3.getKey(), date3.getKey());

    try (JobDataFragment dataInput = new JobDataFragmentWrapper(0, new JobDataFragmentImpl(
      new RecordBatches(asList(
        newRecordBatchHolder(batch1, 0, 5),
        newRecordBatchHolder(batch2, 0, 5),
        newRecordBatchHolder(batch3, 0, 5)
      )), 0, TEST_JOB_ID))) {
      DataPOJO dataOutput = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(dataInput), DataPOJO.class);
      assertEquals(dataInput.getColumns().toString(), dataOutput.getColumns().toString());
      assertEquals(dataInput.getReturnedRowCount(), dataOutput.getReturnedRowCount());

      varChar1.getValue().verify(dataOutput);
      varChar2.getValue().verify(dataOutput);
      date1.getValue().verify(dataOutput);
      date2.getValue().verify(dataOutput);
    }
  }

  @Test
  public void testDataWithOffsetSerialization() throws Exception {
    Pair<? extends ValueVector, ResultVerifier> varChar1 = testVarCharVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> varChar2 = testVarCharVector(0, 5);
    Pair<? extends ValueVector, ResultVerifier> varChar3 = testVarCharVector(5, 10);
    Pair<? extends ValueVector, ResultVerifier> date1 = testDateMilliVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> date2 = testDateMilliVector(0, 5);
    Pair<? extends ValueVector, ResultVerifier> date3 = testDateMilliVector(5, 10);

    RecordBatchData batch1 = createRecordBatch(varChar1.getKey(), date1.getKey());
    RecordBatchData batch2 = createRecordBatch(varChar2.getKey(), date2.getKey());
    RecordBatchData batch3 = createRecordBatch(varChar3.getKey(), date3.getKey());

    try (JobDataFragment dataInput = new JobDataFragmentWrapper(5, new JobDataFragmentImpl(
      new RecordBatches(asList(
        newRecordBatchHolder(batch1, 0, 5),
        newRecordBatchHolder(batch2, 0, 5),
        newRecordBatchHolder(batch3, 0, 5)
      )), 0, TEST_JOB_ID))) {
      DataPOJO dataOutput = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(dataInput), DataPOJO.class);
      assertEquals(dataInput.getColumns().toString(), dataOutput.getColumns().toString());
      assertEquals(dataInput.getReturnedRowCount(), dataOutput.getReturnedRowCount());

      varChar2.getValue().verify(dataOutput);
      varChar3.getValue().verify(dataOutput);
      date2.getValue().verify(dataOutput);
      date3.getValue().verify(dataOutput);
    }
  }

  @Test
  public void testExtractValueBatchesBoundary() throws Exception {
    Pair<? extends ValueVector, ResultVerifier> varChar1 = testVarCharVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> varChar2 = testVarCharVector(0, 5);
    Pair<? extends ValueVector, ResultVerifier> varChar3 = testVarCharVector(5, 10);
    Pair<? extends ValueVector, ResultVerifier> date1 = testDateMilliVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> date2 = testDateMilliVector(0, 5);
    Pair<? extends ValueVector, ResultVerifier> date3 = testDateMilliVector(5, 10);

    List<RecordBatchHolder> recordBatches = new ArrayList<>();
    RecordBatchData data1 = createRecordBatch(varChar1.getKey(), date1.getKey());
    RecordBatchData data2 = createRecordBatch(varChar2.getKey(), date2.getKey());
    RecordBatchData data3 = createRecordBatch(varChar3.getKey(), date3.getKey());
    recordBatches.add(newRecordBatchHolder(data1, 0, data1.getRecordCount()));
    recordBatches.add(newRecordBatchHolder(data2, 0, data2.getRecordCount()));
    recordBatches.add(newRecordBatchHolder(data3, 0, data3.getRecordCount()));

    try (JobDataFragment jdf = new JobDataFragmentWrapper(0, new JobDataFragmentImpl(
      new RecordBatches(recordBatches), 0, TEST_JOB_ID))) {

      String value = jdf.extractString("colVarChar", 3);
      assertEquals(null, value);
      value = jdf.extractString("colVarChar", 8);
      assertEquals(null, value);
      value = jdf.extractString("colVarChar", 10);
      assertEquals("value1", value);
      value = jdf.extractString("colVarChar", 12);
      assertEquals("long long long long value", value);
      value = jdf.extractString("colVarChar", 14);
      assertEquals("l", value);
      try {
        value = jdf.extractString("colVarChar", 15);
        fail("Index out of bounds exception");
      } catch (IllegalArgumentException e) {
        // noop
      }

      try (JobDataFragment jdf2 = new JobDataFragmentWrapper(0 , new JobDataFragmentImpl(
        new RecordBatches(asList(
          newRecordBatchHolder(data1, 2, 5),
          newRecordBatchHolder(data2, 1, 3),
          newRecordBatchHolder(data3, 0, 4)
        )), 0, TEST_JOB_ID))) {
        value = jdf2.extractString("colVarChar", 0); // should be element #2 from first batch
        assertEquals("long long long long value", value);
        value = jdf2.extractString("colVarChar", 3); // will not fit first batch - will be 1st element of the 2nd batch which element 1
        assertEquals("long long long long long long long long long long long long long long long long value", value);
        value = jdf2.extractString("colVarChar", 8); // last in 3rd batch (element #3)
        assertEquals(null, value);
        try {
          value = jdf2.extractString("colVarChar", 9);
          fail("Index out of bounds exception");
        } catch (IllegalArgumentException e) {
          // noop
        }
      }
    }
  }
  @AfterClass
  public static void shutdown() throws Exception {
    AutoCloseables.close(tempBuf, allocator);
  }
}
