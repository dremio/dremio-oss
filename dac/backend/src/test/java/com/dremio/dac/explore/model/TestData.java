/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.service.jobs.RecordBatchHolder.newRecordBatchHolder;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableDateMilliVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableIntervalDayVector;
import org.apache.arrow.vector.NullableIntervalYearVector;
import org.apache.arrow.vector.NullableSmallIntVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableTimeStampMilliVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataFragmentWrapper;
import com.dremio.dac.model.job.JobDataFragmentWrapper.JobDataFragmentSerializer;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobDataFragmentImpl;
import com.dremio.service.jobs.JobDataImpl;
import com.dremio.service.jobs.JobLoader;
import com.dremio.service.jobs.RecordBatchHolder;
import com.dremio.service.jobs.RecordBatches;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

/**
 * Unittests for {@link com.dremio.dac.model.job.JobDataFragmentWrapper}. Currently it only tests the serialization aspects such as
 * <ul>
 *   <li>able to serialize all types of ValueVectors</li>
 *   <li>truncate large cell values</li>
 *   <li>providing URL to fetch the complete cell value for truncated cell values in results</li>
 * </ul>
 */
public class TestData {
  private static final JobId TEST_JOB_ID = new JobId("testJobId");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    SimpleModule module = new SimpleModule();
    module.addSerializer(JobDataFragmentWrapper.class, new JobDataFragmentSerializer());
    OBJECT_MAPPER.registerModule(module);
  }

  private static BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private static ArrowBuf tempBuf = allocator.buffer(1024);

  private static String defaultMaxCellLength;

  private static final String BIGINT_COL = "nBigIntCol";
  private static final String VARCHAR_COL = "nVarCharCol";
  private static final String LIST_COL = "nListCol";
  private static final String MAP_COL = "nMapCol";
  private static final String UNION_COL = "nUnionCol";

  @BeforeClass
  public static void setMaxCellLength() {
    // For testing purposes set the value to 30
    defaultMaxCellLength = System.getProperty(JobData.MAX_CELL_SIZE_KEY);
    System.setProperty(JobData.MAX_CELL_SIZE_KEY, "30");
  }

  @AfterClass
  public static void resetMaxCellLength() {
    if (defaultMaxCellLength != null) {
      System.setProperty(JobData.MAX_CELL_SIZE_KEY, defaultMaxCellLength);
    }
  }

  @Test
  public void serializeBoolean() throws Exception {
    helper(testBitVector(0, 0), "colBit", false, false);
  }

  @Test
  public void serializeTinyInt() throws Exception {
    helper(testTinyIntVector(0, 0), "colTinyInt", false, false);
  }

  @Test
  public void serializeSmallInt() throws Exception {
    helper(testSmallIntVector(0, 0), "colSmallInt", false, false);
  }

  @Test
  public void serializeInt() throws Exception {
    helper(testIntVector(0, 0), "colInt", false, false);
  }

  @Test
  public void serializeBigInt() throws Exception {
    helper(testBigIntVector(0, 0), "colBigInt", false, false);
  }

  @Test
  public void serializeFloat4() throws Exception {
    helper(testFloat4Vector(0, 0), "colFloat4", false, false);
  }

  @Test
  public void serializeFloat8() throws Exception {
    helper(testFloat8Vector(0, 0), "colFloat8", false, false);
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
    helper(testDecimalVector(0, 0), "colDecimal", false, false);
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
    pairs.add(testTinyIntVector(0, 0));
    pairs.add(testSmallIntVector(0, 0));
    pairs.add(testIntVector(0, 0));
    pairs.add(testBigIntVector(0, 0));
    pairs.add(testFloat4Vector(0, 0));
    pairs.add(testFloat8Vector(0, 0));
    pairs.add(testDateMilliVector(0, 0));
    pairs.add(testTimeVector(0, 0));
    pairs.add(testTimeStampVector(0, 0));
    pairs.add(testIntervalYearVector(0, 0));
    pairs.add(testIntervalDayVector(0, 0));
    pairs.add(testDecimalVector(0, 0));
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

  private static Pair<NullableBitVector, ResultVerifier> testBitVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableBitVector colBitV = new NullableBitVector("colBit", allocator);
    colBitV.allocateNew(5);
    colBitV.getMutator().set(0, 1);
    colBitV.getMutator().set(1, 0);
    colBitV.getMutator().setNull(2);
    colBitV.getMutator().set(3, 1);
    colBitV.getMutator().set(4, 1);

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

  private static Pair<NullableTinyIntVector, ResultVerifier> testTinyIntVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableTinyIntVector colTinyIntV = new NullableTinyIntVector("colTinyInt", allocator);
    colTinyIntV.allocateNew(5);
    colTinyIntV.getMutator().set(0, 0);
    colTinyIntV.getMutator().set(1, -1);
    colTinyIntV.getMutator().set(2, 1);
    colTinyIntV.getMutator().setNull(3);
    colTinyIntV.getMutator().set(4, -54);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals(0, ((Integer)output.extractValue("colTinyInt", index++)).intValue());
        assertEquals(-1, ((Integer)output.extractValue("colTinyInt", index++)).intValue());
        assertEquals(1, ((Integer)output.extractValue("colTinyInt", index++)).intValue());
        assertNull(output.extractValue("colTinyInt", index++));
        assertEquals(-54, ((Integer)output.extractValue("colTinyInt", index++)).intValue());
      }
    };

    return Pair.of(colTinyIntV, verifier);
  }

  private static Pair<NullableSmallIntVector, ResultVerifier> testSmallIntVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableSmallIntVector colSmallIntV = new NullableSmallIntVector("colSmallInt", allocator);
    colSmallIntV.allocateNew(5);
    colSmallIntV.getMutator().set(0, 20);
    colSmallIntV.getMutator().setNull(1);
    colSmallIntV.getMutator().set(2, -2000);
    colSmallIntV.getMutator().set(3, 32700);
    colSmallIntV.getMutator().set(4, 0);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals(20, ((Integer)output.extractValue("colSmallInt", 0)).intValue());
        assertNull(output.extractValue("colSmallInt", 1));
        assertEquals(-2000, ((Integer)output.extractValue("colSmallInt", 2)).intValue());
        assertEquals(32700, ((Integer)output.extractValue("colSmallInt", 3)).intValue());
        assertEquals(0, ((Integer)output.extractValue("colSmallInt", 4)).intValue());
      }
    };

    return Pair.of(colSmallIntV, verifier);
  }

  private static Pair<NullableIntVector, ResultVerifier> testIntVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableIntVector colIntV = new NullableIntVector("colInt", allocator);
    colIntV.allocateNew(5);
    colIntV.getMutator().set(0, 20);
    colIntV.getMutator().set(1, 50);
    colIntV.getMutator().set(2, -2000);
    colIntV.getMutator().set(3, 327345);
    colIntV.getMutator().setNull(4);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals(20, ((Integer)output.extractValue("colInt", 0)).intValue());
        assertEquals(50, ((Integer)output.extractValue("colInt", 1)).intValue());
        assertEquals(-2000, ((Integer)output.extractValue("colInt", 2)).intValue());
        assertEquals(327345, ((Integer)output.extractValue("colInt", 3)).intValue());
        assertNull(output.extractValue("colInt", 4));
      }
    };

    return Pair.of(colIntV, verifier);
  }

  private static Pair<NullableBigIntVector, ResultVerifier> testBigIntVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableBigIntVector colBigIntV = new NullableBigIntVector("colBigInt", allocator);
    colBigIntV.allocateNew(5);
    colBigIntV.getMutator().setNull(0);
    colBigIntV.getMutator().set(1, 50);
    colBigIntV.getMutator().set(2, -2000);
    colBigIntV.getMutator().set(3, 327345234234L);
    colBigIntV.getMutator().set(4, 0);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertNull(output.extractValue("colBigInt", index++));
        assertEquals(50, ((Integer)output.extractValue("colBigInt", index++)).intValue());
        assertEquals(-2000, ((Integer)output.extractValue("colBigInt", index++)).intValue());
        assertEquals(327345234234L, ((Long)output.extractValue("colBigInt", index++)).longValue());
        assertEquals(0, ((Integer)output.extractValue("colBigInt", index++)).intValue());
      }
    };

    return Pair.of(colBigIntV, verifier);
  }

  private static Pair<NullableFloat4Vector, ResultVerifier> testFloat4Vector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableFloat4Vector colFloat4V = new NullableFloat4Vector("colFloat4", allocator);
    colFloat4V.allocateNew(5);
    colFloat4V.getMutator().set(0, 20.0f);
    colFloat4V.getMutator().set(1, 50.023f);
    colFloat4V.getMutator().set(2, -238423f);
    colFloat4V.getMutator().setNull(3);
    colFloat4V.getMutator().set(4, 0f);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals(20.0d, ((Double)output.extractValue("colFloat4", index++)).doubleValue(), 0.01f);
        assertEquals(50.023d, ((Double)output.extractValue("colFloat4", index++)).doubleValue(), 0.01f);
        assertEquals(-238423d, ((Double)output.extractValue("colFloat4", index++)).doubleValue(), 0.01f);
        assertNull(output.extractValue("colFloat4", index++));
        assertEquals(0d, ((Double)output.extractValue("colFloat4", index++)).doubleValue(), 0.01f);
      }
    };

    return Pair.of(colFloat4V, verifier);
  }

  private static Pair<NullableFloat8Vector, ResultVerifier> testFloat8Vector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableFloat8Vector colFloat8V = new NullableFloat8Vector("colFloat8", allocator);
    colFloat8V.allocateNew(5);
    colFloat8V.getMutator().set(0, 20.2345234d);
    colFloat8V.getMutator().set(1, 503453.023d);
    colFloat8V.getMutator().set(2, -238423.3453453d);
    colFloat8V.getMutator().set(3, 3273452.345324563245d);
    colFloat8V.getMutator().setNull(4);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals(20.2345234d, ((Double)output.extractValue("colFloat8", index++)).doubleValue(), 0.01f);
        assertEquals(503453.023d, ((Double)output.extractValue("colFloat8", index++)).doubleValue(), 0.01f);
        assertEquals(-238423.3453453d, ((Double)output.extractValue("colFloat8", index++)).doubleValue(), 0.01f);
        assertEquals(3273452.345324563245d, ((Double)output.extractValue("colFloat8", index++)).doubleValue(), 0.01f);
        assertNull(output.extractValue("colFloat8", 4));
      }
    };

    return Pair.of(colFloat8V, verifier);
  }

  private static Pair<NullableDateMilliVector, ResultVerifier> testDateMilliVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableDateMilliVector colDateV = new NullableDateMilliVector("colDate", allocator);
    colDateV.allocateNew(5);
    colDateV.getMutator().set(0, 234);
    colDateV.getMutator().set(1, -2342);
    colDateV.getMutator().setNull(2);
    colDateV.getMutator().set(3, 384928359245L);
    colDateV.getMutator().set(4, 2342893433L);

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

  private static Pair<NullableTimeMilliVector, ResultVerifier> testTimeVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableTimeMilliVector colTimeV = new NullableTimeMilliVector("colTime", allocator);
    colTimeV.allocateNew(5);
    colTimeV.getMutator().set(0, 23423234);
    colTimeV.getMutator().set(1, -234223);
    colTimeV.getMutator().set(2, 34534345);
    colTimeV.getMutator().setNull(3);
    colTimeV.getMutator().set(4, 23434);

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

  private static Pair<NullableTimeStampMilliVector, ResultVerifier> testTimeStampVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableTimeStampMilliVector colTimeStampV = new NullableTimeStampMilliVector("colTimeStamp", allocator);
    colTimeStampV.allocateNew(5);
    colTimeStampV.getMutator().set(0, 23423234);
    colTimeStampV.getMutator().set(1, -234223);
    colTimeStampV.getMutator().setNull(2);
    colTimeStampV.getMutator().set(3, 384928359237L);
    colTimeStampV.getMutator().set(4, 234289342983294234L);

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

  private static Pair<NullableIntervalYearVector, ResultVerifier> testIntervalYearVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableIntervalYearVector colIntervalYearV = new NullableIntervalYearVector("colIntervalYear", allocator);
    colIntervalYearV.allocateNew(5);
    colIntervalYearV.getMutator().set(0, 2342);
    colIntervalYearV.getMutator().set(1, -234);
    colIntervalYearV.getMutator().set(2, 34545);
    colIntervalYearV.getMutator().set(3, 38);
    colIntervalYearV.getMutator().setNull(4);

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

  private static Pair<NullableIntervalDayVector, ResultVerifier> testIntervalDayVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableIntervalDayVector colIntervalDayV = new NullableIntervalDayVector("colIntervalDay", allocator);
    colIntervalDayV.allocateNew(5);
    colIntervalDayV.getMutator().setNull(0);
    colIntervalDayV.getMutator().set(1, 1, -300, 23423);
    colIntervalDayV.getMutator().set(2, 1, 23424, 234234);
    colIntervalDayV.getMutator().set(3, 1, 234, 2323);
    colIntervalDayV.getMutator().set(4, 1, 987, 343);

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

  private static Pair<NullableDecimalVector, ResultVerifier> testDecimalVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableDecimalVector colDecimalV = new NullableDecimalVector("colDecimal", allocator, 10, 10);
    colDecimalV.allocateNew(5);
    DecimalUtility.writeBigDecimalToArrowBuf(new BigDecimal(25.03).setScale(5, RoundingMode.HALF_UP), tempBuf, 0);
    colDecimalV.getMutator().set(0, tempBuf);
    DecimalUtility.writeBigDecimalToArrowBuf(new BigDecimal(2524324.034534), tempBuf, 0);
    colDecimalV.getMutator().set(1, tempBuf);
    colDecimalV.getMutator().setNull(2);
    DecimalUtility.writeBigDecimalToArrowBuf(new BigDecimal(2523423423424234243234.235), tempBuf, 0);
    colDecimalV.getMutator().set(3, tempBuf);
    DecimalUtility.writeBigDecimalToArrowBuf(new BigDecimal(2523423423424234243234.23524324234234), tempBuf, 0);
    colDecimalV.getMutator().set(4, tempBuf);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        assertEquals(2.503E-4, ((Double)output.extractValue("colDecimal", index++)).doubleValue(), 0.001f);
        assertEquals(2.524324034534E23, ((Double)output.extractValue("colDecimal", index++)).doubleValue(), 0.001f);
        assertNull(output.extractValue("colDecimal", index++));
        assertEquals(2.523423423424234E11, ((Double)output.extractValue("colDecimal", index++)).doubleValue(), 0.001f);
        assertEquals(2.523423423424234E11, ((Double)output.extractValue("colDecimal", index++)).doubleValue(), 0.001f);
      }
    };

    return Pair.of(colDecimalV, verifier);
  }

  private static Pair<NullableVarCharVector, ResultVerifier> testVarCharVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableVarCharVector colVarCharV = new NullableVarCharVector("colVarChar", allocator);
    colVarCharV.allocateNew(500, 5);
    colVarCharV.getMutator().set(0, "value1".getBytes());
    colVarCharV.getMutator().set(1,
        "long long long long long long long long long long long long long long long long value".getBytes()
    );
    colVarCharV.getMutator().set(2, "long long long long value".getBytes());
    colVarCharV.getMutator().setNull(3);
    colVarCharV.getMutator().set(4, "l".getBytes());

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

  private static Pair<NullableVarBinaryVector, ResultVerifier> testVarBinaryVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    NullableVarBinaryVector colVarBinaryV = new NullableVarBinaryVector("colVarBinary", allocator);
    colVarBinaryV.allocateNew(500, 5);
    colVarBinaryV.getMutator().set(0, "value1".getBytes());
    colVarBinaryV.getMutator().set(1,
        "long long long long long long long long long long long long long long long value".getBytes()
    );
    colVarBinaryV.getMutator().set(2, "long long long long value".getBytes());
    colVarBinaryV.getMutator().setNull(3);
    colVarBinaryV.getMutator().set(4, "l".getBytes());

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

  private static Pair<MapVector, ResultVerifier> testMapVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
    MapVector colMapV = new MapVector("colMap", allocator, null);

    ComplexWriterImpl mapWriter = new ComplexWriterImpl("colMap", colMapV);

    // colMap contains the following records:
    // { bigint: 23, nVarCharCol: 'value', nListCol: [1970-01-01, 1970-01-03, 1969-12-31], nUnionCol: 2 }
    // { bigint: 223, nVarCharCol: 'long long value', nListCol: [1969-12-29, 1969-12-29], nUnionCol: 'long long value' }
    // { bigint: 54645, nMap: { a: 1 } }
    // { }
    // { bigint: 234543 }

    MapWriter mapWr = mapWriter.rootAsMap();

    mapWr.setPosition(0);
    mapWr.start();
    mapWr.bigInt(BIGINT_COL).writeBigInt(23);
    writeVarChar(mapWr.varChar(VARCHAR_COL), "value");
    mapWr.list(LIST_COL).startList();
    mapWr.list(LIST_COL).dateMilli().writeDateMilli(2312L);
    mapWr.list(LIST_COL).dateMilli().writeDateMilli(234823492L);
    mapWr.list(LIST_COL).dateMilli().writeDateMilli(-2382437L);
    mapWr.list(LIST_COL).endList();
    mapWr.integer(UNION_COL).writeInt(2);
    mapWr.end();

    mapWr.setPosition(1);
    mapWr.start();
    mapWr.bigInt(BIGINT_COL).writeBigInt(223);
    writeVarChar(mapWr.varChar(VARCHAR_COL), "long long value");
    mapWr.list(LIST_COL).startList();
    mapWr.list(LIST_COL).dateMilli().writeDateMilli(-234238942L);
    mapWr.list(LIST_COL).dateMilli().writeDateMilli(-234238942L);
    mapWr.list(LIST_COL).endList();
    writeVarChar(mapWr.varChar(UNION_COL), "long long value");
    mapWr.end();

    mapWr.setPosition(2);
    mapWr.start();
    mapWr.bigInt(BIGINT_COL).writeBigInt(54645L);
    mapWr.map(MAP_COL).start();
    mapWr.map(MAP_COL).integer("a").writeInt(1);
    mapWr.map(MAP_COL).end();
    mapWr.end();

    mapWr.setPosition(4);
    mapWr.start();
    mapWr.bigInt(BIGINT_COL).writeBigInt(234543L);
    mapWr.end();

    mapWriter.setValueCount(5);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        int uIndex = startIndexInJob;
        assertEquals("{colMap={nBigIntCol=23, nVarCharCol=value, nListCol=[1970-01-01, 1970-01-03, 1969-12-31]}}", output.extractValue("colMap", index).toString());
        assertEquals(cellUrl(uIndex++, "colMap"), output.extractUrl("colMap", index++));

        assertEquals("{colMap={nBigIntCol=223, nVarCharCol=long long value, nListCol=[1969-12-29]}}", output.extractValue("colMap", index).toString());
        assertEquals(cellUrl(uIndex++, "colMap"), output.extractUrl("colMap", index++));

        assertEquals("{colMap={nBigIntCol=54645, nMapCol={a=1}}}", output.extractValue("colMap", index).toString());
        assertNull(output.extractUrl("colMap", index++));

        assertEquals("{}", output.extractValue("colMap", index).toString());
        assertNull(output.extractUrl("colMap", index++));

        assertEquals("{colMap={nBigIntCol=234543}}", output.extractValue("colMap", index).toString());
        assertNull(output.extractUrl("colMap", index++));
      }
    };

    return Pair.of(colMapV, verifier);
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

  private static Pair<UnionVector, ResultVerifier> testUnionVector(final int startIndexInCurrentOutput, final int startIndexInJob) {
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

    colUnionV.getMutator().setValueCount(5);

    ResultVerifier verifier = new ResultVerifier() {
      @Override
      public void verify(DataPOJO output) {
        int index = startIndexInCurrentOutput;
        int uIndex = startIndexInJob;
        assertEquals(DataType.INTEGER, output.extractType("colUnion", index));
        assertEquals(0, ((Integer)output.extractValue("colUnion", index)).intValue());
        assertNull(output.extractUrl("colUnion", index++));
        uIndex++;

        assertEquals(DataType.FLOAT, output.extractType("colUnion", index));
        assertEquals(1.0d, ((Double)output.extractValue("colUnion", index)).doubleValue(), 0.01f);
        assertNull(output.extractUrl("colUnion", index++));
        uIndex++;

        assertEquals(DataType.INTEGER, output.extractType("colUnion", index));
        assertEquals(index, ((Integer)output.extractValue("colUnion", index)).intValue());
        assertNull(output.extractUrl("colUnion", index++));
        uIndex++;

        assertEquals(DataType.FLOAT, output.extractType("colUnion", index));
        assertEquals(3.0d, ((Double)output.extractValue("colUnion", index)).doubleValue(), 0.01f);
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
  public void testDataTrunc() throws Exception {
    Pair<? extends ValueVector, ResultVerifier> varChar1 = testVarCharVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> varChar2 = testVarCharVector(5, 5);
    Pair<? extends ValueVector, ResultVerifier> varChar3 = testVarCharVector(10, 10);
    Pair<? extends ValueVector, ResultVerifier> date1 = testDateMilliVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> date2 = testDateMilliVector(5, 5);
    Pair<? extends ValueVector, ResultVerifier> date3 = testDateMilliVector(10, 10);

    RecordBatchData batch1 = createRecordBatch(varChar1.getKey(), date1.getKey());
    RecordBatchData batch2 = createRecordBatch(varChar2.getKey(), date2.getKey());
    RecordBatchData batch3 = createRecordBatch(varChar3.getKey(), date3.getKey());

    JobLoader jobLoader = mock(JobLoader.class);
    when(jobLoader.load(anyInt(), anyInt())).thenReturn(
        new RecordBatches(asList(
            newRecordBatchHolder(batch1, 0, 5),
            newRecordBatchHolder(batch2, 0, 5),
            newRecordBatchHolder(batch3, 0, 5)
        ))
    );

    try (JobData dataInput = new JobDataWrapper(new JobDataImpl(jobLoader, TEST_JOB_ID))) {
      JobDataFragment truncDataInput = dataInput.truncate(10);
      DataPOJO dataOutput = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(truncDataInput), DataPOJO.class);
      assertEquals(truncDataInput.getColumns().toString(), dataOutput.getColumns().toString());
      assertEquals(truncDataInput.getReturnedRowCount(), dataOutput.getReturnedRowCount());

      varChar1.getValue().verify(dataOutput);
      varChar2.getValue().verify(dataOutput);
      date1.getValue().verify(dataOutput);
      date2.getValue().verify(dataOutput);
    }
  }

  @Test
  public void testDataRange() throws Exception {
    Pair<? extends ValueVector, ResultVerifier> varChar1 = testVarCharVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> varChar2 = testVarCharVector(0, 5);
    Pair<? extends ValueVector, ResultVerifier> varChar3 = testVarCharVector(5, 10);
    Pair<? extends ValueVector, ResultVerifier> date1 = testDateMilliVector(0, 0);
    Pair<? extends ValueVector, ResultVerifier> date2 = testDateMilliVector(0, 5);
    Pair<? extends ValueVector, ResultVerifier> date3 = testDateMilliVector(5, 10);

    RecordBatchData batch1 = createRecordBatch(varChar1.getKey(), date1.getKey());
    RecordBatchData batch2 = createRecordBatch(varChar2.getKey(), date2.getKey());
    RecordBatchData batch3 = createRecordBatch(varChar3.getKey(), date3.getKey());

    JobLoader jobLoader = mock(JobLoader.class);
    when(jobLoader.load(anyInt(), anyInt())).thenReturn(
        new RecordBatches(asList(
            newRecordBatchHolder(batch1, 0, 5),
            newRecordBatchHolder(batch2, 0, 5),
            newRecordBatchHolder(batch3, 0, 5)
        ))
    );

    try (JobData dataInput = new JobDataWrapper(new JobDataImpl(jobLoader, TEST_JOB_ID))) {
      JobDataFragment rangeDataInput = dataInput.range(5, 10);
      DataPOJO dataOutput = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rangeDataInput), DataPOJO.class);
      assertEquals(rangeDataInput.getColumns().toString(), dataOutput.getColumns().toString());
      assertEquals(rangeDataInput.getReturnedRowCount(), dataOutput.getReturnedRowCount());

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

    try (JobDataFragment jdf = new JobDataFragmentWrapper(0, new JobDataFragmentImpl(new RecordBatches(recordBatches), 0, TEST_JOB_ID))) {

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

      JobLoader jobLoader = mock(JobLoader.class);
      when(jobLoader.load(anyInt(), anyInt())).thenReturn(
        new RecordBatches(asList(
          newRecordBatchHolder(data1, 2, 5),
          newRecordBatchHolder(data2, 1, 3),
          newRecordBatchHolder(data3, 0, 4)
        ))
      );
      try (JobData dataInput = new JobDataWrapper(new JobDataImpl(jobLoader, TEST_JOB_ID))) {
        JobDataFragment rangeDataInput = dataInput.range(5, 10); // those numbers do not matter, mock holds all the truth
        value = rangeDataInput.extractString("colVarChar", 0); // should be element #2 from first batch
        assertEquals("long long long long value", value);
        value = rangeDataInput.extractString("colVarChar", 3); // will not fit first batch - will be 1st element of the 2nd batch which element 1
        assertEquals("long long long long long long long long long long long long long long long long value", value);
        value = rangeDataInput.extractString("colVarChar", 8); // last in 3rd batch (element #3)
        assertEquals(null, value);
        try {
          value = rangeDataInput.extractString("colVarChar", 9);
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
