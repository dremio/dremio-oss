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
package com.dremio.exec.work.protector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.NoOutputLogger;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.exec.testing.InjectedOutOfMemoryError;
import com.dremio.exec.work.user.LocalUserUtil;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.op.aggregate.hash.HashAggOperator;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;

public class TestQueryReAttempt extends BaseTestQuery {

  private static final int COUNT = 5;

  private BitVector bitVector(String name) {
    BitVector vec = new BitVector(name, getAllocator());
    vec.allocateNew(COUNT);
    vec.set(0, 1);
    vec.set(1, 0);
    vec.setNull(2);
    vec.set(3, 1);
    vec.set(4, 1);

    vec.setValueCount(COUNT);
    return vec;
  }

  private NonNullableStructVector structVector(String name) {
    NonNullableStructVector vector = new NonNullableStructVector(name, allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null), null);

    ComplexWriterImpl structWriter = new ComplexWriterImpl("colMap", vector);

    // colMap contains the following records:
    // { bigint: 23, nVarCharCol: 'value', nListCol: [1970-01-01, 1970-01-03, 1969-12-31], nUnionCol: 2 }
    // { bigint: 223, nVarCharCol: 'long long value', nListCol: [1969-12-29, 1969-12-29], nUnionCol: 'long long value' }
    // { bigint: 54645, nMap: { a: 1 } }
    // { }
    // { bigint: 234543 }

    BaseWriter.StructWriter structWr = structWriter.rootAsStruct();

    structWr.setPosition(0);
    structWr.start();
    structWr.bigInt("bigint").writeBigInt(23);
    structWr.list("list").startList();
    structWr.list("list").dateMilli().writeDateMilli(2312L);
    structWr.list("list").dateMilli().writeDateMilli(234823492L);
    structWr.list("list").dateMilli().writeDateMilli(-2382437L);
    structWr.list("list").endList();
    structWr.integer("union").writeInt(2);
    structWr.end();

    structWr.setPosition(1);
    structWr.start();
    structWr.bigInt("bigint").writeBigInt(223);
    structWr.list("list").startList();
    structWr.list("list").dateMilli().writeDateMilli(-234238942L);
    structWr.list("list").dateMilli().writeDateMilli(-234238942L);
    structWr.list("list").endList();
    structWr.end();

    structWr.setPosition(2);
    structWr.start();
    structWr.bigInt("bigint").writeBigInt(54645L);
    structWr.struct("map").start();
    structWr.struct("map").integer("a").writeInt(1);
    structWr.struct("map").end();
    structWr.end();

    structWr.setPosition(4);
    structWr.start();
    structWr.bigInt("bigint").writeBigInt(234543L);
    structWr.end();

    structWriter.setValueCount(COUNT);
    vector.setValueCount(COUNT);
    return vector;
  }

  private IntVector intVector(String name) {
    IntVector vec = new IntVector(name, allocator);
    vec.allocateNew(5);
    vec.set(0, 20);
    vec.set(1, 50);
    vec.set(2, -2000);
    vec.set(3, 327345);
    vec.setNull(4);

    vec.setValueCount(COUNT);
    return vec;
  }

  @BeforeClass
  public static void enableReAttempts() {
    setSessionOption(ExecConstants.ENABLE_VECTORIZED_HASHAGG, "false");
    setSessionOption(ExecConstants.ENABLE_REATTEMPTS.getOptionName(), "true");
  }

  /**
   * Injects an OOM in HashAgg and confirm that the query succeeds anyway
   */
  @Test
  public void testOOMExceptionReAttempt() throws Exception {
    final String controls = Controls.newBuilder()
            .addException(HashAggOperator.class, HashAggOperator.INJECTOR_DO_WORK_OOM, OutOfMemoryException.class)
            .build();
    ControlsInjectionUtil.setControls(client, controls);
    test(getFile("queries/tpch/01.sql"));
  }

  @Test
  public void testOOMErrorReAttempt() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(HashAggOperator.class, HashAggOperator.INJECTOR_DO_WORK_OOM_ERROR, InjectedOutOfMemoryError.class)
      .build();
    ControlsInjectionUtil.setControls(client, controls);
    test(getFile("queries/tpch/01.sql"));
  }

  @Test
  public void testSupportedSchemaChange() throws Exception {
    // original schema [field1=bit, field2=int]
    // new schema [field1=bit, field3=map, field2=int]
    List<ValueVector> original = Arrays.<ValueVector>asList(bitVector("field1"), intVector("field2"));
    List<ValueVector> updated = Arrays.asList(bitVector("field1"), structVector("field3"), intVector("field2"));
    checkSchemaChange(original, updated);
  }

  @Test
  public void testNonSupportedSchemaChange() throws Exception {
    // field2 changes from int to map
    // original schema [field1=bit, field2=int]
    // new schema [field1=bit, field2=map, field3=int]
    List<ValueVector> original = Arrays.<ValueVector>asList(bitVector("field1"), intVector("field2"));
    List<ValueVector> updated = Arrays.asList(bitVector("field1"), structVector("field2"), intVector("field3"));

    try {
      checkSchemaChange(original, updated);
    } catch (UserException ex) {
      assertEquals(ErrorType.SCHEMA_CHANGE, ex.getErrorType());
    }
  }

  private static QueryWritableBatch createQueryWritableBatch(List<ValueVector> vv) {
    WritableBatch batch = WritableBatch.getBatchNoHV(COUNT, vv, false);
    // I'm missing the queryId, but it should be fine as it's not being used in the tested path
    QueryData header = QueryData.newBuilder().setRowCount(COUNT).setDef(batch.getDef()).build();
    return new QueryWritableBatch(header, batch.getBuffers());
  }

  private void checkSchemaChange(List<ValueVector> first, List<ValueVector> second) throws Exception {
    OptionManager options = Mockito.mock(OptionManager.class);
    ReAttemptHandler attemptHandler = new ExternalAttemptHandler(options);

    // pass first batch (old schema)
    {
      attemptHandler.newAttempt();
      QueryWritableBatch firstBatch = createQueryWritableBatch(first);
      QueryWritableBatch convertedBatch = attemptHandler.convertIfNecessary(firstBatch);

      assertSame("first batch should pass through", firstBatch, convertedBatch);
      release(firstBatch);
    }

    // pass second batch (new schema)
    {
      attemptHandler.newAttempt();
      QueryWritableBatch secondBatch = createQueryWritableBatch(second);
      QueryWritableBatch convertedBatch;
      try {
        convertedBatch = attemptHandler.convertIfNecessary(secondBatch);
        // if the call didn't fail, the extra fields will be released,
        secondBatch = null; // makes sure we don't try to do it again
      } finally {
        release(secondBatch);
      }

      // confirm the converted batch contains the old schema
      assertContainsFields(extractSchema(convertedBatch), first);

      assertBatchCanBeLoaded(convertedBatch, first);
    }
  }

  @Test
  public void testCTAS() {
    OptionManager options = Mockito.mock(OptionManager.class);
    ReAttemptHandler attemptHandler = new ExternalAttemptHandler(options);
    AttemptId id = new AttemptId();
    final UserException userException = UserException.memoryError(null).build(NoOutputLogger.INSTANCE);
    assertEquals(AttemptReason.NONE, attemptHandler.isRecoverable(new ReAttemptContext(id, userException, false, true)));
  }

  private void assertBatchCanBeLoaded(QueryWritableBatch batch, List<ValueVector> original) throws Exception {
    RpcOutcomeListener<GeneralRPCProtos.Ack> listener = Mockito.mock(RpcOutcomeListener.class);

    try (QueryDataBatch dataBatch = LocalUserUtil.acquireData(allocator, listener, batch);
         RecordBatchLoader loader = new RecordBatchLoader(allocator)) {
      loader.load(dataBatch.getHeader().getDef(), dataBatch.getData());

      try (RecordBatchData loadedBatch = new RecordBatchData(loader, allocator)) {
        assertContainsFields(extractSchema(loadedBatch), original);
      }
    }
  }

  private static Set<String> extractSchema(QueryWritableBatch batch) {
    List<SerializedField> fields = batch.getHeader().getDef().getFieldList();
    Set<String> schema = Sets.newHashSet();
    for (SerializedField field : fields) {
      schema.add(field.getNamePart().getName());
    }
    return schema;
  }

  private static Set<String> extractSchema(RecordBatchData batch) {
    Set<String> schema = Sets.newHashSet();
    for (ValueVector vv : batch.getVectors()) {
      schema.add(vv.getField().getName());
    }
    return schema;
  }

  private static void assertContainsFields(Set<String> fields, List<ValueVector> original) {
    assertEquals(original.size(), fields.size());

    for (ValueVector vv : original) {
      final String fieldName = vv.getField().getName();
      assertTrue("original field '" + fieldName + "' is not part of the new schema", fields.contains(fieldName));
    }
  }

  private static void release(QueryWritableBatch batch) {
    if (batch != null) {
      for (ByteBuf byteBuf : batch.getBuffers()) {
        byteBuf.release();
      }
    }
  }
}
