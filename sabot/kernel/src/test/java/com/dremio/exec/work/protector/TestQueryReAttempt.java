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
package com.dremio.exec.work.protector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.exec.work.user.LocalUserUtil;
import com.dremio.sabot.op.aggregate.hash.HashAggOperator;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;

public class TestQueryReAttempt extends BaseTestQuery {

  private static final int COUNT = 5;

  private static NullableBitVector bitVector(String name) {
    NullableBitVector vec = new NullableBitVector(name, getAllocator());
    vec.allocateNew(COUNT);
    vec.getMutator().set(0, 1);
    vec.getMutator().set(1, 0);
    vec.getMutator().setNull(2);
    vec.getMutator().set(3, 1);
    vec.getMutator().set(4, 1);

    vec.getMutator().setValueCount(COUNT);
    return vec;
  }

  private static MapVector mapVector(String name) {
    MapVector vector = new MapVector(name, allocator, null);

    ComplexWriterImpl mapWriter = new ComplexWriterImpl("colMap", vector);

    // colMap contains the following records:
    // { bigint: 23, nVarCharCol: 'value', nListCol: [1970-01-01, 1970-01-03, 1969-12-31], nUnionCol: 2 }
    // { bigint: 223, nVarCharCol: 'long long value', nListCol: [1969-12-29, 1969-12-29], nUnionCol: 'long long value' }
    // { bigint: 54645, nMap: { a: 1 } }
    // { }
    // { bigint: 234543 }

    BaseWriter.MapWriter mapWr = mapWriter.rootAsMap();

    mapWr.setPosition(0);
    mapWr.start();
    mapWr.bigInt("bigint").writeBigInt(23);
    mapWr.list("list").startList();
    mapWr.list("list").dateMilli().writeDateMilli(2312L);
    mapWr.list("list").dateMilli().writeDateMilli(234823492L);
    mapWr.list("list").dateMilli().writeDateMilli(-2382437L);
    mapWr.list("list").endList();
    mapWr.integer("union").writeInt(2);
    mapWr.end();

    mapWr.setPosition(1);
    mapWr.start();
    mapWr.bigInt("bigint").writeBigInt(223);
    mapWr.list("list").startList();
    mapWr.list("list").dateMilli().writeDateMilli(-234238942L);
    mapWr.list("list").dateMilli().writeDateMilli(-234238942L);
    mapWr.list("list").endList();
    mapWr.end();

    mapWr.setPosition(2);
    mapWr.start();
    mapWr.bigInt("bigint").writeBigInt(54645L);
    mapWr.map("map").start();
    mapWr.map("map").integer("a").writeInt(1);
    mapWr.map("map").end();
    mapWr.end();

    mapWr.setPosition(4);
    mapWr.start();
    mapWr.bigInt("bigint").writeBigInt(234543L);
    mapWr.end();

    mapWriter.setValueCount(COUNT);
    vector.getMutator().setValueCount(COUNT);
    return vector;
  }

  private static NullableIntVector intVector(String name) {
    NullableIntVector vec = new NullableIntVector(name, allocator);
    vec.allocateNew(5);
    vec.getMutator().set(0, 20);
    vec.getMutator().set(1, 50);
    vec.getMutator().set(2, -2000);
    vec.getMutator().set(3, 327345);
    vec.getMutator().setNull(4);

    vec.getMutator().setValueCount(COUNT);
    return vec;
  }

  @BeforeClass
  public static void enableReAttempts() {
    setSessionOption(ExecConstants.ENABLE_REATTEMPTS.getOptionName(), "true");
  }

  /**
   * Injects an OOM in HashAgg and confirm that the query succeeds anyway
   */
  @Test
  public void testOOMReAttempt() throws Exception {
    final String controls = Controls.newBuilder()
            .addException(HashAggOperator.class, HashAggOperator.INJECTOR_DO_WORK, OutOfMemoryException.class)
            .build();
    ControlsInjectionUtil.setControls(client, controls);
    test(getFile("queries/tpch/01.sql"));
  }

  @Test
  public void testSupportedSchemaChange() throws Exception {
    // original schema [field1=bit, field2=int]
    // new schema [field1=bit, field3=map, field2=int]
    List<ValueVector> original = Arrays.<ValueVector>asList(bitVector("field1"), intVector("field2"));
    List<ValueVector> updated = Arrays.asList(bitVector("field1"), mapVector("field3"), intVector("field2"));
    checkSchemaChange(original, updated);
  }

  @Test
  public void testNonSupportedSchemaChange() throws Exception {
    // field2 changes from int to map
    // original schema [field1=bit, field2=int]
    // new schema [field1=bit, field2=map, field3=int]
    List<ValueVector> original = Arrays.<ValueVector>asList(bitVector("field1"), intVector("field2"));
    List<ValueVector> updated = Arrays.asList(bitVector("field1"), mapVector("field2"), intVector("field3"));

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

      assertTrue("first batch should pass through", firstBatch == convertedBatch);

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

  private static void assertBatchCanBeLoaded(QueryWritableBatch batch, List<ValueVector> original) throws Exception {
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
