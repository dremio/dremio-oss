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
package com.dremio.sabot.op.writer;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.BasePath;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * This implementation does not perform any operation while consuming the data but initializes the
 * outgoing vectors to emit maximum one batch of the custom output records.
 *
 * <p>This can be used for writing summary records
 */
public class CustomOutputHandler implements WriterCommitterOutputHandler {

  private final VectorContainer outgoing;
  private final OperatorContext context;
  private SingleInputOperator.State state = SingleInputOperator.State.NEEDS_SETUP;

  public CustomOutputHandler(OperatorContext context, WriterCommitterPOP config) {
    this.outgoing = context.createOutputVectorContainer(RecordWriter.SCHEMA);
    this.context = context;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());
    outgoing.allocateNew();
    outgoing.setRecordCount(0);
    outgoing.setAllCount(0);
    state = SingleInputOperator.State.CAN_CONSUME;
    final OperatorStats operatorStats = context.getStats();
    operatorStats.setProfileDetails(UserBitShared.OperatorProfileDetails.newBuilder().build());
    return outgoing;
  }

  @Override
  public SingleInputOperator.State getState() {
    return state;
  }

  @Override
  public int outputData() throws Exception {
    int recordCount = outgoing.getRecordCount();
    this.state = SingleInputOperator.State.DONE; // produce only one batch
    return recordCount;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    Preconditions.checkState(this.state.equals(SingleInputOperator.State.CAN_CONSUME));
    this.state = SingleInputOperator.State.CAN_PRODUCE;
  }

  @Override
  public void consumeData(int records) throws Exception {
    // Do nothing
  }

  @Override
  public void write(WriterCommitterRecord rec) {
    int idx = outgoing.getRecordCount();
    setVarcharValue(idx, RecordWriter.FRAGMENT, rec.fragment());
    setVarcharValue(idx, RecordWriter.PATH, rec.path());
    setVarBinaryValue(idx, RecordWriter.METADATA, rec.metadata());
    setVarBinaryValue(idx, RecordWriter.ICEBERG_METADATA, rec.icebergMetadata());
    setVarBinaryValue(idx, RecordWriter.FILE_SCHEMA, rec.fileSchema());
    setLongValue(idx, RecordWriter.RECORDS, rec.records());
    setLongValue(idx, RecordWriter.FILESIZE, rec.fileSize());
    setIntValue(idx, RecordWriter.PARTITION, rec.partition());
    setIntValue(idx, RecordWriter.OPERATION_TYPE, rec.operationType());
    setPartitionData(idx, rec.partitionData());
    setVarBinaryValue(idx, RecordWriter.REFERENCED_DATA_FILES, rec.referencedDataFiles());
    outgoing.setAllCount(idx + 1);
    outgoing.setRecordCount(idx + 1);
  }

  private void setPartitionData(int idx, List<VarBinaryHolder> partitionData) {
    ListVector partitionDataVV = getVV(ListVector.class, RecordWriter.PARTITION_DATA);
    if (partitionData == null) {
      partitionDataVV.setNull(idx);
      partitionDataVV.setValueCount(idx + 1);
    } else {
      UnionListWriter listWriter = partitionDataVV.getWriter();
      listWriter.startList();
      for (int i = 0; i < partitionData.size(); i++) {
        listWriter.setPosition(i);
        listWriter.write(partitionData.get(i));
      }
      listWriter.setValueCount(partitionData.size());
      listWriter.endList();
    }
  }

  private void setIntValue(int idx, Field field, Integer value) {
    IntVector intVector = getVV(IntVector.class, field);
    if (value == null) {
      intVector.setNull(idx);
    } else {
      intVector.setSafe(idx, value);
    }
    intVector.setValueCount(idx + 1);
  }

  private void setLongValue(int idx, Field field, Long value) {
    BigIntVector bigIntVector = getVV(BigIntVector.class, field);
    if (value == null) {
      bigIntVector.setNull(idx);
    } else {
      bigIntVector.setSafe(idx, value);
    }
    bigIntVector.setValueCount(idx + 1);
  }

  private void setVarBinaryValue(int idx, Field field, VarBinaryHolder value) {
    VarBinaryVector varBinaryVV = getVV(VarBinaryVector.class, field);
    if (value == null) {
      varBinaryVV.setNull(idx);
    } else {
      varBinaryVV.setSafe(idx, value);
    }
    varBinaryVV.setValueCount(idx + 1);
  }

  private void setVarcharValue(int idx, Field field, VarCharHolder value) {
    VarCharVector varcharVV = getVV(VarCharVector.class, field);
    if (value == null) {
      varcharVV.setNull(idx);
    } else {
      varcharVV.setSafe(idx, value);
    }
    varcharVV.setValueCount(idx + 1);
  }

  private <T extends ValueVector> T getVV(Class<T> clazz, Field field) {
    return outgoing
        .getValueAccessorById(
            clazz,
            outgoing.getSchema().getFieldId(BasePath.getSimple(field.getName())).getFieldIds()[0])
        .getValueVector();
  }

  @Override
  public void close() throws Exception {
    this.state = SingleInputOperator.State.DONE;
    AutoCloseables.close(outgoing);
  }
}
