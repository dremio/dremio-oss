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

package com.dremio.sabot.op.writer;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.RecordWriter.OutputEntryListener;
import com.dremio.exec.store.WritePartition;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Charsets;

public class WriterOperator implements SingleInputOperator {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterOperator.class);

  private final Listener listener = new Listener();
  private final OperatorContext context;
  private final RecordWriter recordWriter;
  private final String fragmentUniqueId;
  private final VectorContainer output;
  private final WriterOptions options;

  private boolean completedInput = false;

  private State state = State.NEEDS_SETUP;

  private VectorContainer maskedContainer;
  private NullableBigIntVector summaryVector;
  private NullableVarCharVector fragmentIdVector;
  private NullableVarCharVector pathVector;
  private NullableVarBinaryVector metadataVector;
  private NullableIntVector partitionNumberVector;

  private PartitionWriteManager partitionManager;

  private WritePartition partition = null;

  public WriterOperator(OperatorContext context, WriterOptions options, RecordWriter recordWriter) throws OutOfMemoryException {
    this.context = context;
    this.output = VectorContainer.create(context.getAllocator(), RecordWriter.SCHEMA);
    this.options = options;
    final FragmentHandle handle = context.getFragmentHandle();
    this.fragmentUniqueId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    this.recordWriter = recordWriter;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    state.is(State.NEEDS_SETUP);

    if(options.hasPartitions()){
      partitionManager = new PartitionWriteManager(options, incoming);
      this.maskedContainer = partitionManager.getMaskedContainer();
      recordWriter.setup(maskedContainer, listener);
    } else {
      recordWriter.setup(incoming, listener);
    }

    // Create the RecordWriter.SCHEMA vectors.
    fragmentIdVector = output.addOrGet(RecordWriter.FRAGMENT);
    pathVector = output.addOrGet(RecordWriter.PATH);
    summaryVector = output.addOrGet(RecordWriter.RECORDS);
    metadataVector = output.addOrGet(RecordWriter.METADATA);
    partitionNumberVector = output.addOrGet(RecordWriter.PARTITION);
    output.buildSchema();
    output.setInitialCapacity(context.getTargetBatchSize());
    state = State.CAN_CONSUME;
    return output;
  }

  @Override
  public void consumeData(final int records) throws Exception {
    state.is(State.CAN_CONSUME);

    // no partitions.
    if(!options.hasPartitions()){
      if(partition == null){
        partition = WritePartition.NONE;
        recordWriter.startPartition(partition);
      }
      recordWriter.writeBatch(0, records);
      moveToCanProduceStateIfOutputExists();
      return;
    }

    // we're partitioning.

    // always need to keep the masked container in alignment.
    maskedContainer.setRecordCount(records);

    int pointer = 0;
    int start = 0;
    while(pointer < records){
      final WritePartition newPartition = partitionManager.getExistingOrNewPartition(pointer);
      if(newPartition != partition){
        int length = pointer - start;
        if(length > 0){
          recordWriter.writeBatch(start, length);
        }
        this.partition = newPartition;
        start = pointer;
        recordWriter.startPartition(partition);

        moveToCanProduceStateIfOutputExists();
      }
      pointer++;
    }

    // write any remaining to existing partition.
    recordWriter.writeBatch(start, pointer - start);
    moveToCanProduceStateIfOutputExists();
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    output.allocateNew();

    final byte[] fragmentUniqueIdBytes = fragmentUniqueId.getBytes();
    int i = 0;
    for(OutputEntry e : listener.entries){
      fragmentIdVector.setSafe(i, fragmentUniqueIdBytes, 0, fragmentUniqueIdBytes.length);

      summaryVector.setSafe(i, e.recordCount);

      if (e.metadata != null) {
        metadataVector.setSafe(i, e.metadata, 0, e.metadata.length);
      }

      if (e.path != null) {
        byte[] bytePath = e.path.getBytes(Charsets.UTF_8);
        pathVector.setSafe(i, bytePath, 0, bytePath.length);
      }

      if (e.partitionNumber != null) {
        partitionNumberVector.setSafe(i, e.partitionNumber);
      }

      i++;
    }

    listener.entries.clear();

    if(completedInput){
      state = State.DONE;
    } else {
      state = State.CAN_CONSUME;
    }
    return output.setAllCount(i);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    recordWriter.close();
    this.completedInput = true;
    state = State.CAN_PRODUCE;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(recordWriter, output);
  }

  private void moveToCanProduceStateIfOutputExists() {
    if (listener.entries.size() > 0) {
      state = State.CAN_PRODUCE;
    }
  }

  private class Listener implements OutputEntryListener {

    private final List<OutputEntry> entries = new ArrayList<>();

    @Override
    public void recordsWritten(long recordCount, String path, byte[] metadata, Integer partitionNumber) {
      entries.add(new OutputEntry(recordCount, path, metadata, partitionNumber));
    }

  }

  private class OutputEntry {
    private final long recordCount;
    private final String path;
    private final byte[] metadata;
    private final Integer partitionNumber;

    public OutputEntry(long recordCount, String path, byte[] metadata, Integer partitionNumber) {
      super();
      this.recordCount = recordCount;
      this.path = path;
      this.metadata = metadata;
      this.partitionNumber = partitionNumber;
    }

  }

}
