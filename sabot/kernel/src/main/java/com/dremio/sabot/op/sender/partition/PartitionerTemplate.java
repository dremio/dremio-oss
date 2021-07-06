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
package com.dremio.sabot.op.sender.partition;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

import javax.inject.Named;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.SuppressForbidden;
import com.dremio.common.expression.BasePath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.sig.RuntimeOverridden;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator.Metric;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public abstract class PartitionerTemplate implements Partitioner {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerTemplate.class);

  private SelectionVector2 sv2;
  private VectorAccessible incoming;
  private OperatorStats stats;
  private int start;
  private int end;
  private List<OutgoingRecordBatch> outgoingBatches = Lists.newArrayList();
  private final Constructor<OutgoingRecordBatch> innerConstructor;

  private int minOutgoingBatchRecordCount;

  /** how much memory should a partition use */
  private int targetOutgoingBatchSize;

  public OutgoingRecordBatch newOutgoingRecordBatch(OperatorStats stats, HashPartitionSender operator, AccountingExecTunnel tunnel,
      OperatorContext context, BufferAllocator allocator, int oppositeMinorFragmentId, int outgoingBatchRecordCount) {
    try {
      return innerConstructor.newInstance(this, stats, operator, tunnel, context, allocator,
        oppositeMinorFragmentId, outgoingBatchRecordCount);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  @SuppressForbidden
  public PartitionerTemplate() throws SchemaChangeException {
    try{
      this.innerConstructor = (Constructor<OutgoingRecordBatch>) this.getClass().getDeclaredClasses()[0].getConstructor(
        this.getClass(), OperatorStats.class, HashPartitionSender.class, AccountingExecTunnel.class,
        OperatorContext.class, BufferAllocator.class, int.class, int.class);
      innerConstructor.setAccessible(true);
    }catch(Exception ex){
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public List<? extends PartitionOutgoingBatch> getOutgoingBatches() {
    return outgoingBatches;
  }

  @Override
  public PartitionOutgoingBatch getOutgoingBatch(int index) {
    if ( index >= start && index < end) {
      return outgoingBatches.get(index - start);
    }
    return null;
  }

  @Override
  public final void setup(VectorAccessible incoming,
                          HashPartitionSender popConfig,
                          OperatorStats stats,
                          OperatorContext context,
                          TunnelProvider tunnelProvider,
                          int numPartitions,
                          int start, int end) {

    this.incoming = incoming;
    this.stats = stats;
    this.start = start;
    this.end = end;
    doSetup(context.getFunctionContext(), incoming, null);

    final OptionManager options = context.getOptions();
    minOutgoingBatchRecordCount = (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MIN);
    // how many records we can keep in memory before we are forced to flush the outgoing batch
    final int outgoingBatchRecordCount = (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
    targetOutgoingBatchSize = popConfig.getProps().getTargetBatchSize();

    int fieldId = 0;
    for (MinorFragmentEndpoint destination : popConfig.getDestinations(context.getEndpointsIndex())) {
      // create outgoingBatches only for subset of Destination Points
      if ( fieldId >= start && fieldId < end ) {
        logger.debug("start: {}, count: {}, fieldId: {}", start, end, fieldId);
        outgoingBatches.add(newOutgoingRecordBatch(stats, popConfig,
            tunnelProvider.getExecTunnel(destination.getEndpoint()), context, context.getAllocator(), destination.getMinorFragmentId(),
            outgoingBatchRecordCount)
        );
      }
      fieldId++;
    }

    for (OutgoingRecordBatch outgoingRecordBatch : outgoingBatches) {
      outgoingRecordBatch.initializeBatch();
    }

    SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();
    switch(svMode){
      case TWO_BYTE:
        this.sv2 = incoming.getSelectionVector2();
        break;

      case NONE:
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }
  }

  @Override
  public OperatorStats getStats() {
    return stats;
  }

  /**
   * Flush each outgoing record batch, and optionally reset the state of each outgoing record
   * batch (on schema change).  Note that the schema is updated based on incoming at the time
   * this function is invoked.
   */
  @Override
  public void flushOutgoingBatches() {
    logger.debug("Attempting to flush all outgoing batches");
    for (OutgoingRecordBatch batch : outgoingBatches) {
      batch.flush();
    }
  }

  @Override
  public void sendTermination() {
    logger.debug("Sending Stream Termination message.");
    for (OutgoingRecordBatch batch : outgoingBatches) {
      batch.sendTermination();
    }
  }

  @Override
  public void partitionBatch(final VectorAccessible incoming) throws IOException {
    final SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();
    final int recordCount = incoming.getRecordCount();

    // Keeping the for loop inside the case to avoid case evaluation for each record.
    switch(svMode) {
      case NONE:
        for (int recordId = 0; recordId < recordCount; ++recordId) {
          doCopy(recordId);
        }
        break;

      case TWO_BYTE:
        final SelectionVector2 sv2 = this.sv2;
        for (int recordId = 0; recordId < recordCount; ++recordId) {
          int svIndex = sv2.getIndex(recordId);
          doCopy(svIndex);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }
  }

  /**
   * Helper method to copy data based on partition
   * @param svIndex
   * @throws IOException
   */
  private void doCopy(int svIndex) throws IOException {
    int index = doEval(svIndex);
    if ( index >= start && index < end) {
      OutgoingRecordBatch outgoingBatch = outgoingBatches.get(index - start);
      outgoingBatch.copy(svIndex);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoingBatches);
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") OutgoingRecordBatch[] outgoing) throws SchemaChangeException;
  public abstract int doEval(@Named("inIndex") int inIndex);

  public class OutgoingRecordBatch implements PartitionOutgoingBatch, VectorAccessible, AutoCloseable {

    private final AccountingExecTunnel tunnel;
    private final HashPartitionSender operator;
    private final OperatorContext context;
    private final BufferAllocator allocator;
    private final VectorContainer vectorContainer = new VectorContainer();
    private final int oppositeMinorFragmentId;
    private final OperatorStats stats;

    private int maxRecordCount;

    private boolean dropAll = false;
    private int recordCount;
    private int totalRecords;

    public OutgoingRecordBatch(OperatorStats stats, HashPartitionSender operator, AccountingExecTunnel tunnel,
                               OperatorContext context, BufferAllocator allocator, int oppositeMinorFragmentId, int maxRecordCount) {
      this.context = context;
      this.allocator = allocator;
      this.operator = operator;
      this.tunnel = tunnel;
      this.stats = stats;
      this.oppositeMinorFragmentId = oppositeMinorFragmentId;
      this.maxRecordCount = maxRecordCount;
    }

    protected void copy(int inIndex) throws IOException {
      doEval(inIndex, recordCount);
      recordCount++;
      totalRecords++;
      if (recordCount == maxRecordCount) {
        flush();
      }
    }

    @Override
    public void terminate() {
      // receiver already terminated, don't send anything to it from now on
      dropAll = true;
    }

    @RuntimeOverridden
    protected void doSetup(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing) {}

    @RuntimeOverridden
    protected void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex) { }

    public void sendTermination() {
      final FragmentHandle handle = context.getFragmentHandle();
      FragmentStreamComplete completion = FragmentStreamComplete.newBuilder()
          .setQueryId(handle.getQueryId())
          .setSendingMajorFragmentId(handle.getMajorFragmentId())
          .setSendingMinorFragmentId(handle.getMinorFragmentId())
          .setReceivingMajorFragmentId(operator.getReceiverMajorFragmentId())
          .addReceivingMinorFragmentId(oppositeMinorFragmentId)
          .build();
      tunnel.sendStreamComplete(completion);

      dropAll = true;
    }

    public void flush() {
      if (dropAll) {
        // If we are in dropAll mode, we still want to copy the data, because we can't stop copying a single outgoing
        // batch with out stopping all outgoing batches. Other option is check for status of dropAll before copying
        // every single record in copy method which has the overhead for every record all the time. Resetting the output
        // count, reusing the same buffers and copying has overhead only for outgoing batches whose receiver has
        // terminated.

        // Reset the count to 0 and use existing buffers for exhausting input where receiver of this batch is terminated
        recordCount = 0;
        return;
      }

      if(recordCount == 0){
        return;
      }

      final FragmentHandle handle = context.getFragmentHandle();

      vectorContainer.setRecordCount(recordCount);
      vectorContainer.setAllCount(recordCount);
      if (!vectorContainer.hasSchema()) {
        vectorContainer.buildSchema();
      }

      FragmentWritableBatch writableBatch = FragmentWritableBatch.create(
          handle.getQueryId(),
          handle.getMajorFragmentId(),
          handle.getMinorFragmentId(),
          operator.getReceiverMajorFragmentId(),
          vectorContainer,
          oppositeMinorFragmentId);

      // update the outgoing batch size if the buffer is too big
      final long batchLength = writableBatch.getByteCount();
      if (batchLength > targetOutgoingBatchSize) {
        maxRecordCount = Math.max(minOutgoingBatchRecordCount, maxRecordCount/2);
      } else if (batchLength * 2 <= targetOutgoingBatchSize) {
        maxRecordCount = Math.min(Character.MAX_VALUE, maxRecordCount * 2);
      }

      updateStats(writableBatch);
      tunnel.sendRecordBatch(writableBatch);

      // reset values and reallocate the buffer for each value vector based on the incoming batch.
      // NOTE: the value vector is directly referenced by generated code; therefore references
      // must remain valid.
      recordCount = 0;
      vectorContainer.zeroVectors();
      for (VectorWrapper<?> wrapper : vectorContainer) {
        AllocationHelper.allocateNew(wrapper.getValueVector(), maxRecordCount);
      }
    }

    public void updateStats(FragmentWritableBatch writableBatch) {
      stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
      stats.addLongStat(Metric.BATCHES_SENT, 1);
      stats.addLongStat(Metric.RECORDS_SENT, writableBatch.getRecordCount());
    }

    /**
     * Initialize the OutgoingBatch based on the current schema in incoming RecordBatch
     */
    public void initializeBatch() {
      for (VectorWrapper<?> v : incoming) {
        // create new vector
        ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), allocator);
        outgoingVector.setInitialCapacity(maxRecordCount);
        vectorContainer.add(outgoingVector);
      }
      vectorContainer.allocateNew();
      doSetup(incoming, vectorContainer);
    }

    public void resetBatch() {
      recordCount = 0;
      vectorContainer.clear();
    }

    @Override
    public BatchSchema getSchema() {
      return incoming.getSchema();
    }

    @Override
    public int getRecordCount() {
      return recordCount;
    }


    @Override
    public long getTotalRecords() {
      return totalRecords;
    }

    @Override
    public TypedFieldId getValueVectorId(BasePath path) {
      return vectorContainer.getValueVectorId(path);
    }

    @Override
    public <T extends ValueVector> VectorWrapper<T> getValueAccessorById(Class<T> clazz, int... fieldIds) {
      return vectorContainer.getValueAccessorById(clazz, fieldIds);
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      return vectorContainer.iterator();
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      throw new UnsupportedOperationException();
    }

    public void close(){
      vectorContainer.close();
    }

  }
}
