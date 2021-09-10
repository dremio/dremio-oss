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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.commons.collections.CollectionUtils;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.RecordWriter.OutputEntryListener;
import com.dremio.exec.store.RecordWriter.WriteStatsListener;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Charsets;

public class WriterOperator implements SingleInputOperator {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterOperator.class);

  private final Listener listener = new Listener();
  private final StatsListener statsListener = new StatsListener();
  private final OperatorContext context;
  private final OperatorStats stats;
  private final RecordWriter recordWriter;
  private final String fragmentUniqueId;
  private final VectorContainer output;
  private final WriterOptions options;

  private boolean completedInput = false;

  private State state = State.NEEDS_SETUP;

  private VectorContainer maskedContainer;
  private BigIntVector summaryVector;
  private BigIntVector fileSizeVector;
  private VarCharVector fragmentIdVector;
  private VarCharVector pathVector;
  private VarBinaryVector metadataVector;
  private VarBinaryVector icebergMetadataVector;
  private IntVector partitionNumberVector;
  private VarBinaryVector schemaVector;
  private ListVector partitionDataVector;

  private PartitionWriteManager partitionManager;

  private WritePartition partition = null;

  private long writtenRecords = 0L;
  private long writtenRecordLimit;
  private boolean reachedOutputLimit = false;

  public enum Metric implements MetricDef {
    BYTES_WRITTEN,    // Number of bytes written to the output file(s)
    OUTPUT_LIMITED;   // 1, if the output limit was reached; 0, if not

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public WriterOperator(OperatorContext context, WriterOptions options, RecordWriter recordWriter) throws OutOfMemoryException {
    this.context = context;
    this.stats = context.getStats();
    this.output = context.createOutputVectorContainer(RecordWriter.SCHEMA);
    this.options = options;
    final FragmentHandle handle = context.getFragmentHandle();
    this.fragmentUniqueId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    this.recordWriter = recordWriter;
    this.writtenRecordLimit = options.getRecordLimit();
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    state.is(State.NEEDS_SETUP);

    if(options.hasPartitions() || options.hasDistributions()){
      partitionManager = new PartitionWriteManager(options, incoming,
        options.getIcebergWriterOperation() != WriterOptions.IcebergWriterOperation.NONE);
      this.maskedContainer = partitionManager.getMaskedContainer();
      recordWriter.setup(maskedContainer, listener, statsListener);
    } else {
      recordWriter.setup(incoming, listener, statsListener);
    }

    // Create the RecordWriter.SCHEMA vectors.
    fragmentIdVector = output.addOrGet(RecordWriter.FRAGMENT);
    pathVector = output.addOrGet(RecordWriter.PATH);
    summaryVector = output.addOrGet(RecordWriter.RECORDS);
    fileSizeVector = output.addOrGet(RecordWriter.FILESIZE);
    metadataVector = output.addOrGet(RecordWriter.METADATA);
    partitionNumberVector = output.addOrGet(RecordWriter.PARTITION);
    icebergMetadataVector = output.addOrGet(RecordWriter.ICEBERG_METADATA);
    schemaVector = output.addOrGet(RecordWriter.FILE_SCHEMA);
    partitionDataVector  = output.addOrGet(RecordWriter.PARTITION_DATA);
    output.buildSchema();
    output.setInitialCapacity(context.getTargetBatchSize());
    state = State.CAN_CONSUME;
    return output;
  }

  @Override
  public void consumeData(final int records) throws Exception {
    state.is(State.CAN_CONSUME);

    // no partitions.
    if(!options.hasPartitions() && !options.hasDistributions()){
      if(partition == null){
        partition = WritePartition.NONE;
        recordWriter.startPartition(partition);
      }
      recordWriter.writeBatch(0, records);
      writtenRecords += records;
      if (writtenRecords > writtenRecordLimit) {
        recordWriter.close();
        reachedOutputLimit = true;
        state = State.CAN_PRODUCE;
      } else {
        moveToCanProduceStateIfOutputExists();
      }
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

      if (e.icebergMetadata != null) {
        icebergMetadataVector.setSafe(i, e.icebergMetadata, 0, e.icebergMetadata.length);
      }

      if (e.path != null) {
        byte[] bytePath = e.path.getBytes(Charsets.UTF_8);
        pathVector.setSafe(i, bytePath, 0, bytePath.length);
      }

      if (e.partitionNumber != null) {
        partitionNumberVector.setSafe(i, e.partitionNumber);
      }

      if (e.schema != null) {
        schemaVector.setSafe(i, e.schema, 0, e.schema.length);
      }

      if (CollectionUtils.isNotEmpty(e.partitions)) {
        UnionListWriter listWriter = partitionDataVector.getWriter();
        listWriter.setPosition(i);
        listWriter.startList();
        for (IcebergPartitionData partitionData : e.partitions) {
          byte[] bytes = IcebergSerDe.serializeToByteArray(partitionData);
          ArrowBuf buffer = null;
          try {
            buffer = context.getAllocator().buffer(bytes.length);
            buffer.setBytes(0, bytes);
            listWriter.varBinary().writeVarBinary(0, bytes.length, buffer);
          } finally {
            AutoCloseables.close(buffer);
          }
        }
        listWriter.endList();
      }

      fileSizeVector.setSafe(i, e.fileSize);

      i++;
    }

    listener.entries.clear();

    if(completedInput || reachedOutputLimit) {
      stats.addLongStat(Metric.OUTPUT_LIMITED, reachedOutputLimit ? 1 : 0);
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
    public void recordsWritten(long recordCount, long fileSize, String path, byte[] metadata, Integer partitionNumber, byte[] icebergMetadata, byte[] schema, Collection<IcebergPartitionData> partitions) {
      entries.add(new OutputEntry(recordCount, fileSize, path, metadata, icebergMetadata, partitionNumber, schema, partitions));
    }

  }

  private class StatsListener implements WriteStatsListener {
    @Override
    public void bytesWritten(long byteCount) {
      stats.addLongStat(Metric.BYTES_WRITTEN, byteCount);
    }
  }

  private class OutputEntry {
    private final long recordCount;
    private final long fileSize;
    private final String path;
    private final byte[] metadata;
    private final byte[] icebergMetadata;
    private final byte[] schema;
    private final Integer partitionNumber;
    private final Collection<IcebergPartitionData> partitions;

    OutputEntry(long recordCount, long fileSize, String path, byte[] metadata, byte[] icebergMetadata, Integer partitionNumber, byte[] schema, Collection<IcebergPartitionData> partitions) {
      this.recordCount = recordCount;
      this.fileSize = fileSize;
      this.path = path;
      this.metadata = metadata;
      this.icebergMetadata = icebergMetadata;
      this.partitionNumber = partitionNumber;
      this.schema = schema;
      this.partitions = partitions;
    }

  }

}
