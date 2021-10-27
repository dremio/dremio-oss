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
package com.dremio.exec.store.parquet;

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.scan.MutatorSchemaChangeCallBack;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.DatasetHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;


/**
 * Parquet scan table function
 */
public abstract class ScanTableFunction extends AbstractTableFunction {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanTableFunction.class);

  protected final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();
  protected RecordReader currentRecordReader;

  private ScanOperator.ScanMutator mutator;
  private MutatorSchemaChangeCallBack callBack = new MutatorSchemaChangeCallBack();
  protected FragmentExecutionContext fec;
  protected OpProps props;
  private VarBinaryVector inputSplits;
  private VarBinaryVector inputColIds;
  private int batchSize;
  private int currentRow;
  private BatchSchema schema;
  private List<SchemaPath> selectedColumns;
  private List<RuntimeFilter> runtimeFilters = new ArrayList<>();
  private boolean isColIdMapSet = true;
  // This is set to true after we are done consuming from upstream and we want to produce the
  // remianing buffered splits if present.
  private boolean produceFromBufferedSplits = false;
  private BoostBufferManager boostBufferManager;

  public ScanTableFunction(FragmentExecutionContext fec,
                           OperatorContext context,
                           OpProps props,
                           TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.fec = fec;
    this.props = props;
    this.schema = functionConfig.getFunctionContext().getFullSchema();
    this.selectedColumns = functionConfig.getFunctionContext().getColumns() == null ? null : ImmutableList.copyOf(functionConfig.getFunctionContext().getColumns());
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    this.incoming = accessible;
    this.outgoing = context.createOutputVectorContainer();
    this.mutator = new ScanOperator.ScanMutator(outgoing, fieldVectorMap, context, callBack);
    schema.maskAndReorder(functionConfig.getFunctionContext().getColumns()).materializeVectors(selectedColumns, mutator);
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    callBack.getSchemaChangedAndReset();

    inputSplits = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordReader.SPLIT_INFORMATION);
    if (DatasetHelper.isIcebergFile(functionConfig.getFunctionContext().getFormatSettings())) {
      inputColIds = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordReader.COL_IDS);
      isColIdMapSet = false;
    }
    createRecordReaderIterator();
    //initialise boost buffer manager here
    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    currentRow = row;
    if (row != 0) {
      return;
    }

    if (!isColIdMapSet) {
      setIcebergColumnIds(inputColIds.get(0));
      isColIdMapSet = true;
    }

    if (produceFromBufferedSplits) {
      // This is setting up the next reader so that when processRow() is called the record readers
      // from the buffered splits can be processed
      setupNextReader();
      return;
    }

    batchSize = incoming.getRecordCount();
    if (batchSize == 0) {
      return;
    }

    // getReaders
    List<SplitAndPartitionInfo> splits = new ArrayList<>();
    for(int record=0; record<batchSize; ++record) {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(inputSplits.get(record));
           ObjectInput in = new ObjectInputStream(bis)) {
        splits.add((SplitAndPartitionInfo) in.readObject());
      }
      catch (Exception e) {
        throw UserException
                .dataReadError(e)
                .message("Failed to read input split information.")
                .build(logger);
      }
    }

    try {
      addSplits(splits);
    } catch (Exception e) {
      ScanOperator.handleExceptionDuringScan(e, functionConfig.getFunctionContext().getReferencedTables(), logger);
    }
    setupNextReader();
  }

  void setupNextReader() throws Exception {
    OperatorStats stats = context.getStats();
    if (!getRecordReaderIterator().hasNext()) {
      return;
    }
    try {
      stats.startSetup();
      currentRecordReader = getRecordReaderIterator().next();
      this.runtimeFilters.forEach(currentRecordReader::addRuntimeFilter);
      checkNotNull(currentRecordReader).setup(mutator);
    } catch (Exception e) {
      ScanOperator.handleExceptionDuringScan(e, functionConfig.getFunctionContext().getReferencedTables(), logger);
    } finally {
      stats.stopSetup();
    }
    stats.addLongStat(ScanOperator.Metric.NUM_READERS, 1);
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (currentRow != 0) {
      return 0;
    }
    if (currentRecordReader == null) {
      return 0;
    }
    currentRecordReader.allocate(fieldVectorMap);
    int records;
    while ((records = currentRecordReader.next()) == 0) {
      //insert boost buffer manager here if the spilt is to be boosted.
      currentRecordReader.close();
      currentRecordReader = null;
      if (!getRecordReaderIterator().hasNext()) {
        return 0;
      }

      setupNextReader();
      currentRecordReader.allocate(fieldVectorMap);
    }
    outgoing.setRecordCount(records);
    outgoing.setAllCount(records);
    return records;
  }

  @Override
  public void closeRow() throws Exception {
    if (currentRow == 0) {
      AutoCloseables.close(currentRecordReader);
      currentRecordReader = null;
    }
  }

  /*
   * create a recordReaderIterator with empty spltis
   */
  protected abstract RecordReaderIterator createRecordReaderIterator();

  protected abstract RecordReaderIterator getRecordReaderIterator();

  /*
   * add splits to the underlying recordreaderiterator
   */
  protected abstract void addSplits(List<SplitAndPartitionInfo> splits);

  public boolean hasBufferedRemaining() {
    produceFromBufferedSplits = true;
    getRecordReaderIterator().produceFromBuffered(true);
    return getRecordReaderIterator().hasNext();
  }

  protected void setIcebergColumnIds(byte[] extendedProperty) { }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    final String senderInfo = String.format("Frag %d, OpId %d", message.getSendingMajorFragmentId(), message.getSendingOperatorId());
    if (message.getBuffers()==null || message.getBuffers().length!=1) {
      logger.warn("Empty runtime filter received from {}", senderInfo);
      return;
    }

    try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable()) {
      // Operator ID int is transformed as follows - (fragmentId << 16) + opId;
      logger.info("Filter received from {} minor fragment {} into op {}", senderInfo, message.getSendingMinorFragmentId(),
              props.getOperatorId());
      // scan operator handles the OOB message that it gets from the join operator
      final ExecProtos.RuntimeFilter protoFilter = message.getPayload(ExecProtos.RuntimeFilter.parser());
      final ArrowBuf msgBuf = message.getIfSingleBuffer().get();
      final RuntimeFilter filter = RuntimeFilter.getInstance(protoFilter, msgBuf, senderInfo, context.getStats());
      rollbackCloseable.add(filter);

      boolean isAlreadyPresent = this.runtimeFilters.stream()
              .anyMatch(r -> r.getSenderInfo().equals(filter.getSenderInfo()) && r.isOnSameColumns(filter));
      if (isAlreadyPresent) {
        logger.debug("Skipping enforcement because filter is already present {}", filter);
      } else {
        logger.debug("Adding filter to the record readers {}", filter);
        getRecordReaderIterator().addRuntimeFilter(filter);
        this.runtimeFilters.add(filter);
        Optional.ofNullable(currentRecordReader).ifPresent(c -> c.addRuntimeFilter(filter));
        context.getStats().addLongStat(ScanOperator.Metric.NUM_RUNTIME_FILTERS, 1);
        rollbackCloseable.commit();
      }
    } catch (Exception e) {
      logger.warn("Error while merging runtime filter piece from " + message.getSendingMajorFragmentId() + ":"
              + message.getSendingMinorFragmentId(), e);
    }
  }

  @VisibleForTesting
  List<RuntimeFilter> getRuntimeFilters() {
    return this.runtimeFilters;
  }



  @Override
  public void close() throws Exception {
    final List<AutoCloseable> closeables = new ArrayList<>(runtimeFilters.size() + 3);
    closeables.add(super::close);
    closeables.add(currentRecordReader);
    closeables.addAll(runtimeFilters);
    AutoCloseables.close(closeables);
    currentRecordReader = null;
    //close boost buffer manager here.
    this.context.getStats().setReadIOStats();
  }
}
