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
package com.dremio.sabot.op.windowframe;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.physical.config.WindowPOP;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.exec.context.OperatorContext;
import java.util.List;
import javax.inject.Named;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.ValueVector;

/**
 * WindowFramer implementation that doesn't support the FRAME clause (will assume the default
 * frame). <br>
 * According to the SQL standard, LEAD, LAG, ROW_NUMBER, NTILE and all ranking functions don't
 * support the FRAME clause. This class will handle such functions.
 */
public abstract class NoFrameSupportTemplate implements WindowFramer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(NoFrameSupportTemplate.class);

  private FunctionContext context;
  private VectorAccessible container;
  private VectorContainer internal;
  private boolean lagCopiedToInternal;
  private List<VectorContainer> batches;
  private int outputCount; // number of rows in currently/last processed batch

  private VectorAccessible current;

  // true when at least one window function needs to process all batches of a partition before
  // passing any batch downstream
  private boolean requireFullPartition;

  private Partition partition; // current partition being processed
  private int currentBatchIndex; // index of current batch in batches

  @Override
  public void setup(
      final List<VectorContainer> batches,
      final VectorAccessible container,
      final OperatorContext oContext,
      final boolean requireFullPartition,
      final WindowPOP popConfig,
      FunctionContext context)
      throws SchemaChangeException {
    this.container = container;
    this.batches = batches;
    this.context = context;

    internal = new VectorContainer(oContext.getAllocator());
    allocateInternal();
    lagCopiedToInternal = false;

    outputCount = 0;
    partition = null;

    this.requireFullPartition = requireFullPartition;
  }

  private void allocateInternal() {
    for (VectorWrapper<?> w : container) {
      ValueVector vv = internal.addOrGet(w.getField());
      vv.allocateNew();
    }
  }

  /** processes all rows of the first batch. */
  @Override
  public void doWork(int batchIndex) throws Exception {
    int currentRow = 0;
    currentBatchIndex = batchIndex;
    this.current = batches.get(currentBatchIndex);
    outputCount = current.getRecordCount();

    while (currentRow < outputCount) {
      if (partition != null) {
        assert currentRow == 0 : "pending windows are only expected at the start of the batch";
        // we have a pending window we need to handle from a previous call to doWork()
        logger.trace("we have a pending partition {}", partition);
        if (!requireFullPartition) {
          // we didn't compute the whole partition length in the previous partition, we need to
          // update the length now
          updatePartitionSize(partition, currentRow);
        }
      } else {
        newPartition(current, currentRow);
      }
      currentRow = processPartition(currentRow);
      if (partition.isDone()) {
        cleanPartition();
      }
    }
  }

  private void newPartition(final VectorAccessible current, final int currentRow)
      throws SchemaChangeException {
    partition = new Partition();
    updatePartitionSize(partition, currentRow);
    setupPartition(context, current, container);
  }

  private void cleanPartition() {
    partition = null;
    resetValues();
    for (VectorWrapper<?> vw : internal) {
      ValueVector vv = vw.getValueVector();
      if ((vv instanceof BaseValueVector)) {
        ArrowBuf validityBuffer = vv.getValidityBuffer();
        validityBuffer.setZero(0, validityBuffer.capacity());
      }
    }
    lagCopiedToInternal = false;
  }

  /**
   * process all rows (computes and writes function values) of current batch that are part of
   * current partition.
   *
   * @param currentRow first unprocessed row
   * @return index of next unprocessed row
   * @throws Exception if it can't write into the container
   */
  private int processPartition(final int currentRow) throws Exception {
    logger.trace(
        "process partition {}, currentRow: {}, outputCount: {}",
        partition,
        currentRow,
        outputCount);
    setupCopyNext(current, container);
    copyPrevFromInternal();
    // copy remaining from current
    setupCopyPrev(current, container);
    int row = currentRow;
    partition.setFirstRowInPartition(currentRow);

    // process all rows except the last one of the batch/partition
    while (row < outputCount && !partition.isDone()) {
      partition.setCurrentRowInPartition(row);
      copyPrev(row, row, partition);
      processRow(row);
      if (!partition.isDone()) {
        copyNext(row, row, partition);
      }
      row++;
    }

    // if we didn't reach the end of partition yet
    if (!partition.isDone() && batches.size() - currentBatchIndex > 1) {
      // copy next value onto the current one
      partition.setCurrentRowInPartition(row);
      setupCopyFromFirst(batches.get(currentBatchIndex + 1), container);
      copyFromFirst(0, row, partition);
      copyPrevToInternal(current, row);
    }

    return row;
  }

  private void copyPrevToInternal(VectorAccessible current, int row) {
    logger.trace("copying {} into internal", row);
    setupCopyToFirst(current, internal);
    copyToFirst(row, 0, partition);
    lagCopiedToInternal = true;
  }

  private void copyPrevFromInternal() {
    if (lagCopiedToInternal) {
      setupCopyFromInternal(internal, container);
      copyFromInternal(0, 0);
      lagCopiedToInternal = false;
    }
  }

  private void processRow(final int row) throws Exception {
    if (partition.isFrameDone()) {
      // because all peer rows share the same frame, we only need to compute and aggregate the frame
      // once
      final long peers = countPeers(row);
      partition.newFrame(peers);
    }
    outputRow(row, partition);
    partition.rowAggregated();
  }

  /**
   * updates partition's length after computing the number of rows for the current the partition
   * starting at the specified row of the first batch. If !requiresFullPartition, this method will
   * only count the rows in the current batch
   */
  private void updatePartitionSize(final Partition partition, final int start) {
    logger.trace("compute partition size starting from {} on {} batches", start, batches.size());

    long length = 0;
    boolean lastBatch = false;
    int row = start;

    // count all rows that are in the same partition of start
    // keep increasing length until we find first row of next partition or we reach the very last
    // batch
    int batchIndex = 0;
    outer:
    for (VectorAccessible batch : batches) {
      if (batchIndex++ < currentBatchIndex) {
        continue;
      }
      final int recordCount = batch.getRecordCount();

      // check first container from start row, and subsequent containers from first row
      for (; row < recordCount; row++, length++) {
        if (!isSamePartition(start, current, row, batch)) {
          break outer;
        }
      }

      if (!requireFullPartition) {
        // we are only interested in the first batch's records
        break;
      }

      row = 0;
    }

    if (!requireFullPartition) {
      // this is the last batch of current partition if
      lastBatch =
          row < outputCount // partition ends before the end of the batch
              || batches.size() - currentBatchIndex == 1 // it's the last available batch
              || !isSamePartition(
                  start,
                  current,
                  0,
                  batches.get(currentBatchIndex + 1)); // next batch contains a different partition
    }

    partition.updateLength(length, !(requireFullPartition || lastBatch));
  }

  /**
   * count number of peer rows for current row
   *
   * @param start starting row of the current frame
   * @return num peer rows for current row
   * @throws SchemaChangeException
   */
  private long countPeers(final int start) throws SchemaChangeException {
    long length = 0;

    // a single frame can include rows from multiple batches
    // start processing first batch and, if necessary, move to next batches
    int batchIndex = 0;
    for (VectorAccessible batch : batches) {
      if (batchIndex++ < currentBatchIndex) {
        continue;
      }
      final int recordCount = batch.getRecordCount();

      // for every remaining row in the partition, count it if it's a peer row
      for (int row = (batch == current) ? start : 0; row < recordCount; row++, length++) {
        if (!isPeer(start, current, row, batch)) {
          break;
        }
      }
    }

    return length;
  }

  @Override
  public int getOutputCount() {
    return outputCount;
  }

  // we need this abstract method for code generation
  @Override
  public void close() {
    logger.trace("clearing internal");
    internal.clear();
  }

  /**
   * called once for each row after we evaluate all peer rows. Used to write a value in the row
   *
   * @param outIndex index of row
   * @param partition object used by "computed" window functions
   */
  public abstract void outputRow(
      @Named("outIndex") int outIndex, @Named("partition") Partition partition);

  /**
   * Called once per partition, before processing the partition. Used to setup read/write vectors
   *
   * @param incoming batch we will read from
   * @param outgoing batch we will be writing to
   * @throws SchemaChangeException
   */
  public abstract void setupPartition(
      @Named("context") FunctionContext context,
      @Named("incoming") VectorAccessible incoming,
      @Named("outgoing") VectorAccessible outgoing)
      throws SchemaChangeException;

  /**
   * copies value(s) from inIndex row to outIndex row. Mostly used by LEAD. inIndex always points to
   * the row next to outIndex
   *
   * @param inIndex source row of the copy
   * @param outIndex destination row of the copy.
   */
  public abstract void copyNext(
      @Named("inIndex") int inIndex,
      @Named("outIndex") int outIndex,
      @Named("partition") Partition partition);

  public abstract void setupCopyNext(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  public abstract void copyFromFirst(
      @Named("inIndex") int inIndex,
      @Named("outIndex") int outIndex,
      @Named("partition") Partition partition);

  public abstract void setupCopyFromFirst(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  /**
   * copies value(s) from inIndex row to outIndex row. Mostly used by LAG. inIndex always points to
   * the previous row
   *
   * @param inIndex source row of the copy
   * @param outIndex destination row of the copy.
   */
  public abstract void copyPrev(
      @Named("inIndex") int inIndex,
      @Named("outIndex") int outIndex,
      @Named("partition") Partition partition);

  public abstract void setupCopyPrev(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  public abstract void copyToFirst(
      @Named("inIndex") int inIndex,
      @Named("outIndex") int outIndex,
      @Named("partition") Partition partition);

  public abstract void setupCopyToFirst(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  public abstract void copyFromInternal(
      @Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

  public abstract void setupCopyFromInternal(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  /** reset all window functions */
  public abstract boolean resetValues();

  /**
   * compares two rows from different batches (can be the same), if they have the same value for the
   * partition by expression
   *
   * @param b1Index index of first row
   * @param b1 batch for first row
   * @param b2Index index of second row
   * @param b2 batch for second row
   * @return true if the rows are in the same partition
   */
  @Override
  public abstract boolean isSamePartition(
      @Named("b1Index") int b1Index,
      @Named("b1") VectorAccessible b1,
      @Named("b2Index") int b2Index,
      @Named("b2") VectorAccessible b2);

  /**
   * compares two rows from different batches (can be the same), if they have the same value for the
   * order by expression
   *
   * @param b1Index index of first row
   * @param b1 batch for first row
   * @param b2Index index of second row
   * @param b2 batch for second row
   * @return true if the rows are in the same partition
   */
  @Override
  public abstract boolean isPeer(
      @Named("b1Index") int b1Index,
      @Named("b1") VectorAccessible b1,
      @Named("b2Index") int b2Index,
      @Named("b2") VectorAccessible b2);
}
