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
import com.dremio.exec.physical.config.WindowPOP.Bound;
import com.dremio.exec.physical.config.WindowPOP.BoundType;
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
import org.apache.commons.lang3.tuple.Pair;

/**
 * WindowFramer implementation that supports the FRAME clause. <br>
 * According to the SQL specification, FIRST_VALUE, LAST_VALUE and all aggregate functions support
 * the FRAME clause. This class will handle such functions even if the FRAME clause is not present.
 */
public abstract class FrameSupportTemplate implements WindowFramer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FrameSupportTemplate.class);

  protected FunctionContext context;
  private VectorAccessible container;
  private VectorContainer internal;
  private List<VectorContainer> batches;
  private int outputCount; // number of rows in currently/last processed batch

  private VectorContainer current;

  private int frameLastRow;

  // true when at least one window function needs to process all batches of a partition before
  // passing any batch downstream
  private boolean requireFullPartition;

  private long remainingRows; // num unprocessed rows in current partition
  private long
      remainingPeersFull; // num unprocessed peer rows in full frame (for unbounded following will
  // be full partition)
  private long remainingPeers; // num unprocessed peer rows in current frame
  private boolean
      partialPartition; // true if we remainingRows only account for the current batch and more
  // batches are expected for the current partition

  private WindowPOP popConfig;
  private int currentBatchIndex;

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

    outputCount = 0;

    this.requireFullPartition = requireFullPartition;
    this.popConfig = popConfig;
  }

  private void allocateInternal() {
    for (VectorWrapper<?> w : container) {
      ValueVector vv = internal.addOrGet(w.getField());
      vv.allocateNew();
    }
  }

  private boolean isPartitionDone() {
    return !partialPartition && remainingRows == 0;
  }

  /** processes all rows of the first batch. */
  @Override
  public void doWork(int batchIndex) throws Exception {
    currentBatchIndex = batchIndex;
    int currentRow = 0;
    this.current = batches.get(currentBatchIndex);
    outputCount = current.getRecordCount();
    setupSaveFirstValue(current, internal);

    while (currentRow < outputCount) {
      if (!isPartitionDone()) {
        // we have a pending partition we need to handle from a previous call to doWork()
        assert currentRow == 0 : "pending partitions are only expected at the start of the batch";
        logger.trace("we have a pending partition {}", remainingRows);

        if (!requireFullPartition) {
          // we didn't compute the whole partition length in the previous partition, we need to
          // update the length now
          updatePartitionSize(currentRow);
        }
      } else {
        newPartition(current, currentRow);
      }

      currentRow = processPartition(currentRow);
      if (isPartitionDone()) {
        reset();
      }
    }
  }

  private void newPartition(final VectorAccessible current, final int currentRow)
      throws SchemaChangeException {
    remainingRows = 0;
    remainingPeersFull = 0;
    remainingPeers = 0;
    updatePartitionSize(currentRow);

    setupPartition(context, current, container);
    saveFirstValue(currentRow, 0);
  }

  private void reset() {
    resetValues();
    for (VectorWrapper<?> vw : internal) {
      ValueVector vv = vw.getValueVector();
      if ((vv instanceof BaseValueVector)) {
        ArrowBuf validityBuffer = vv.getValidityBuffer();
        validityBuffer.setZero(0, validityBuffer.capacity());
      }
    }
  }

  /**
   * process all rows (computes and writes aggregation values) of current batch that are part of
   * current partition.
   *
   * @param currentRow first unprocessed row
   * @return index of next unprocessed row
   * @throws Exception if it can't write into the container
   */
  private int processPartition(final int currentRow) throws Exception {
    logger.trace(
        "{} rows remaining to process, currentRow: {}, outputCount: {}",
        remainingRows,
        currentRow,
        outputCount);

    setupWriteFirstValue(internal, container);

    if (popConfig.isFrameUnitsRows()) {
      return processROWS(currentRow);
    } else {
      return processRANGE(currentRow);
    }
  }

  private int processROWS(int row) {
    setupReadLastValue(current, container);
    setupEvaluatePeer(current, container);
    Bound lowerBound = popConfig.getLowerBound();
    Bound upperBound = popConfig.getUpperBound();
    if (lowerBound.isUnbounded() && upperBound.getType().equals(BoundType.CURRENT_ROW)) {
      return processROWSUnboundToCurrent(row);
    } else if (lowerBound.getType().equals(BoundType.CURRENT_ROW) && upperBound.isUnbounded()) {
      return processROWSCurrentToUnbound(row);
    } else {
      return processROWSFromTo(row);
    }
  }

  private int processROWSFromTo(int row) {
    int firstRow = row;
    int lastRow = row + (int) remainingRows - 1;
    while (row < outputCount && !isPartitionDone()) {
      logger.trace("aggregating row {}", row);
      int startOffset = getStartOffsetForROW(row);
      int endOffset = getEndOffsetForROW(row, lastRow);
      // this for case when lowerBound is unbounded/N preceding - so it's possible that we this
      // frame contain rows from previous batch;
      if (startOffset < firstRow && currentBatchIndex > 0) {
        processROWFromPrevBatch(row, startOffset, Math.min(endOffset, 0), currentBatchIndex);
      }
      // process row in current batch. startOffset should be between firstRow and lastRow
      // and less than outputCount(count of rows in current batch)
      // or startOffset could be less firstRow, but endOffset should be more than firstRow
      // (this is the case when frame contain rows from previous and current batches);
      if ((startOffset >= firstRow && startOffset <= lastRow && startOffset < outputCount)
          || (startOffset < firstRow && endOffset >= firstRow)) {
        processROWFromCurrentBatch(row, startOffset, firstRow, lastRow, endOffset);
      }
      // this for case when upperBound is unbounded/N following - so it's possible that we this
      // frame contain rows from next batch;
      if (endOffset >= outputCount
          && lastRow >= outputCount
          && moreThanNBatchLeft(currentBatchIndex, 1)) {
        processROWFromNextBatch(
            row,
            popConfig.getLowerBound().isUnbounded() ? 0 : Math.max(0, startOffset - outputCount),
            popConfig.getUpperBound().isUnbounded()
                ? lastRow - outputCount
                : endOffset - outputCount,
            currentBatchIndex);
      }
      outputRow(row);
      remainingRows--;
      row++;
      reset();
    }
    return row;
  }

  // process rows from beginning of partition to current row
  private int processROWSUnboundToCurrent(int row) {
    while (row < outputCount && !isPartitionDone()) {
      logger.trace("aggregating row {}", row);
      writeFirstValue(0, row);
      evaluatePeer(row);
      outputRow(row);
      writeLastValue(row, row);
      remainingRows--;
      row++;
    }
    return row;
  }

  // process rows from current row to end of partition
  private int processROWSCurrentToUnbound(int row) {
    int endOffset = row + (int) remainingRows - 1;
    int currentEndOffset = Math.min(endOffset, outputCount - 1);
    // it's possible that frame could contain rows from next batch
    endOffset = getEndOffsetForNextBatch(endOffset, outputCount, currentBatchIndex);
    int startOffset = currentEndOffset;
    setupEvaluatePeer(current, container);
    while (row < outputCount && !isPartitionDone()) {
      logger.trace("aggregating row {}", row);
      saveFirstValue(row, row);
      writeFirstValue(row, row);
      evaluatePeer(startOffset);
      outputRow(startOffset);
      writeLastValue(endOffset, row);
      startOffset--;
      remainingRows--;
      row++;
    }
    reset();
    return row;
  }

  private int getEndOffsetForNextBatch(int endOffset, int outputCount, int currentBatchIndex) {
    if (endOffset > outputCount) {
      endOffset = endOffset - outputCount;
      VectorContainer next = getNextBatch(currentBatchIndex);
      int currentEndOffset = endOffset;
      if (next.getRecordCount() < endOffset) {
        endOffset =
            getEndOffsetForNextBatch(endOffset, next.getRecordCount(), currentBatchIndex + 1);
        currentEndOffset = outputCount - 1;
      } else {
        // if endOffset is less than outputCount - last value will be here
        setupReadLastValue(next, container);
      }
      setupEvaluatePeer(next, container);
      while (currentEndOffset >= 0) {
        evaluatePeer(currentEndOffset);
        currentEndOffset--;
      }
    }
    return endOffset;
  }

  private void processROWFromCurrentBatch(
      int row, int startOffset, int firstRow, int lastRow, int endOffset) {
    setupEvaluatePeer(current, container);
    setupReadLastValue(current, container);
    setupSaveFirstValue(current, internal);
    int currentOffset = Math.max(startOffset, firstRow);
    int endOffsetForCurrentBatch =
        popConfig.getUpperBound().isUnbounded()
            ? Math.min(lastRow, outputCount)
            : Math.min(endOffset, lastRow);
    // check if we need to save the first value because it is the first row of the partition
    if (startOffset >= 0 || firstRow > 0 || currentBatchIndex == 0) {
      saveFirstValue(currentOffset, row);
      writeFirstValue(row, row);
    }
    while (currentOffset <= endOffsetForCurrentBatch) {
      evaluatePeer(currentOffset);
      currentOffset++;
    }
    writeLastValue(endOffsetForCurrentBatch, row);
  }

  // if end is unbounded, endOffset will be last row in partition. If end is preceding, endOffset
  // will be before current row.
  // If end is following, endOffset will be after current row.
  private int getEndOffsetForROW(int row, int lastRow) {
    Bound upperBound = popConfig.getUpperBound();
    if (upperBound.isUnbounded()) {
      return lastRow;
    }
    switch (upperBound.getType()) {
      case PRECEDING:
        return row - upperBound.getOffset();
      case FOLLOWING:
        return row + upperBound.getOffset();
      case CURRENT_ROW:
        return row;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported bound type %s", upperBound.getType()));
    }
  }

  // if start is unbounded, startOffset will be first row in partition
  private int getStartOffsetForROW(int row) {
    Bound lowerBound = popConfig.getLowerBound();
    if (lowerBound.isUnbounded()) {
      return lowerBound.getOffset();
    }
    switch (lowerBound.getType()) {
      case PRECEDING:
        return row - lowerBound.getOffset();
      case FOLLOWING:
        return row + lowerBound.getOffset();
      case CURRENT_ROW:
        return row;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported bound type %s", lowerBound.getType()));
    }
  }

  private void processROWFromPrevBatch(
      int row, int startOffset, int endOffset, int currentBatchIndex) {
    VectorContainer previous = getPrevBatch(currentBatchIndex);
    // don't do anything if previous batch is closed
    if (previous.isNewSchema()) {
      return;
    }
    int previousBatchLastRow = getLastRowIndex(previous.getRecordCount());
    int targetStartOffset = previous.getRecordCount() + startOffset;
    // if endOffset is above 0, that mean that we need to process rows from targetStartOffset
    // to last row in batch
    int targetEndOffset = Math.min(previous.getRecordCount() + endOffset, previousBatchLastRow);
    // it's possible that frame could contain rows from -2 batches
    boolean hasPrevBatch = prevNBatchesExist(currentBatchIndex, 2);
    // if targetStartOffset is less than 0, that we need to check previous batch (if it exists)
    // and targetStartOffset will be 0 (first row in previous batch)
    if (popConfig.getLowerBound().isUnbounded() || targetStartOffset < 0) {
      if (hasPrevBatch) {
        processROWFromPrevBatch(
            row, targetStartOffset, Math.min(targetEndOffset, 0), currentBatchIndex - 1);
      }
      targetStartOffset = 0;
    }
    // if targetEndOffset is less than 0, that mean this frame didn't contain rows from current
    // batch
    if (targetEndOffset < 0 || !isSamePartition(row, current, targetEndOffset, previous)) {
      return;
    }
    int offset = targetEndOffset;
    setupEvaluatePeer(previous, container);
    // check if we need to save the last value because if end is not preceding or unbounded in this
    // case
    // last value can't be in this partition
    Bound end = popConfig.getUpperBound();
    if (end.getType().equals(BoundType.PRECEDING) && endOffset < 0 && !end.isUnbounded()) {
      setupReadLastValue(previous, container);
      writeLastValue(targetEndOffset, row);
    }
    // process rows that are in the same partition
    for (int i = targetEndOffset; i >= targetStartOffset; i--) {
      if (!isSamePartition(row, current, i, previous)) {
        break;
      }
      evaluatePeer(i);
      offset = i;
    }
    // process first value if it's in this partition
    if (previous.getRecordCount() + startOffset >= 0 || !hasPrevBatch) {
      setupSaveFirstValue(previous, internal);
      saveFirstValue(offset, row);
      writeFirstValue(row, row);
    }
  }

  private void processROWFromNextBatch(
      int row, int startOffset, int endOffset, int currentBatchIndex) {
    VectorContainer next = getNextBatch(currentBatchIndex);
    int nextRecordCount = next.getRecordCount();
    int lastRow = getLastRowIndex(nextRecordCount);
    int targetStartOffset = startOffset;
    int targetEndOffset = endOffset;

    // it's possible that frame could contain rows from several batches
    if (endOffset > lastRow) {
      if (moreThanNBatchLeft(currentBatchIndex, 2)) {
        // it is possible that frame could contain rows from next batch (in this case startOffset =
        // 0),
        // or contains rows only from next batch (startOffset = targetStartOffset - lastRow).
        processROWFromNextBatch(
            row,
            Math.max(targetStartOffset - nextRecordCount, 0),
            endOffset - nextRecordCount,
            currentBatchIndex + 1);
      }
      // targetEndOffset in this case will last row in this batch
      targetEndOffset = lastRow;
    }
    // if targetStartOffset is more than last row, we don't need to process anything here
    if (startOffset > lastRow || !isSamePartition(row, current, startOffset, next)) {
      return;
    }
    Bound start = popConfig.getLowerBound();
    // if row is last row of this batch, we need to save the first value from targetStartOffset
    if (start.getType().equals(BoundType.FOLLOWING)
        && (row == outputCount - 1 || row + start.getOffset() > outputCount - 1)) {
      setupSaveFirstValue(next, internal);
      saveFirstValue(targetStartOffset, row);
      writeFirstValue(row, row);
    }
    // process rows that are in the same partition
    for (int i = targetStartOffset; i <= targetEndOffset; i++) {
      if (!isSamePartition(row, current, i, next)) {
        break;
      }
      setupEvaluatePeer(next, container);
      evaluatePeer(i);
      targetStartOffset = i;
    }
    // process last value if it's in this partition
    if (targetStartOffset <= endOffset && endOffset <= lastRow) {
      setupReadLastValue(next, container);
      writeLastValue(targetStartOffset, row);
    }
  }

  private int processRANGE(int row) throws Exception {
    while (row < outputCount && !isPartitionDone()) {
      if (remainingPeersFull == 0 || shouldUpdateForFollowing()) {
        // because all peer rows share the same frame, we only need to compute and aggregate the
        // frame once
        if (popConfig.getLowerBound().getType().equals(BoundType.CURRENT_ROW)) {
          reset();
          saveFirstValue(row, 0);
        }
        Pair<Long, Long> peers = aggregatePeersForNextBatch(row, currentBatchIndex);
        remainingPeersFull = peers.getLeft();
        remainingPeers = peers.getRight();
      }
      writeFirstValue(0, row);
      outputRow(row);
      writeLastValue(frameLastRow, row);

      remainingRows--;
      remainingPeersFull--;
      remainingPeers--;
      row++;
    }
    return row;
  }

  // if upper bound is unbounded following, should be processed current frame + all following
  // frames.
  private boolean shouldUpdateForFollowing() {
    return popConfig.getLowerBound().getType().equals(BoundType.CURRENT_ROW)
        && popConfig.getUpperBound().isUnbounded()
        && remainingPeers == 0;
  }

  /**
   * updates partition's length after computing the number of rows for the current the partition
   * starting at the specified row of the first batch. If !requiresFullPartition, this method will
   * only count the rows in the current batch
   */
  private void updatePartitionSize(final int start) {
    logger.trace("compute partition size starting from {} on {} batches", start, batches.size());

    long length = 0;
    int row = start;

    // count all rows that are in the same partition of start
    // keep increasing length until we find first row of next partition or we reach the very last
    // batch

    outer:
    for (int i = currentBatchIndex; i < batches.size(); i++) {
      final VectorAccessible batch = batches.get(i);
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
      boolean lastBatch =
          row < outputCount // partition ends before the end of the batch
              || batches.size() - currentBatchIndex == 1 // it's the last available batch
              || !isSamePartition(
                  start,
                  current,
                  0,
                  batches.get(currentBatchIndex + 1)); // next batch contains a different partition
      partialPartition = !lastBatch;
    } else {
      partialPartition = false;
    }
    remainingRows += length;
  }

  /**
   * aggregates all peer rows of current row
   *
   * @param start starting row of the current frame
   * @return num peer rows for current row
   * @throws SchemaChangeException
   */
  private Pair<Long, Long> aggregatePeersForNextBatch(final int start, int batchIndex)
      throws SchemaChangeException {
    logger.trace("aggregating rows starting from {}", start);

    final boolean unboundedFollowing = popConfig.getUpperBound().isUnbounded();
    VectorAccessible last = current;
    long length = 0;
    long lengthWithSamePeer = 0;

    // a single frame can include rows from multiple batches
    // start processing first batch and, if necessary, move to next batches
    outer:
    for (int i = batchIndex; i < batches.size(); i++) {
      VectorAccessible batch = batches.get(i);
      setupEvaluatePeer(batch, container);
      final int recordCount = batch.getRecordCount();

      // for every remaining row in the partition, count it if it's a peer row
      for (int row = (batch == current) ? start : 0; row < recordCount; row++, length++) {
        if (unboundedFollowing) {
          if (length >= remainingRows) {
            break outer;
          }
          // for unbounded following, we need to process all rows in partition
          if (isPeer(start, current, row, batch)) {
            lengthWithSamePeer++;
          }
        } else {
          if (!isPeer(start, current, row, batch)) {
            break outer;
          }
        }

        evaluatePeer(row);
        last = batch;
        frameLastRow = row;
      }
    }

    setupReadLastValue(last, container);
    return Pair.of(length, lengthWithSamePeer);
  }

  private boolean moreThanNBatchLeft(int currentBatchIndex, int n) {
    return currentBatchIndex < batches.size() - n;
  }

  private boolean prevNBatchesExist(int currentBatchIndex, int n) {
    return currentBatchIndex >= n;
  }

  private VectorContainer getNextBatch(int currentBatchIndex) {
    return batches.get(currentBatchIndex + 1);
  }

  private VectorContainer getPrevBatch(int currentBatchIndex) {
    return batches.get(currentBatchIndex - 1);
  }

  private int getLastRowIndex(int count) {
    return count - 1;
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
   * called once for each peer row of the current frame.
   *
   * @param index of row to aggregate
   */
  public abstract void evaluatePeer(@Named("index") int index);

  public abstract void setupEvaluatePeer(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing)
      throws SchemaChangeException;

  public abstract void setupReadLastValue(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing)
      throws SchemaChangeException;

  public abstract void writeLastValue(@Named("index") int index, @Named("outIndex") int outIndex);

  public abstract void setupSaveFirstValue(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing)
      throws SchemaChangeException;

  public abstract void saveFirstValue(@Named("index") int index, @Named("outIndex") int outIndex);

  public abstract void setupWriteFirstValue(
      @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  public abstract void writeFirstValue(@Named("index") int index, @Named("outIndex") int outIndex);

  /**
   * called once for each row after we evaluate all peer rows. Used to write a value in the row
   *
   * @param outIndex index of row
   */
  public abstract void outputRow(@Named("outIndex") int outIndex);

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
