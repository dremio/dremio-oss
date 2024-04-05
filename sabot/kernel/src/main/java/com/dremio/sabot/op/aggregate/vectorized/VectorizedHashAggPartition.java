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
package com.dremio.sabot.op.aggregate.vectorized;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.ResizeListener;
import com.dremio.sabot.op.common.ht2.SpaceCheckListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.netty.util.internal.PlatformDependent;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.Types;

public class VectorizedHashAggPartition implements SpaceCheckListener, AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VectorizedHashAggPartition.class);
  boolean spilled;
  final LBlockHashTable hashTable;
  final AccumulatorSet accumulator;
  final int blockWidth;
  int records;
  ArrowBuf buffer;
  private VectorizedHashAggDiskPartition spillInfo;
  private String identifier;
  private final boolean decimalV2Enabled;
  private int varFieldLen = 0;
  private int varFieldCount = 0;

  private int[] batchSpaceUsage;
  private int[] batchValueCount;
  private final Stopwatch forceAccumTimer = Stopwatch.createUnstarted();
  private int numForceAccums = 0;

  /*
  When this VectorizedHashAggOperator picks this partition to output, it does it one batch at a time and serially.
  Below member keeps track of current batch index to output.
   */
  private int nextBatchToOutput = 0;

  /**
   * Create a partition. Used by {@link VectorizedHashAggOperator} at setup time to initialize all
   * partitions. The operator allocates all the data structures for a partition and invokes this
   * constructor.
   *
   * @param accumulator accumulator for the partition
   * @param hashTable hashtable for the partition
   * @param blockWidth pivot block width.
   * @param decimalV2Enabled
   */
  public VectorizedHashAggPartition(
      final AccumulatorSet accumulator,
      final LBlockHashTable hashTable,
      final int blockWidth,
      final String identifier,
      final ArrowBuf buffer,
      boolean decimalV2Enabled) {
    Preconditions.checkArgument(
        hashTable != null, "Error: initializing a partition with invalid hash table");
    this.spilled = false;
    this.records = 0;
    this.hashTable = hashTable;
    this.hashTable.registerSpaceCheckListener(this);
    this.accumulator = accumulator;
    this.blockWidth = blockWidth;
    this.spillInfo = null;
    this.identifier = identifier;
    this.buffer = buffer;
    this.decimalV2Enabled = decimalV2Enabled;
    batchSpaceUsage = new int[0];
    batchValueCount = new int[0];
    buffer.getReferenceManager().retain(1);
  }

  /**
   * Get identifier for this partition
   *
   * @return partition identifier
   */
  public String getIdentifier() {
    return identifier;
  }

  @VisibleForTesting
  public ArrowBuf getBuffer() {
    return this.buffer;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(hashTable, accumulator, buffer);
    this.buffer = null;
  }

  /**
   * Check if the partition has been spilled (1 or more times)
   *
   * @return true if partition has been spilled, false if partition has never been spilled.
   */
  public boolean isSpilled() {
    return spilled;
  }

  /**
   * Get the spill info for the partition.
   *
   * @return disk version of partition that has spill related info
   */
  public VectorizedHashAggDiskPartition getSpillInfo() {
    return spillInfo;
  }

  /**
   * Set spill info for the partition.
   *
   * @param spillInfo spill related information
   */
  public void setSpilled(final VectorizedHashAggDiskPartition spillInfo) {
    Preconditions.checkArgument(
        spillInfo != null, "Error: attempted to mark partition as spilled with invalid spill info");
    this.spilled = true;
    this.spillInfo = spillInfo;
  }

  /**
   * Get the size (in bytes) of the data structures inside the partition. This is used by {@link
   * VectorizedHashAggPartitionSpillHandler} when deciding the victim partition to be spilled.
   *
   * @return total size (in bytes) of all the partition structures
   */
  public long getSize() {
    return hashTable.getSizeInBytes() + accumulator.getSizeInBytes();
  }

  /**
   * Downsize the partition's structures. Release all memory associated with partition' structures
   * except for what is needed for holding a single batch of data. We currently use this in two
   * places:
   *
   * <p>(1) after spilling this partition (2) after sending data back from the operator from this
   * partition
   *
   * <p>In both cases we don't want to release all memory associated with the partition since we may
   * not get it back and we need memory for starting the second iteration of the operator to process
   * spilled partitions.
   *
   * @throws Exception
   */
  public void resetToMinimumSize() throws Exception {
    /* hashtable internally resets the corresponding accumulator to minimum size */
    hashTable.resetToMinimumSize();
    setNextBatchToOutput(0);
  }

  /**
   * {@link VectorizedHashAggOperator} uses this function to record metadata about a new record that
   * has been inserted into this {@link LBlockHashTable} of this partition. Each partition maintains
   * an offset buffer to store a 8 byte tuple < 4 byte ordinal, 4 byte record index> for each record
   * inserted into the partition. Note that this buffer is always overwritten (as opposed to zeroed
   * out) for every cycle of consumeData()
   *
   * @param ordinal hash table ordinal of the newly inserted record
   * @param keyIndex absolute index of the record in incoming batch
   */
  public void appendRecord(int ordinal, int keyIndex) {
    long addrNext =
        buffer.memoryAddress()
            + (records * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH);
    PlatformDependent.putInt(addrNext + VectorizedHashAggOperator.HTORDINAL_OFFSET, ordinal);
    PlatformDependent.putInt(addrNext + VectorizedHashAggOperator.KEYINDEX_OFFSET, keyIndex);
    ++records;
  }

  /**
   * The number of records in the partition are reset to 0 when we finish accumulation for that
   * partition
   *
   * <p>-- this could be done midway when we decide to spill this partition in
   * accumulateBeforeSpill() -- this will also be done at the end of consume cycle when we
   * accumulate for each partition in accumulateForAllPartitions()
   */
  public void resetRecords() {
    records = 0;
    /* Reset the batchSpaceUsage as well */
    resetBatchSpaceAndCount();
  }

  /**
   * Return the number of records in this partition
   *
   * @return records
   */
  public int getRecords() {
    return records;
  }

  /** Set the partition as not spilled. */
  void transitionToMemoryOnlyPartition() {
    Preconditions.checkArgument(
        spilled && spillInfo != null, "Error: expecting a spilled partition");
    this.spilled = false;
    this.spillInfo = null;
    this.records = 0;
  }

  /**
   * Update accumulator info for this partition. This is used by {@link VectorizedHashAggOperator}
   * when it starts the processing of spilled partitions. We need to update partition's accumulator
   * in two ways:
   *
   * <p>(1) Set the input vector for each accumulator. The new input vector is the one we
   * deserialized from spilled batch. (2) For count, count1, sum, $sum0 convert the accumulator
   * type. For example an pre-spill IntSumAccumulator will become BigIntSumAccumulator for
   * post-spill processing.
   *
   * @param accumulatorTypes
   * @param accumulatorVectors
   */
  public void updateAccumulator(
      final byte[] accumulatorTypes,
      final FieldVector[] accumulatorVectors,
      final BufferAllocator computationVectorAllocator) {
    Preconditions.checkArgument(
        accumulatorTypes.length == accumulatorVectors.length,
        "Error: detected inconsistent number of accumulators");
    final Accumulator[] partitionAccumulators = this.accumulator.getChildren();
    for (int i = 0; i < accumulatorTypes.length; i++) {
      final Accumulator partitionAccumulator = partitionAccumulators[i];
      final FieldVector deserializedAccumulator = accumulatorVectors[i];
      final byte accumulatorType = accumulatorTypes[i];
      if (accumulatorType == AccumulatorBuilder.AccumulatorType.COUNT1.ordinal()) {
        partitionAccumulators[i] =
            new SumAccumulators.BigIntSumAccumulator(
                (CountOneAccumulator) partitionAccumulator,
                deserializedAccumulator,
                hashTable.getActualValuesPerBatch(),
                computationVectorAllocator);
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.COUNT.ordinal()) {
        partitionAccumulators[i] =
            new SumAccumulators.BigIntSumAccumulator(
                (CountColumnAccumulator) partitionAccumulator,
                deserializedAccumulator,
                hashTable.getActualValuesPerBatch(),
                computationVectorAllocator);
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.SUM.ordinal()) {
        updateSumAccumulator(
            deserializedAccumulator, partitionAccumulators,
            i, computationVectorAllocator);
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.SUM0.ordinal()) {
        updateSumZeroAccumulator(
            deserializedAccumulator, partitionAccumulators,
            i, computationVectorAllocator);
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.HLL.ordinal()) {
        /*
         * HLL NDV results are serialized and saved in variable width vector. In order to
         * merge them across spilled batches, need to unionize them.
         */
        partitionAccumulators[i] =
            new NdvAccumulators.NdvUnionAccumulators(
                (BaseNdvAccumulator) partitionAccumulator,
                deserializedAccumulator,
                hashTable.getActualValuesPerBatch(),
                computationVectorAllocator);
        try {
          /* This never fail */
          partitionAccumulator.close();
        } catch (Exception e) {
          logger.error("An unexpected error was thrown while closing partitionAccumulator", e);
          e.printStackTrace();
        }
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.HLL_MERGE.ordinal()) {
        partitionAccumulators[i].setInput(deserializedAccumulator);
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.LISTAGG.ordinal()
          || accumulatorType == AccumulatorBuilder.AccumulatorType.LOCAL_LISTAGG.ordinal()) {
        /*
         * LISTAGG results are saved as ListVector on top of BaseVariableWidthVector. In order to
         * merge them across spilled batches, need to call LISTAGG_MERGE.
         */
        partitionAccumulators[i] =
            new ListAggMergeAccumulator(
                (ListAggAccumulator) partitionAccumulator,
                deserializedAccumulator,
                hashTable.getActualValuesPerBatch(),
                computationVectorAllocator,
                accumulatorType == AccumulatorBuilder.AccumulatorType.LOCAL_LISTAGG.ordinal());
        try {
          /* This never fail */
          partitionAccumulator.close();
        } catch (Exception e) {
          logger.error("An unexpected error was thrown while closing partitionAccumulator", e);
          e.printStackTrace();
        }
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.LISTAGG_MERGE.ordinal()) {
        partitionAccumulators[i].setInput(deserializedAccumulator);
      } else {
        /* handle MIN, MAX */
        Preconditions.checkArgument(
            accumulatorType == AccumulatorBuilder.AccumulatorType.MAX.ordinal()
                || accumulatorType == AccumulatorBuilder.AccumulatorType.MIN.ordinal(),
            "Error: unexpected type of accumulator. Expecting min or max");
        updateMinMaxAccumulator(
            deserializedAccumulator, partitionAccumulators,
            i, computationVectorAllocator);
      }
    }
    /*
     * accumulators in accmulator.children might have updated. Recalculate the actual
     * fixedLen and varLen accumulators list.
     */
    accumulator.updateVarlenAndFixedAccumusLst();
  }

  private void updateMinMaxAccumulator(
      final FieldVector deserializedAccumulator,
      final Accumulator[] partitionAccumulators,
      final int index,
      final BufferAllocator computationVectorAllocator) {

    /* We only need to handle DECIMAL in case decimal v2 is disabled.
     * For example, Int(Min/Max)Accumulator remains Int(Min/Max)Accumulator, Float(Min/Max)Accumulator
     * remains Float(Min/Max)Accumulator and similarly for Double(Min/Max), BigInt(Min/Max) etc.
     * The accumulator vector that was spilled now becomes the new input vector for
     * post-spill processing. However, for decimal min/max we store the output
     * (min and max values) in an accumulator vector of type Double. So for post-spill
     * processing Decimal(Min/Max)Accumulator becomes Double(Min/Max)Accumulator
     */
    final Accumulator partitionAccumulator = partitionAccumulators[index];
    if (!decimalV2Enabled) {
      if (partitionAccumulator instanceof MaxAccumulators.DecimalMaxAccumulator) {
        Preconditions.checkArgument(
            partitionAccumulator.getInput() instanceof DecimalVector,
            "Error: expecting decimal vector");
        partitionAccumulators[index] =
            new MaxAccumulators.DoubleMaxAccumulator(
                (MaxAccumulators.DecimalMaxAccumulator) partitionAccumulator,
                deserializedAccumulator,
                hashTable.getActualValuesPerBatch(),
                computationVectorAllocator);
        return;
      } else if (partitionAccumulator instanceof MinAccumulators.DecimalMinAccumulator) {
        Preconditions.checkArgument(
            partitionAccumulator.getInput() instanceof DecimalVector,
            "Error: expecting decimal vector");
        partitionAccumulators[index] =
            new MinAccumulators.DoubleMinAccumulator(
                (MinAccumulators.DecimalMinAccumulator) partitionAccumulator,
                deserializedAccumulator,
                hashTable.getActualValuesPerBatch(),
                computationVectorAllocator);
        return;
      }
      /* fall through in all other cases */
    }
    partitionAccumulator.setInput(deserializedAccumulator);
  }

  private void updateSumAccumulator(
      final FieldVector deserializedAccumulator,
      final Accumulator[] partitionAccumulators,
      final int index,
      final BufferAllocator computationVectorAllocator) {
    final Accumulator partitionAccumulator = partitionAccumulators[index];
    Types.MinorType type = deserializedAccumulator.getMinorType();
    switch (type) {
      case BIGINT:
        {
          if (partitionAccumulator.getInput() instanceof BigIntVector) {
            /* We started with BigIntSumAccumulator so type doesn't change.
             * Just set the input vector to the accumulator vector read from
             * spilled batch
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumAccumulators.BigIntSumAccumulator,
                "Error: expecting bigint sum accumulator");
            partitionAccumulator.setInput(deserializedAccumulator);
          } else {
            /* We started with IntSumAccumulator that has input vector of type INT
             * and accumulator vector of type BIGINT and the output vector into which
             * we will eventually transfer contents also BIGINT. We spilled the
             * accumulator vector. So IntSumAccumulator now becomes BigIntSumAccumulator
             * for post-spill processing with the accumulator vector read from spilled batch
             * acting as new input vector for BigIntSumAccumulator
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumAccumulators.IntSumAccumulator,
                "Error: expecting int sum accumulator");
            partitionAccumulators[index] =
                new SumAccumulators.BigIntSumAccumulator(
                    (SumAccumulators.IntSumAccumulator) partitionAccumulator,
                    deserializedAccumulator,
                    hashTable.getActualValuesPerBatch(),
                    computationVectorAllocator);
          }

          break;
        }

      case FLOAT8:
        {
          if (partitionAccumulator.getInput() instanceof Float8Vector) {
            /* We started with DoubleSumAccumulator so type doesn't change.
             * Just set the input vector to the accumulator vector read from
             * spilled batch
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumAccumulators.DoubleSumAccumulator,
                "Error: expecting double sum accumulator");
            partitionAccumulator.setInput(deserializedAccumulator);
          } else if (partitionAccumulator.getInput() instanceof Float4Vector) {
            /* We started with FloatSumAccumulator that has input vector of type FLOAT
             * and accumulator vector of type DOUBLE and the output vector into which
             * we will eventually transfer contents also DOUBLE. We spilled the
             * accumulator vector. So FloatSumAccumulator now becomes DoubleSumAccumulator
             * for post-spill processing with the accumulator vector read from spilled batch
             * acting as new input vector for DoubleSumAccumulator
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumAccumulators.FloatSumAccumulator,
                "Error: expecting float sum accumulator");
            partitionAccumulators[index] =
                new SumAccumulators.DoubleSumAccumulator(
                    (SumAccumulators.FloatSumAccumulator) partitionAccumulator,
                    deserializedAccumulator,
                    hashTable.getActualValuesPerBatch(),
                    computationVectorAllocator);
          } else if (partitionAccumulator.getInput() instanceof DecimalVector) {
            /* We started with DecimalSumAccumulator that has input vector of type DECIMAL
             * and accumulator vector of type DOUBLE and the output vector into which
             * we will eventually transfer contents also DOUBLE. We spilled the
             * accumulator vector. So DecimalSumAccumulator now becomes DoubleSumAccumulator
             * for post-spill processing with the accumulator vector read from spilled batch
             * acting as new input vector for DoubleSumAccumulator
             */
            // ensure that if this happens decimal complete is turned off.
            Preconditions.checkArgument(!decimalV2Enabled);
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumAccumulators.DecimalSumAccumulator,
                "Error: expecting decimal sum accumulator");
            partitionAccumulators[index] =
                new SumAccumulators.DoubleSumAccumulator(
                    (SumAccumulators.DecimalSumAccumulator) partitionAccumulator,
                    deserializedAccumulator,
                    hashTable.getActualValuesPerBatch(),
                    computationVectorAllocator);
          }

          break;
        }

      case DECIMAL:
        {
          Preconditions.checkArgument(
              partitionAccumulator instanceof SumAccumulators.DecimalSumAccumulatorV2,
              "Error: expecting decimal sum accumulator");
          partitionAccumulator.setInput(deserializedAccumulator);
          break;
        }

      default:
        {
          /* we should not be here */
          throw new IllegalStateException("Error: incorrect type of deserialized sum accumulator");
        }
    }
  }

  private void updateSumZeroAccumulator(
      final FieldVector deserializedAccumulator,
      final Accumulator[] partitionAccumulators,
      final int index,
      final BufferAllocator computationVectorAllocator) {
    final Accumulator partitionAccumulator = partitionAccumulators[index];
    Types.MinorType type = deserializedAccumulator.getMinorType();
    switch (type) {
      case BIGINT:
        {
          if (partitionAccumulator.getInput() instanceof BigIntVector) {
            /* We started with BigIntSumZeroAccumulator so type doesn't change.
             * Just set the input vector to the accumulator vector read from
             * spilled batch
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumZeroAccumulators.BigIntSumZeroAccumulator,
                "Error: expecting bigint sumzero accumulator");
            partitionAccumulator.setInput(deserializedAccumulator);
          } else {
            /* We started with IntSumZeroAccumulator that has input vector of type INT
             * and accumulator vector of type BIGINT and the output vector into which
             * we will eventually transfer contents also BIGINT. We spilled the
             * accumulator vector. So IntSumZeroAccumulator now becomes BigIntSumZeroAccumulator
             * for post-spill processing with the accumulator vector read from spilled batch
             * acting as new input vector for BigIntSumZeroAccumulator
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumZeroAccumulators.IntSumZeroAccumulator,
                "Error: expecting int sumzero accumulator");
            partitionAccumulators[index] =
                new SumZeroAccumulators.BigIntSumZeroAccumulator(
                    (SumZeroAccumulators.IntSumZeroAccumulator) partitionAccumulator,
                    deserializedAccumulator,
                    hashTable.getActualValuesPerBatch(),
                    computationVectorAllocator);
          }

          break;
        }

      case FLOAT8:
        {
          if (partitionAccumulator.getInput() instanceof Float8Vector) {
            /* We started with DoubleSumZeroAccumulator so type doesn't change.
             * Just set the input vector to the accumulator vector read from
             * spilled batch
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumZeroAccumulators.DoubleSumZeroAccumulator,
                "Error: expecting double sum zero accumulator");
            partitionAccumulator.setInput(deserializedAccumulator);
          } else if (partitionAccumulator.getInput() instanceof Float4Vector) {
            /* We started with FloatSumZeroAccumulator that has input vector of type FLOAT
             * and accumulator vector of type DOUBLE and the output vector into which
             * we will eventually transfer contents also DOUBLE. We spilled the
             * accumulator vector. So FloatZeroAccumulator now becomes DoubleSumZeroAccumulator
             * for post-spill processing with the accumulator vector read from spilled batch
             * acting as new input vector for DoubleSumZeroAccumulator
             */
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumZeroAccumulators.FloatSumZeroAccumulator,
                "Error: expecting float sum zero accumulator");
            partitionAccumulators[index] =
                new SumZeroAccumulators.DoubleSumZeroAccumulator(
                    (SumZeroAccumulators.FloatSumZeroAccumulator) partitionAccumulator,
                    deserializedAccumulator,
                    hashTable.getActualValuesPerBatch(),
                    computationVectorAllocator);
          } else if (partitionAccumulator.getInput() instanceof DecimalVector) {
            /* We started with DecimalSumZeroAccumulator that has input vector of type DECIMAL
             * and accumulator vector of type DOUBLE and the output vector into which
             * we will eventually transfer contents also DOUBLE. We spilled the
             * accumulator vector. So DecimalSumZeroAccumulator now becomes DoubleSumAccumulator
             * for post-spill processing with the accumulator vector read from spilled batch
             * acting as new input vector for DoubleSumAccumulator
             */
            // ensure that if this happens decimal complete is turned off.
            Preconditions.checkArgument(!decimalV2Enabled);
            Preconditions.checkArgument(
                partitionAccumulator instanceof SumZeroAccumulators.DecimalSumZeroAccumulator,
                "Error: expecting decimal sum zero accumulator");
            partitionAccumulators[index] =
                new SumZeroAccumulators.DoubleSumZeroAccumulator(
                    (SumZeroAccumulators.DecimalSumZeroAccumulator) partitionAccumulator,
                    deserializedAccumulator,
                    hashTable.getActualValuesPerBatch(),
                    computationVectorAllocator);
          }
          break;
        }

      case DECIMAL:
        {
          Preconditions.checkArgument(
              partitionAccumulator instanceof SumZeroAccumulators.DecimalSumZeroAccumulatorV2,
              "Error: expecting decimal sum zero accumulator");
          partitionAccumulator.setInput(deserializedAccumulator);
          break;
        }

      default:
        {
          /* we should not be here */
          throw new IllegalStateException(
              "Error: incorrect type of deserialized sumzero accumulator");
        }
    }
  }

  public void updateIdentifier(final String identifier) {
    this.identifier = identifier;
  }

  public int getNumberOfBlocks() {
    return hashTable.getCurrentNumberOfBlocks();
  }

  @VisibleForTesting
  public LBlockHashTable getHashTable() {
    return hashTable;
  }

  @VisibleForTesting
  public AccumulatorSet getAccumulator() {
    return accumulator;
  }

  private void reallocateBatchSpaceAndCount() {
    final int[] newBatches = new int[batchSpaceUsage.length + 64];
    final int[] newBatchCounts = new int[batchValueCount.length + 64];

    for (int i = 0; i < batchSpaceUsage.length; ++i) {
      newBatches[i] = batchSpaceUsage[i];
      newBatchCounts[i] = batchValueCount[i];
    }
    batchSpaceUsage = newBatches;
    batchValueCount = newBatchCounts;
  }

  private int getBatchUsedSpace(int batchIndex) {
    while (batchIndex >= batchSpaceUsage.length) {
      reallocateBatchSpaceAndCount();
    }
    Preconditions.checkArgument(batchIndex < batchSpaceUsage.length);
    return batchSpaceUsage[batchIndex];
  }

  private int getBatchValueCounts(final int batchIndex) {
    if (batchIndex >= batchValueCount.length) {
      reallocateBatchSpaceAndCount();
      Preconditions.checkArgument(batchIndex < batchValueCount.length);
    }
    return batchValueCount[batchIndex];
  }

  public void addToBatchSpaceAndCount(final int batchIndex, final int space, final int count) {
    /* When this function called, batchSpaceUsage and batchValueCount already allocated */
    batchSpaceUsage[batchIndex] += space;
    batchValueCount[batchIndex] += count;
  }

  private void resetBatchSpaceAndCount() {
    Arrays.fill(batchSpaceUsage, 0);
    Arrays.fill(batchValueCount, 0);
  }

  public void setVarFieldLengthAndCount(final int varFieldLen, final int varFieldCount) {
    this.varFieldLen = varFieldLen;
    this.varFieldCount = varFieldCount;
  }

  @Override
  public boolean resizeListenerHasSpace(
      ResizeListener resizeListener, final int batchIndex, final int offsetInBatch, long seed) {
    if (varFieldLen == -1) {
      return true;
    }

    if (resizeListener.hasSpace(
        varFieldLen + getBatchUsedSpace(batchIndex),
        varFieldCount + getBatchValueCounts(batchIndex),
        batchIndex,
        offsetInBatch)) {
      return true;
    }

    /* Not enough space, so try force accumulate all the saved records and recheck it resizer has enough space. */
    forceAccumulateAndCompact(resizeListener, batchIndex, varFieldLen);
    if (resizeListener.hasSpace(
        varFieldLen + getBatchUsedSpace(batchIndex),
        varFieldCount + getBatchValueCounts(batchIndex),
        batchIndex,
        offsetInBatch)) {
      return true;
    }

    /* Still not enough space. Splice the batch and return false for hashtable to retry the insert. */
    int newBatchIndex = hashTable.splice(batchIndex, seed);
    spliceBatchSpaceUsageAndCount(batchIndex, newBatchIndex);
    return false;
  }

  /**
   * Batch 1 was spliced into two. Let batchSpaceUsage and batchValueCount arrays reflect that.
   *
   * @param batchIndex1
   * @param batchIndex2
   */
  private void spliceBatchSpaceUsageAndCount(final int batchIndex1, final int batchIndex2) {
    if (batchIndex1 >= batchSpaceUsage.length || batchIndex2 >= batchSpaceUsage.length) {
      reallocateBatchSpaceAndCount();
    }
    final int originalCount = batchValueCount[batchIndex1];
    final int originalSpace = batchSpaceUsage[batchIndex1];

    batchSpaceUsage[batchIndex2] = originalSpace / 2;
    batchSpaceUsage[batchIndex1] = originalSpace - (originalSpace / 2);

    batchValueCount[batchIndex2] = originalCount / 2;
    batchValueCount[batchIndex1] = originalCount - (originalCount / 2);
  }

  private void forceAccumulateAndCompact(
      ResizeListener resizeListener, final int batchIndex, final int nextRecSize) {
    ++numForceAccums;
    forceAccumTimer.start();

    if (getRecords() > 0) {
      resizeListener.accumulate(
          getBuffer().memoryAddress(),
          getRecords(),
          hashTable.getBitsInChunk(),
          hashTable.getChunkOffsetMask());
      resetRecords();
    }

    resizeListener.compact(batchIndex, nextRecSize);

    forceAccumTimer.stop();
  }

  public int getForceAccumCount() {
    return numForceAccums;
  }

  public long getForceAccumTime(TimeUnit unit) {
    return forceAccumTimer.elapsed(unit);
  }

  public int getNextBatchToOutput() {
    return nextBatchToOutput;
  }

  public void setNextBatchToOutput(int nextBatchToOutput) {
    this.nextBatchToOutput = nextBatchToOutput;
  }
}
