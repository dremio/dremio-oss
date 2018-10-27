/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.DecimalVector;

import com.google.common.annotations.VisibleForTesting;
import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.google.common.base.Preconditions;
import org.apache.arrow.vector.types.Types;

public class VectorizedHashAggPartition implements AutoCloseable {
  boolean spilled;
  final LBlockHashTable hashTable;
  final AccumulatorSet accumulator;
  final int blockWidth;
  int records;
  int recordsSpilled;
  private VectorizedHashAggDiskPartition spillInfo;
  private String identifier;

  /**
   * Create a partition. Used by {@link VectorizedHashAggOperator} at setup
   * time to initialize all partitions. The operator allocates all the data
   * structures for a partition and invokes this constructor.
   *
   * @param accumulator accumulator for the partition
   * @param hashTable hashtable for the partition
   * @param blockWidth pivot block width.
   */
  public VectorizedHashAggPartition(final AccumulatorSet accumulator,
                                    final LBlockHashTable hashTable,
                                    final int blockWidth,
                                    final String identifier) {
    Preconditions.checkArgument(hashTable != null, "Error: initializing a partition with invalid hash table");
    this.spilled = false;
    this.records = 0;
    this.recordsSpilled = 0;
    this.hashTable = hashTable;
    this.accumulator = accumulator;
    this.blockWidth = blockWidth;
    this.spillInfo = null;
    this.identifier = identifier;
  }

  /**
   * Get identifier for this partition
   * @return partition identifier
   */
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(hashTable, accumulator);
  }

  /**
   * Check if the partition has been spilled (1 or more times)
   * @return true if partition has been spilled, false if partition
   *         has never been spilled.
   */
  public boolean isSpilled() { return spilled; }

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
   * @param spillInfo spill related information
   */
  public void setSpilled(final VectorizedHashAggDiskPartition spillInfo) {
    Preconditions.checkArgument(spillInfo != null, "Error: attempted to mark partition as spilled with invalid spill info");
    this.spilled = true;
    this.spillInfo = spillInfo;
  }

  /**
   * Get the size (in bytes) of the data structures inside
   * the partition. This is used by {@link VectorizedHashAggPartitionSpillHandler}
   * when deciding the victim partition to be spilled.
   *
   * @return total size (in bytes) of all the partition structures
   */
  public long getSize() {
    return hashTable.getSizeInBytes() + accumulator.getSizeInBytes();
  }

  /**
   * Downsize the partition's structures.
   * Release all memory associated with partition' structures except
   * for what is needed for holding a single batch of data.
   * We currently use this in two places:
   *
   * (1) after spilling this partition
   * (2) after sending data back from the operator from this partition
   *
   * In both cases we don't want to release all memory associated with
   * the partition since we may not get it back and we need memory
   * for starting the second iteration of the operator to process
   * spilled partitions.
   *
   * @throws Exception
   */
  public void resetToMinimumSize() throws Exception {
    /* hashtable internally resets the corresponding accumulator to minimum size */
    hashTable.resetToMinimumSize();
  }

  public void bumpRecords(final int increment) {
    records += increment;
  }

  public void resetRecords() {
    records = 0;
  }

  public int getRecords() {
    return records;
  }

  public int getRecordsSpilled() {
    return recordsSpilled;
  }

  public void bumpRecordsSpilled(final int records) {
    recordsSpilled += records;
  }

  public void resetSpilledRecords() {
    recordsSpilled = 0;
  }

  /**
   * Set the partition as not spilled.
   */
  public void transitionToMemoryOnlyPartition() {
    Preconditions.checkArgument(spilled && spillInfo != null, "Error: expecting a spilled partition");
    this.spilled = false;
    this.spillInfo = null;
    this.records = 0;
    this.recordsSpilled = 0;
  }

  /**
   * Update accumulator info for this partition. This is used by
   * {@link VectorizedHashAggOperator} when it starts the processing
   * of spilled partitions. We need to update partition's accumulator
   * in two ways:
   *
   * (1) Set the input vector for each accumulator. The new input vector
   * is the one we deserialized from spilled batch.
   * (2) For count, count1, sum, $sum0 convert the accumulator type.
   * For example an pre-spill IntSumAccumulator will become
   * BigIntSumAccumulator for post-spill processing.
   *
   * @param accumulatorTypes
   * @param accumulatorVectors
   */
  public void updateAccumulator(final byte[] accumulatorTypes,
                                final FieldVector[] accumulatorVectors,
                                final BufferAllocator computationVectorAllocator) {
    Preconditions.checkArgument(accumulatorTypes.length == accumulatorVectors.length,
                                "Error: detected inconsistent number of accumulators");
    final Accumulator[] partitionAccumulators = this.accumulator.getChildren();
    for (int i = 0; i < accumulatorTypes.length; i++) {
      final Accumulator partitionAccumulator = partitionAccumulators[i];
      final FieldVector deserializedAccumulator = accumulatorVectors[i];
      final byte accumulatorType = accumulatorTypes[i];
      if (accumulatorType == AccumulatorBuilder.AccumulatorType.COUNT1.ordinal()) {
        /* handle COUNT(1) */
        partitionAccumulators[i] =
          new SumAccumulators.BigIntSumAccumulator((CountOneAccumulator)partitionAccumulator,
                                                   deserializedAccumulator,
                                                   hashTable.getMaxValuesPerBatch(),
                                                   computationVectorAllocator);
      } else if(accumulatorType == AccumulatorBuilder.AccumulatorType.COUNT.ordinal()) {
        /* handle COUNT() */
        partitionAccumulators[i] =
          new SumAccumulators.BigIntSumAccumulator((CountColumnAccumulator)partitionAccumulator,
                                                   deserializedAccumulator,
                                                   hashTable.getMaxValuesPerBatch(),
                                                   computationVectorAllocator);
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.SUM.ordinal()) {
        /* handle SUM */
        updateSumAccumulator(deserializedAccumulator, partitionAccumulators,
                             i, computationVectorAllocator);
      } else if (accumulatorType == AccumulatorBuilder.AccumulatorType.SUM0.ordinal()) {
        /* handle $SUM0 */
        updateSumZeroAccumulator(deserializedAccumulator, partitionAccumulators,
                                 i, computationVectorAllocator);
      }
      else {
        /* handle MIN, MAX */
        Preconditions.checkArgument(
          accumulatorType == AccumulatorBuilder.AccumulatorType.MAX.ordinal() ||
            accumulatorType == AccumulatorBuilder.AccumulatorType.MIN.ordinal(),
          "Error: unexpected type of accumulator. Expecting min or max");
        updateMinMaxAccumulator(deserializedAccumulator, partitionAccumulators,
                                i, computationVectorAllocator);
      }
    }
  }

  private void updateMinMaxAccumulator(final FieldVector deserializedAccumulator,
                                       final Accumulator[] partitionAccumulators,
                                       final int index,
                                       final BufferAllocator computationVectorAllocator) {

    /* We only need to handle DECIMAL.
     * For example, Int(Min/Max)Accumulator remains Int(Min/Max)Accumulator, Float(Min/Max)Accumulator
     * remains Float(Min/Max)Accumulator and similarly for Double(Min/Max), BigInt(Min/Max) etc.
     * The accumulator vector that was spilled now becomes the new input vector for
     * post-spill processing. However, for decimal min/max we store the output
     * (min and max values) in an accumulator vector of type Double. So for post-spill
     * processing Decimal(Min/Max)Accumulator becomes Double(Min/Max)Accumulator
     */
    final Accumulator partitionAccumulator = partitionAccumulators[index];
    if (partitionAccumulator instanceof MaxAccumulators.DecimalMaxAccumulator) {
      Preconditions.checkArgument(partitionAccumulator.getInput() instanceof DecimalVector,
                                  "Error: expecting decimal vector");
      partitionAccumulators[index] =
        new MaxAccumulators.DoubleMaxAccumulator((MaxAccumulators.DecimalMaxAccumulator)partitionAccumulator,
                                                 deserializedAccumulator,
                                                 hashTable.getMaxValuesPerBatch(),
                                                 computationVectorAllocator);
    } else if (partitionAccumulator instanceof MinAccumulators.DecimalMinAccumulator) {
      Preconditions.checkArgument(partitionAccumulator.getInput() instanceof DecimalVector,
                                  "Error: expecting decimal vector");
      partitionAccumulators[index] =
        new MinAccumulators.DoubleMinAccumulator((MinAccumulators.DecimalMinAccumulator)partitionAccumulator,
                                                 deserializedAccumulator,
                                                 hashTable.getMaxValuesPerBatch(),
                                                 computationVectorAllocator);
    } else {
      partitionAccumulator.setInput(deserializedAccumulator);
    }

  }

  private void updateSumAccumulator(final FieldVector deserializedAccumulator,
                                    final Accumulator[] partitionAccumulators,
                                    final int index,
                                    final BufferAllocator computationVectorAllocator) {
    final Accumulator partitionAccumulator = partitionAccumulators[index];
    Types.MinorType type = deserializedAccumulator.getMinorType();
    switch (type) {
      case BIGINT: {
        if (partitionAccumulator.getInput() instanceof BigIntVector) {
          /* We started with BigIntSumAccumulator so type doesn't change.
           * Just set the input vector to the accumulator vector read from
           * spilled batch
           */
          Preconditions.checkArgument(partitionAccumulator instanceof SumAccumulators.BigIntSumAccumulator,
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
          Preconditions.checkArgument(partitionAccumulator instanceof SumAccumulators.IntSumAccumulator,
                                      "Error: expecting int sum accumulator");
          partitionAccumulators[index] =
            new SumAccumulators.BigIntSumAccumulator((SumAccumulators.IntSumAccumulator)partitionAccumulator,
                                                     deserializedAccumulator,
                                                     hashTable.getMaxValuesPerBatch(),
                                                     computationVectorAllocator);
        }

        break;
      }

      case FLOAT8: {
        if (partitionAccumulator.getInput() instanceof Float8Vector) {
          /* We started with DoubleSumAccumulator so type doesn't change.
           * Just set the input vector to the accumulator vector read from
           * spilled batch
           */
          Preconditions.checkArgument(partitionAccumulator instanceof SumAccumulators.DoubleSumAccumulator,
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
          Preconditions.checkArgument(partitionAccumulator instanceof SumAccumulators.FloatSumAccumulator,
                                      "Error: expecting float sum accumulator");
          partitionAccumulators[index] =
            new SumAccumulators.DoubleSumAccumulator((SumAccumulators.FloatSumAccumulator)partitionAccumulator,
                                                     deserializedAccumulator,
                                                     hashTable.getMaxValuesPerBatch(),
                                                     computationVectorAllocator);
        } else if (partitionAccumulator.getInput() instanceof DecimalVector) {
           /* We started with DecimalSumAccumulator that has input vector of type DECIMAL
           * and accumulator vector of type DOUBLE and the output vector into which
           * we will eventually transfer contents also DOUBLE. We spilled the
           * accumulator vector. So DecimalSumAccumulator now becomes DoubleSumAccumulator
           * for post-spill processing with the accumulator vector read from spilled batch
           * acting as new input vector for DoubleSumAccumulator
           */
           Preconditions.checkArgument(partitionAccumulator instanceof SumAccumulators.DecimalSumAccumulator,
                                       "Error: expecting decimal sum accumulator");
          partitionAccumulators[index] =
            new SumAccumulators.DoubleSumAccumulator((SumAccumulators.DecimalSumAccumulator)partitionAccumulator,
                                                     deserializedAccumulator,
                                                     hashTable.getMaxValuesPerBatch(),
                                                     computationVectorAllocator);
        }

        break;
      }

      default: {
        /* we should not be here */
        throw new IllegalStateException("Error: incorrect type of deserialized sum accumulator");
      }
    }
  }

  private void updateSumZeroAccumulator(final FieldVector deserializedAccumulator,
                                        final Accumulator[] partitionAccumulators,
                                        final int index,
                                        final BufferAllocator computationVectorAllocator) {
    final Accumulator partitionAccumulator = partitionAccumulators[index];
    Types.MinorType type = deserializedAccumulator.getMinorType();
    switch (type) {
      case BIGINT: {
        if (partitionAccumulator.getInput() instanceof BigIntVector) {
          /* We started with BigIntSumZeroAccumulator so type doesn't change.
           * Just set the input vector to the accumulator vector read from
           * spilled batch
           */
          Preconditions.checkArgument(partitionAccumulator instanceof SumZeroAccumulators.BigIntSumZeroAccumulator,
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
          Preconditions.checkArgument(partitionAccumulator instanceof SumZeroAccumulators.IntSumZeroAccumulator,
                                      "Error: expecting int sumzero accumulator");
          partitionAccumulators[index] =
            new SumZeroAccumulators.BigIntSumZeroAccumulator((SumZeroAccumulators.IntSumZeroAccumulator)partitionAccumulator,
                                                             deserializedAccumulator,
                                                             hashTable.getMaxValuesPerBatch(),
                                                             computationVectorAllocator);
        }

        break;
      }

      case FLOAT8: {
        if (partitionAccumulator.getInput() instanceof Float8Vector) {
          /* We started with DoubleSumZeroAccumulator so type doesn't change.
           * Just set the input vector to the accumulator vector read from
           * spilled batch
           */
          Preconditions.checkArgument(partitionAccumulator instanceof SumZeroAccumulators.DoubleSumZeroAccumulator,
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
          Preconditions.checkArgument(partitionAccumulator instanceof SumZeroAccumulators.FloatSumZeroAccumulator,
                                      "Error: expecting float sum zero accumulator");
          partitionAccumulators[index] =
          new SumZeroAccumulators.DoubleSumZeroAccumulator((SumZeroAccumulators.FloatSumZeroAccumulator)partitionAccumulator,
                                                           deserializedAccumulator,
                                                           hashTable.getMaxValuesPerBatch(),
                                                           computationVectorAllocator);
        } else if (partitionAccumulator.getInput() instanceof DecimalVector) {
           /* We started with DecimalSumZeroAccumulator that has input vector of type DECIMAL
           * and accumulator vector of type DOUBLE and the output vector into which
           * we will eventually transfer contents also DOUBLE. We spilled the
           * accumulator vector. So DecimalSumZeroAccumulator now becomes DoubleSumAccumulator
           * for post-spill processing with the accumulator vector read from spilled batch
           * acting as new input vector for DoubleSumAccumulator
           */
          Preconditions.checkArgument(partitionAccumulator instanceof SumZeroAccumulators.DecimalSumZeroAccumulator,
                                      "Error: expecting decimal sum zero accumulator");
          partitionAccumulators[index] =
            new SumZeroAccumulators.DoubleSumZeroAccumulator((SumZeroAccumulators.DecimalSumZeroAccumulator) partitionAccumulator,
                                                             deserializedAccumulator,
                                                             hashTable.getMaxValuesPerBatch(),
                                                             computationVectorAllocator);
        }
        break;
      }

      default: {
        /* we should not be here */
        throw new IllegalStateException("Error: incorrect type of deserialized sumzero accumulator");
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
}
