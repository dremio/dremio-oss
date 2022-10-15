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

import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.SKETCH_SIZE;

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedListVarcharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.Numbers;
import com.dremio.exec.expr.TypeHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 *
 * This partition is also known as "EXTRA PARTITION" or "READ PARTITION".
 * The data structures in this partition are pre-allocated (upfront) during
 * operator setup with the purpose of always having enough memory to load a
 * single spilled batch of data from a partition.
 *
 * {@link VectorizedHashAggOperator} is responsible for allocating the
 * memory for this partition and it should happen at the very beginning
 * during operator setup when we take care of other kinds of pre-allocation.
 * The memory associated with data structures here will not be released
 * until operator itself is closed; all data has been pumped out of the
 * operator.
 *
 * This partition is _NOT_ accounted towards the number of partitions used
 * by {@link VectorizedHashAggOperator} to partition/distribute the
 * incoming data. It is sort of a holder with pre-allocated data
 * structures to deserialize spilled batch which then serves as a new
 * incoming into the operator.
 *
 * Description of data structures:
 *
 * (1) Fixed Width Buffer:
 *
 * ArrowBuf to hold pivoted data for fixed width GROUP BY key columns. We spill
 * pivoted data and we read back the same pivoted streams which are good enough
 * to be re-inserted into the hash tables when we start processing a particular
 * spilled partition and re-partition. So, we don't have to pivot again after
 * reading a spilled batch.
 *
 * (2) Variable Width Buffer:
 *
 * ArrowBuf to hold pivoted data for variable width GROUP BY key columns.
 * Similar to fixed width pivoted data, this is used as is for re-partitioning
 * and insertion into multiple hash tables.
 *
 * (3) Post Spill Accumulator Vectors:
 *
 * Along with pivoted key data from hash table, we also spill accumulator output
 * vectors which hold computed values. When we read back a spilled batch,
 * it has the accumulator vectors too and they should now become as input
 * accumulator vectors.
 *
 * Together these data structures constitute "post-spill" incoming batch
 * which is used by the operator to restart aggregation processing --
 * re-partitioning, accumulation etc.
 *
 */
public class PartitionToLoadSpilledData implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionToLoadSpilledData.class);
  private final BufferAllocator allocator;
  private ArrowBuf fixedKeyColPivotedData;
  private ArrowBuf variableKeyColPivotedData;
  private final FieldVector[] postSpillAccumulatorVectors;
  private final int batchSize;
  private int recordsInBatch;
  private final byte[] accumulatorTypes;
  private final int varLenAccumulatorCapacity;

  public PartitionToLoadSpilledData(final BufferAllocator allocator,
                                    final int fixedDataLength,
                                    final int variableDataLength,
                                    final List<Field> postSpillAccumulatorVectorFields,
                                    final byte[] accumulatorTypes,
                                    final int batchSize,
                                    final int varLenAccumulatorCapacity) throws Exception {
    Preconditions.checkArgument(allocator != null, "Error: need a valid allocator to pre-allocate memory");
    this.allocator = allocator;
    this.varLenAccumulatorCapacity = varLenAccumulatorCapacity;
    /* we use Numbers.nextPowerOfTwo because that is how memory allocation happens
     * inside FixedBlockVector and VariableBlockVector when inserting into hashtable.
     * if we don't use nextPowerOfTwo for actual allocation size, we might run into
     * situation where a spilled batch has more data than the capacity we have pre-allocated
     * here to load the spilled batch into memory.
     */
    try(AutoCloseables.RollbackCloseable rollbackable = new AutoCloseables.RollbackCloseable()) {
      fixedKeyColPivotedData = allocator.buffer(Numbers.nextPowerOfTwo(fixedDataLength));
      rollbackable.add(fixedKeyColPivotedData);
      variableKeyColPivotedData = allocator.buffer(Numbers.nextPowerOfTwo(variableDataLength));
      rollbackable.add(variableKeyColPivotedData);
      initBuffers();
      this.batchSize = batchSize;
      this.recordsInBatch = 0;
      this.postSpillAccumulatorVectors = new FieldVector[postSpillAccumulatorVectorFields.size()];
      this.accumulatorTypes = new byte[postSpillAccumulatorVectorFields.size()];
      initPostSpillAccumulatorVectors(postSpillAccumulatorVectorFields, accumulatorTypes, rollbackable);
      rollbackable.commit();
      logger.debug("Extra Partition Pre-allocation, fixed-data length: {}, variable-data length: {}, actual fixed-data capacity: {}, actual variable-data capacty: {}, batchSize: {}",
                   fixedDataLength, variableDataLength, fixedKeyColPivotedData.capacity(), variableKeyColPivotedData.capacity(), batchSize);
    }
  }

  /**
   * Pre-Allocate the accumulator vectors that will be used to deserialize spilled
   * accumulators. These vectors are constructed keeping the target accumulator type
   * in mind. For example SUM(INT) will accumulate into a BIGINT vector and the latter
   * one is spilled so the vectors for deserialization should be preallocated
   * considering the type. The target type information is already available here
   * since {@link VectorizedHashAggOperator} has materialized the aggregate
   * expressions.
   * @param postSpillAccumulatorVectorFields accumulator vector types
   */
  private void initPostSpillAccumulatorVectors(final List<Field> postSpillAccumulatorVectorFields,
                                               final byte[] accumulatorTypes,
                                               final AutoCloseables.RollbackCloseable rollbackCloseable) {
    int count = 0;
    for (Field field : postSpillAccumulatorVectorFields) {
      FieldVector vector = TypeHelper.getNewVector(field, allocator);
      rollbackCloseable.add(vector);
      /* we have aggregation on INT, BIGINT, FLOAT, FLOAT4 and DECIMAL types of
       * columns which are all fixed width as well as (for min/max) on VARCHAR/VARBINARY
       * with variable width. Also the aggregation of HLL and HLL_MERGE are also output
       * as variable width.
       */
      final Types.MinorType type = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(field.getType());
      if (type == Types.MinorType.VARCHAR || type == Types.MinorType.VARBINARY || type == Types.MinorType.LIST) {
        if (accumulatorTypes[count] == AccumulatorBuilder.AccumulatorType.MAX.ordinal() ||
            accumulatorTypes[count] == AccumulatorBuilder.AccumulatorType.MIN.ordinal()) {
          ((BaseVariableWidthVector)vector).allocateNew(varLenAccumulatorCapacity, batchSize);
        } else if (accumulatorTypes[count] == AccumulatorBuilder.AccumulatorType.LISTAGG.ordinal() ||
                   accumulatorTypes[count] == AccumulatorBuilder.AccumulatorType.LOCAL_LISTAGG.ordinal() ||
                   accumulatorTypes[count] == AccumulatorBuilder.AccumulatorType.LISTAGG_MERGE.ordinal()) {
          vector = FixedListVarcharVector.allocListVector(allocator, batchSize);
        } else {
          Preconditions.checkArgument(accumulatorTypes[count] == AccumulatorBuilder.AccumulatorType.HLL.ordinal() ||
            accumulatorTypes[count] == AccumulatorBuilder.AccumulatorType.HLL_MERGE.ordinal());
          ((BaseVariableWidthVector)vector).allocateNew(SKETCH_SIZE * batchSize, batchSize);
        }
      } else {
        Preconditions.checkArgument(vector instanceof BaseFixedWidthVector, "Error: detected invalid accumulator vector type");
        ((BaseFixedWidthVector) vector).allocateNew(batchSize);
      }

      Preconditions.checkArgument(vector.getValueCapacity() >= batchSize, "Error: failed to correctly pre-allocate accumulator vector in extra partition");
      postSpillAccumulatorVectors[count] = vector;
      accumulatorTypes[count] = (byte)type.ordinal();
      count++;
    }
  }

  /**
   * Initialize the reader and writer index of buffers.
   * Also the contents of buffers are zeroed out.
   */
  private void initBuffers() {
    fixedKeyColPivotedData.setZero(0, fixedKeyColPivotedData.capacity());
    variableKeyColPivotedData.setZero(0, variableKeyColPivotedData.capacity());
    fixedKeyColPivotedData.readerIndex(0);
    fixedKeyColPivotedData.writerIndex(0);
    variableKeyColPivotedData.readerIndex(0);
    variableKeyColPivotedData.writerIndex(0);
  }

  /**
   * Get the buffer that stores the fixed width pivoted
   * GROUP BY key column data after reading a spilled batch.
   *
   * @return buffer that stores the fixed width data
   */
  public ArrowBuf getFixedKeyColPivotedData() {
    return fixedKeyColPivotedData;
  }

  /**
   * Get the buffer that stores the variable width pivoted
   * GROUP BY key column data after reading a spilled batch.
   *
   * @return buffer that stores the variable width data
   */
  public ArrowBuf getVariableKeyColPivotedData() {
    return variableKeyColPivotedData;
  }

  /**
   * Get pre-allocated batch size.
   *
   * @return batch size
   */
  public int getPreallocatedBatchSize() {
    return batchSize;
  }

  /**
   * Get accumulator types from deserialized spilled batch
   *
   * @return accumulator types
   */
  public byte[] getAccumulatorTypes() {
    return accumulatorTypes;
  }

  /**
   * Get number of records in deserialized spilled batch
   * This should never be more than pre-allocated batch size.
   *
   * @return number of records in deserialized spilled batch
   */
  public int getRecordsInBatch() {
    Preconditions.checkArgument(recordsInBatch == -1 || recordsInBatch > 0);
    return recordsInBatch;
  }

  /**
   * Get the array of vectors that is used to store
   * accumulator vector data from deserialized spilled
   * batch. The vectors then act as new incoming
   * into {@link VectorizedHashAggOperator} for post-spill
   * processing.
   *
   * @return accumulator vectors
   */
  public FieldVector[] getPostSpillAccumulatorVectors() {
    return postSpillAccumulatorVectors;
  }

  /**
   * Set the number of records in deserialized spilled batch.
   * This is used by {@link VectorizedHashAggPartitionSerializable} when
   * it is loading a spilled batch into memory
   *
   * @param records number of records in deserialized spilled batch
   */
  public void setRecordsInBatch(final int records) {
    Preconditions.checkArgument(records == -1 || records > 0);
    Preconditions.checkArgument(records <= batchSize, "Error: detected invalid number of records read from spilled batch");
    this.recordsInBatch = records;
  }

  /**
   * This is done after reading every spilled batch so that
   * we can correctly identify length of data read in every batch
   * and the length doesn't end up being cumulative which will then
   * lead to incorrect results, segfaults etc.
   *
   * It is probably not necessary to zero out the buffers again
   * Overwriting the contents should be fine as long as we
   * correctly identify the length read in each iteration.
   *
   * Zeroing out is likely an expensive operation so unless
   * we figure out it is absolutely necessary for correctness,
   * we will just reset the reader/writer indexes.
   */
  public void reset() {
    recordsInBatch = 0;
    fixedKeyColPivotedData.readerIndex(0);
    fixedKeyColPivotedData.writerIndex(0);
    variableKeyColPivotedData.readerIndex(0);
    variableKeyColPivotedData.writerIndex(0);
    for (FieldVector vector : postSpillAccumulatorVectors) {
      vector.reset();
      vector.setValueCount(0);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(ImmutableList.copyOf(postSpillAccumulatorVectors));
    if (variableKeyColPivotedData != null) {
      variableKeyColPivotedData.close();
      variableKeyColPivotedData = null;
    }
    if (fixedKeyColPivotedData != null) {
      fixedKeyColPivotedData.close();
      fixedKeyColPivotedData = null;
    }
  }
}
