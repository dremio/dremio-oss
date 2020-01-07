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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.arrow.vector.FieldVector;

import com.dremio.exec.cache.AbstractStreamSerializable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.HashAggPartitionWritableBatch.HashAggPartitionBatchDefinition;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Implements Serialization and Deserialization for {@link VectorizedHashAggPartition}.
 * Provides support for reading and writing partition structures for
 * {@link VectorizedHashAggOperator}.
 *
 * For consistency and ease of design, the semantics of this module are kept
 * same as for {@link com.dremio.exec.cache.VectorAccessibleSerializable} as and when applicable.
 *
 * Partition Structures:
 * 1. LBlockHashTable
 *  -- fixed block vector(s)
 *  -- variable block vector(s)
 * 2. Accumulation vector(s)
 *  -- multiple accumulators
 *  -- each accumulator may have multiple accumulation vectors
 *     depending on number of batches inserted into hash table
 *
 * This module only implements I/O functionality -- SerDe for partition structures.
 * The module should not have any knowledge of spill file, spill manager etc.
 */
public class VectorizedHashAggPartitionSerializable extends AbstractStreamSerializable {

  private VectorizedHashAggPartition hashAggPartition;
  private final PartitionToLoadSpilledData partitionToLoadSpilledData;
  private static final int IO_CHUNK_SIZE = 32 * 1024;
  private byte ioBuffer[] = new byte[IO_CHUNK_SIZE];
  private long numBatchesSpilled;
  private long numRecordsSpilled;
  private long spilledDataSize;
  private ByteBuffer intBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE); //byte array of 4 bytes
  private HashAggPartitionWritableBatch inProgressWritableBatch;
  private final OperatorStats operatorStats;
  /**
   * Used to serialize a HashAggPartition to disk. Caller should use
   * this constructor when they decide a particular partition
   * should be spilled to disk.
   *
   * @param hashAggPartition hash agg partition to serialize/spill
   *
   * the caller is expected to invoke writeToStream(output stream) method after
   * instantiating VectorizedHashAggPartitionSerializable to serialize the
   * partition to disk.
   */
  public VectorizedHashAggPartitionSerializable(final VectorizedHashAggPartition hashAggPartition, final OperatorStats stats) {
    this.hashAggPartition = hashAggPartition;
    this.partitionToLoadSpilledData = null;
    this.operatorStats = stats;
    initLocalStats();
  }

  /**
   * Used to deserialize spilled partition data from disk.
   *
   * @param partitionToLoadSpilledData extra partition (aka loading partition) used
   *                                   to deserialize spilled batches.
   *
   * the caller is expected to invoke readFromStream(input stream) method after
   * instantiating this object to read back a spilled partition.
   */
  public VectorizedHashAggPartitionSerializable(final PartitionToLoadSpilledData partitionToLoadSpilledData, final OperatorStats stats) {
    Preconditions.checkArgument(partitionToLoadSpilledData != null, "ERROR: Need a valid handle for loading partition for reading spilled batches");
    this.hashAggPartition = null;
    this.partitionToLoadSpilledData = partitionToLoadSpilledData;
    this.operatorStats = stats;
    initLocalStats();
  }

  private void initLocalStats() {
    this.numBatchesSpilled = 0;
    this.numRecordsSpilled = 0;
    this.spilledDataSize = 0;
  }

  /**
   * This function reads spilled hashagg partition data (a single batch)
   * from disk.
   *
   * @param input input stream for the source spill file.
   * @throws IOException
   *
   * NOTE: Once this module is glued with the new spill service,
   * few things are expected to change.
   */
  @Override
  public void readFromStream(final InputStream input) throws IOException {
    /* STEP 1: read chunk header */
    int numBytesToRead = HashAggPartitionWritableBatch.BATCH_HEADER_LENGTH;
    while (numBytesToRead > 0) {
      int numBytesRead;
      //track io time as wait time
      try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        numBytesRead = input.read(ioBuffer, HashAggPartitionWritableBatch.BATCH_HEADER_LENGTH - numBytesToRead, numBytesToRead);
      }
      if (numBytesRead == -1) {
        /* indicate detection of unexpected end of stream */
        partitionToLoadSpilledData.setRecordsInBatch(-1);
        return;
      }
      numBytesToRead -= numBytesRead;
    }

    /* STEP 2: parse header from the bytes read above */
    final int fixedBufferLength = getLEIntFromByteArray(ioBuffer, HashAggPartitionWritableBatch.FIXED_BUFFER_LENGTH_OFFSET);
    final int variableBufferLength = getLEIntFromByteArray(ioBuffer, HashAggPartitionWritableBatch.VARIABLE_BUFFER_LENGTH_OFFSET);
    final int numAccumulators = getLEIntFromByteArray(ioBuffer, HashAggPartitionWritableBatch.NUM_ACCUMULATORS_OFFSET);

    /* STEP 3: read info on types of accumulators -- sum, min, max etc */
    final byte[] accumulatorTypes = partitionToLoadSpilledData.getAccumulatorTypes();
    Preconditions.checkArgument(input.read(accumulatorTypes, 0, numAccumulators) == numAccumulators,
      "ERROR: read incorrect length of accumulator types");

    /* STEP 4: read metadata for accumulator vectors */
    final UserBitShared.RecordBatchDef accumulatorBatchDef;
    //track io time as wait time
    try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      accumulatorBatchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(input);
    }
    final int numRecordsInBatch = accumulatorBatchDef.getRecordCount();
    Preconditions.checkArgument(numRecordsInBatch <= partitionToLoadSpilledData.getPreallocatedBatchSize(),
      "Error: incorrect preallocated batch size for reading spilled batch");
    partitionToLoadSpilledData.setRecordsInBatch(numRecordsInBatch);

    /* STEP 5: Get buffers to read the fixed and variable width pivoted key column data.
     * We are not expected to fail here. As per the minimum upfront memory allocation
     * requirements and the rule enforced on max variable width data length in a single batch,
     * we should pre-allocate enough memory for a single batch. Since we spill and read back at
     * batch boundaries, we should have necessary memory in the extra partition.
     */
    final ArrowBuf fixedBlockBuffer = partitionToLoadSpilledData.getFixedKeyColPivotedData();
    final ArrowBuf variableBlockBuffer = partitionToLoadSpilledData.getVariableKeyColPivotedData();
    Preconditions.checkArgument(fixedBufferLength <= fixedBlockBuffer.capacity(),
      "Error: detected incorrect amount of provisioned memory for deserializing fixed width spilled data");
    Preconditions.checkArgument(variableBufferLength <= variableBlockBuffer.capacity(),
      "Error: detected  incorrect amount of provisioned memory for deserializing variable width spilled data");

    /* STEP 6: read fixed width pivoted data from spilled chunk */
    readIntoArrowBuf(fixedBlockBuffer, fixedBufferLength, input);
    Preconditions.checkArgument(fixedBlockBuffer.readableBytes() == fixedBufferLength,
      "ERROR: read incorrect length of fixed width data from spilled chunk");

    /* STEP 7: read variable width pivoted data from spilled chunk */
    readIntoArrowBuf(variableBlockBuffer, variableBufferLength, input);
    Preconditions.checkArgument(variableBlockBuffer.readableBytes() == variableBufferLength,
      "ERROR: read incorrect length of variable width data from spilled chunk");

    /* STEP 8: read data for accumulator vectors from spilled chunk */
    readAccumulators(accumulatorBatchDef, input);
  }

  /**
   * Read accumulator vectors from spilled batch into memory. The memory for de-serializing comes
   * from{@link PartitionToLoadSpilledData} that has pre-allocated memory for all the structures
   * written in a spilled batch.
   *
   * @param batchDef batch definition
   * @param input input stream
   * @throws IOException
   */
  private void readAccumulators(final UserBitShared.RecordBatchDef batchDef,
                                final InputStream input) throws IOException {
    final FieldVector[] vectorList = partitionToLoadSpilledData.getPostSpillAccumulatorVectors();
    final List<UserBitShared.SerializedField> fieldList = batchDef.getFieldList();
    Preconditions.checkArgument(fieldList.size() == vectorList.length, "Error: read incorrect number of accumulator vectors from spilled batch");
    int count = 0;
    for (UserBitShared.SerializedField metaData : fieldList) {
      final FieldVector vector = vectorList[count];
      final int rawDataLength = metaData.getBufferLength();
      final UserBitShared.SerializedField bitsField = metaData.getChild(0);
      final UserBitShared.SerializedField valuesField = metaData.getChild(1);
      final int bitsLength = bitsField.getBufferLength();
      final int dataLength = valuesField.getBufferLength();
      Preconditions.checkArgument(rawDataLength == bitsLength + dataLength, "Error, read incorrect accumulator vector buffer length");
      final ArrowBuf validityBuffer = vector.getValidityBuffer();
      final ArrowBuf dataBuffer = vector.getDataBuffer();
      readIntoArrowBuf(validityBuffer, bitsLength, input);
      readIntoArrowBuf(dataBuffer, dataLength, input);
      vector.setValueCount(metaData.getValueCount());
      count++;
    }
  }

  /**
   * Reads an ArrowBuf from stream
   *
   * @param buffer buffer to read
   * @param bufferLength length of buffer
   * @param input source input stream
   *
   * @throws IOException
   */
  private void readIntoArrowBuf(final ArrowBuf buffer, final int bufferLength,
                                final InputStream input) throws IOException {
    int numBytesToRead = bufferLength;
    while (numBytesToRead > 0) {
      final int lenghtToRead = Math.min(ioBuffer.length, numBytesToRead);
      final int numBytesRead;
      //track io time as wait time
      try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        numBytesRead = input.read(ioBuffer, 0, lenghtToRead);
      }
      if (numBytesRead == -1 && numBytesToRead > 0) {
        throw new EOFException("ERROR: Unexpected end of stream while reading chunk data");
      }
      buffer.writeBytes(ioBuffer, 0, numBytesRead);
      numBytesToRead -= numBytesRead;
    }
  }

  /**
   * This function spills a partition to disk.
   *
   * @param output output stream for the target spill file
   *
   * The caller controls the output stream semantics. For example,
   * caller uses the spill manager and provides a high performance
   * output stream corresponding to a spill file.
   *
   * It goes over all batches in the partition and serializes one batch at
   * a time.
   */
  @Override
  public void writeToStream(final OutputStream output) throws IOException {
    final LBlockHashTable hashTable = hashAggPartition.hashTable;
    if (isPartitionEmpty(hashTable)) {
      return;
    }
    final List<ArrowBuf> fixedBlockBuffers = hashTable.getFixedBlockBuffers();
    final List<ArrowBuf> variableBlockBuffers = hashTable.getVariableBlockBuffers();
    Preconditions.checkArgument((fixedBlockBuffers.size() == variableBlockBuffers.size()),
                                "ERROR: inconsistent number of buffers in hash table");
    final HashAggPartitionWritableBatch hashTableWritableBatch =
      new HashAggPartitionWritableBatch(hashTable, fixedBlockBuffers, variableBlockBuffers,
                                        hashAggPartition.blockWidth, hashAggPartition.accumulator,
                                        hashTable.size(), hashTable.getMaxValuesPerBatch());
    HashAggPartitionBatchDefinition batchDefinition;
    /* spill entire partition -- one batch at a time */
    while ((batchDefinition = hashTableWritableBatch.getNextWritableBatch()) != null) {
      writeBatchToStreamHelper(output, hashTableWritableBatch, batchDefinition);
    }
  }

  /**
   * Writes a batch (comprising of one or more ArrowBufs) to the
   * provided output stream
   * @param output output stream handle for a spill file
   * @param writableBatch batch to spill
   * @param batchDefinition batch metadata
   * @throws IOException
   */
  private void writeBatchToStreamHelper(final OutputStream output,
                                        final HashAggPartitionWritableBatch writableBatch,
                                        final HashAggPartitionBatchDefinition batchDefinition) throws IOException {
    /* write chunk metadata */
    writeBatchDefinition(batchDefinition, output);
    final ArrowBuf[] buffersToSpill = writableBatch.getBuffers();
      /* write chunk data */
    for (ArrowBuf buffer: buffersToSpill) {
      spilledDataSize += buffer.readableBytes();
      writeArrowBuf(buffer, output);
    }
    numBatchesSpilled++;
    numRecordsSpilled += batchDefinition.accumulatorBatchDef.getRecordCount();
  }

  /**
   * Spills a single batch from victim partition to disk
   *
   * @param output output stream for the target spill file
   * @return true if there are no more batches to be spilled, false otherwise
   * @throws IOException failure while writing to stream
   */
  boolean writeBatchToStream(final OutputStream output) throws Exception {
    if (inProgressWritableBatch == null) {
      final LBlockHashTable hashTable = hashAggPartition.hashTable;
      final List<ArrowBuf> fixedBlockBuffers = hashTable.getFixedBlockBuffers();
      final List<ArrowBuf> variableBlockBuffers = hashTable.getVariableBlockBuffers();
      Preconditions.checkArgument((fixedBlockBuffers.size() == variableBlockBuffers.size()),
                                  "ERROR: inconsistent number of buffers in hash table");
      inProgressWritableBatch = new HashAggPartitionWritableBatch(hashTable, fixedBlockBuffers, variableBlockBuffers,
                                                                  hashAggPartition.blockWidth, hashAggPartition.accumulator,
                                                                  hashTable.size(), hashTable.getMaxValuesPerBatch());
    }

    HashAggPartitionBatchDefinition batchDefinition = inProgressWritableBatch.getNextWritableBatch();

    if (batchDefinition == null) {
      inProgressWritableBatch = null;
      return true;
    }

    final int idx = batchDefinition.getCurrentBatchIndex();
    writeBatchToStreamHelper(output, inProgressWritableBatch, batchDefinition);

    //release memory from this batch
    hashAggPartition.hashTable.releaseBatch(idx);
    return false;
  }

  /**
   * Check if a partition is empty
   * @param partitionHashTable partition's hashtable
   * @return true if partition is empty (0 entries in hashtable), false otherwise
   */
  private boolean isPartitionEmpty(final LBlockHashTable partitionHashTable) {
    if (partitionHashTable.size() == 0) {
      Preconditions.checkArgument(partitionHashTable.blocks() == 0, "Error: detected inconsistent hashtable state");
      return true;
    }

    return false;
  }

  /**
   * Get number of batches spilled
   * @return batches spilled
   */
  long getNumBatchesSpilled() {
    return numBatchesSpilled;
  }

  /**
   * Get number of records spilled
   * @return records spilled
   */
  long getNumRecordsSpilled() {
    return numRecordsSpilled;
  }

  /**
   * Get size (in bytes) of data spilled
   * @return size of spilled data
   */
  long getSpilledDataSize() {
    return spilledDataSize;
  }

  /**
   * Write the ArrowBuf to stream.
   *
   * @param buffer buffer to write
   * @param output output stream for the spill file
   * @throws IOException if IO fails
   */

  private void writeArrowBuf(final ArrowBuf buffer, final OutputStream output) throws IOException {
    final int bufferLength = buffer.readableBytes();
    for (int writePos = 0; writePos < bufferLength; writePos += ioBuffer.length) {
      final int lengthToWrite = Math.min(ioBuffer.length, bufferLength - writePos);
      buffer.getBytes(writePos, ioBuffer, 0, lengthToWrite);
      //track io time as wait time
      try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        output.write(ioBuffer, 0, lengthToWrite);
      }
    }
  }

  /**
   * Write the batch metadata
   *
   * @param batchDefinition
   * @param output
   *
   * @throws IOException
   */
  private void writeBatchDefinition(final HashAggPartitionBatchDefinition batchDefinition,
                                    final OutputStream output) throws IOException {
    try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      output.write(getByteArrayFromInt(batchDefinition.fixedBufferLength));
      output.write(getByteArrayFromInt(batchDefinition.variableBufferLength));
      output.write(getByteArrayFromInt(batchDefinition.numAccumulators));
      output.write(batchDefinition.accumulatorTypes);
      batchDefinition.accumulatorBatchDef.writeDelimitedTo(output);
    }
  }

  /**
   * Returns the VectorizedHashAggPartition constructed after
   * reading data from disk.
   *
   * Used only for test purposes to verify SerDe and that we
   * are able to reconstruct in-memory structures as expected.
   *
   * @return partition
   */
  @VisibleForTesting
  public VectorizedHashAggPartition getPartition() {
    return hashAggPartition;
  }

  private int getLEIntFromByteArray(byte[] array, int index) {
    return PlatformDependent.getInt(array, index);
  }

  public byte[] getByteArrayFromInt(int value) {
    PlatformDependent.putInt(intBuffer.array(), 0, value);
    return intBuffer.array();
  }
}
