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
package com.dremio.sabot.op.join.vhash.spill.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.LargeMemoryUtil;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.sort.external.SpillManager.SpillInputStream;
import com.dremio.sabot.op.sort.external.SpillManager.SpillOutputStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/**
 * Utility class for ser/deser of spill records.
 * Each chunk has the following format :
 *   1. 4-byte magic
 *   2. 4-byte for number-of-records in the chunk
 *   3. 4-byte len for pivoted fixed portion
 *   4. 4-byte len for unpivoted fixed portion
 *   5. serialized (arrow format) batch for unpivoted columns
 *
 *   The sum of (3) & (4) must be fit in one page. deserialized output of (5) must fit in one page.
 *
 *  A Spill file contains one or more chunks in the above format.
 */
public class SpillSerializable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpillSerializable.class);
  private static final int IO_CHUNK_SIZE = 32 * 1024;
  private static final int CHUNK_HEADER_LEN = 16;
  private static final int CHUNK_MAGIC_OFFSET = 0;
  private static final int NUM_RECORDS_LENGTH_OFFSET = 4;
  private static final int FIXED_BUFFER_LENGTH_OFFSET = 8;
  private static final int VARIABLE_BUFFER_LENGTH_OFFSET = 12;
  private static final int CHUNK_MAGIC = 0x6a6f696e; // 'JOIN' in hex

  private final ByteBuffer intBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE); //byte array of 4 bytes
  private final byte[] ioBuffer = new byte[IO_CHUNK_SIZE];

  public long writeChunkToStream(SpillChunk chunk, SpillOutputStream output) throws IOException {
    byte[] bytes;
    long bytesWritten = 0;

    // write the magic
    bytes = getByteArrayFromInt(LargeMemoryUtil.checkedCastToInt(CHUNK_MAGIC));
    output.write(bytes);
    bytesWritten += bytes.length;

    // write the record count
    bytes = getByteArrayFromInt(LargeMemoryUtil.checkedCastToInt(chunk.getNumRecords()));
    output.write(bytes);
    bytesWritten += bytes.length;

    // write the lengths of fixed & variable parts from pivoted data
    bytes = getByteArrayFromInt(LargeMemoryUtil.checkedCastToInt(chunk.getFixed().capacity()));
    output.write(bytes);
    bytesWritten += bytes.length;

    bytes = getByteArrayFromInt(LargeMemoryUtil.checkedCastToInt(chunk.getVariable().capacity()));
    output.write(bytes);
    bytesWritten += bytes.length;

    // write fixed & variable parts from pivoted data
    bytesWritten += writeArrowBuf(chunk.getFixed(), output);
    bytesWritten += writeArrowBuf(chunk.getVariable(), output);

    // write out non-pivoted data
    bytesWritten += output.writeBatch(chunk.getContainer());
    return bytesWritten;
  }

  // write one arrow buf to the output stream.
  private long writeArrowBuf(ArrowBuf buffer, SpillOutputStream output) throws IOException {
    final int bufferLength = LargeMemoryUtil.checkedCastToInt(buffer.capacity());
    for (int writePos = 0; writePos < bufferLength; writePos += ioBuffer.length) {
      final int lengthToWrite = Math.min(ioBuffer.length, bufferLength - writePos);
      buffer.getBytes(writePos, ioBuffer, 0, lengthToWrite);
      output.write(ioBuffer, 0, lengthToWrite);
    }
    return bufferLength;
  }

  SpillChunk readChunkFromStream(PagePool pagePool, BatchSchema unpivotedColumnsSchema, SpillInputStream input) throws IOException {
    try (AutoCloseables.RollbackCloseable rc = new AutoCloseables.RollbackCloseable(true)) {
      Page pivotedPage = rc.add(pagePool.newPage());
      Page unpivotedPage = rc.add(pagePool.newPage());

      /* read chunk header */
      int numBytesToRead = CHUNK_HEADER_LEN;
      while (numBytesToRead > 0) {
        int numBytesRead = input.read(ioBuffer, CHUNK_HEADER_LEN - numBytesToRead, numBytesToRead);
        if (numBytesRead == -1) {
          /* indicate detection of unexpected end of stream */
          return null;
        }
        numBytesToRead -= numBytesRead;
      }

      // get the length of the fixed/variable portions from the header
      final int magic = getLEIntFromByteArray(ioBuffer, CHUNK_MAGIC_OFFSET);
      Preconditions.checkState(magic == CHUNK_MAGIC);

      final int numRecords = getLEIntFromByteArray(ioBuffer, NUM_RECORDS_LENGTH_OFFSET);
      final int fixedBufferLength = getLEIntFromByteArray(ioBuffer, FIXED_BUFFER_LENGTH_OFFSET);
      final int variableBufferLength = getLEIntFromByteArray(ioBuffer, VARIABLE_BUFFER_LENGTH_OFFSET);

      // read the fixed buffer
      ArrowBuf fixed = rc.add(pivotedPage.slice(fixedBufferLength));
      readIntoArrowBuf(fixed, fixedBufferLength, input);

      // read the variable buffer
      ArrowBuf variable = rc.add(pivotedPage.slice(variableBufferLength));
      readIntoArrowBuf(variable, variableBufferLength, input);

      // read the non-pivoted portion
      VectorContainer container = rc.add(new VectorContainer(pagePool.getAllocator()));
      container.addSchema(unpivotedColumnsSchema);
      container.buildSchema();
      container.zeroVectors();
      input.load(container, unpivotedPage::slice);
      rc.commit();

      return new SpillChunk(numRecords, fixed, variable, container, ImmutableList.of(pivotedPage, unpivotedPage));
    } catch (IOException|RuntimeException ex) {
      logger.error("failed to read chunk", ex);
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  // read one arrow buf from the input stream
  private void readIntoArrowBuf(final ArrowBuf buffer, final int bufferLength,
                                final SpillInputStream input) throws IOException {
    int numBytesToRead = bufferLength;
    while (numBytesToRead > 0) {
      final int lenghtToRead = Math.min(ioBuffer.length, numBytesToRead);
      final int numBytesRead = input.read(ioBuffer, 0, lenghtToRead);
      if (numBytesRead == -1) {
        throw new EOFException("ERROR: Unexpected end of stream while reading chunk data");
      }
      buffer.writeBytes(ioBuffer, 0, numBytesRead);
      numBytesToRead -= numBytesRead;
    }
  }

  private byte[] getByteArrayFromInt(int value) {
    PlatformDependent.putInt(intBuffer.array(), 0, value);
    return intBuffer.array();
  }

  private int getLEIntFromByteArray(byte[] array, int index) {
    return PlatformDependent.getInt(array, index);
  }
}
