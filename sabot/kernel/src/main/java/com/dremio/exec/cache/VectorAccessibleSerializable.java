/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.cache;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.hadoop.util.CompatibilityUtil;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.metrics.Metrics;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

/**
 * A wrapper around a VectorAccessible. Will serialize a VectorAccessible and write to an OutputStream, or can read
 * from an InputStream and construct a new VectorContainer.
 */
public class VectorAccessibleSerializable extends AbstractStreamSerializable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorAccessibleSerializable.class);
  static final MetricRegistry metrics = Metrics.getInstance();
  static final String WRITER_TIMER = MetricRegistry.name(VectorAccessibleSerializable.class, "writerTime");

  private byte tmpBuffer[] = new byte[32*1024];

  private VectorContainer va;
  private WritableBatch batch;
  private final BufferAllocator allocator;
  private int recordCount = -1;
  private BatchSchema.SelectionVectorMode svMode = BatchSchema.SelectionVectorMode.NONE;
  private SelectionVector2 sv2;

  private boolean retain = false;

  public VectorAccessibleSerializable(BufferAllocator allocator) {
    this.allocator = allocator;
    va = new VectorContainer();
  }

  public VectorAccessibleSerializable(WritableBatch batch, BufferAllocator allocator) {
    this(batch, null, allocator);
  }



  /**
   * Write the contents of a ArrowBuf to a stream. Done this way, rather
   * than calling the ArrowBuf.getBytes() method, because this method
   * avoids repeated heap allocation for the intermediate heap buffer.
   *
   * @param buf the ArrowBuf to write
   * @param output the output stream
   * @throws IOException if a write error occurs
   */

  private void writeBuf(ArrowBuf buf, OutputStream output) throws IOException {
    int bufLength = buf.readableBytes();
    for (int posn = 0; posn < bufLength; posn += tmpBuffer.length) {
      int len = Math.min(tmpBuffer.length, bufLength - posn);
      buf.getBytes(posn, tmpBuffer, 0, len);
      output.write(tmpBuffer, 0, len);
    }
  }

  /**
   * Creates a wrapper around batch and sv2 for writing to a stream. sv2 will never be released by this class, and ownership
   * is maintained by caller.
   * @param batch
   * @param sv2
   * @param allocator
   */
  public VectorAccessibleSerializable(WritableBatch batch, SelectionVector2 sv2, BufferAllocator allocator) {
    this.allocator = allocator;
    this.batch = batch;
    if (sv2 != null) {
      this.sv2 = sv2;
      svMode = BatchSchema.SelectionVectorMode.TWO_BYTE;
    }
  }

  /**
   * Reads from an InputStream and parses a RecordBatchDef. From this, we construct a SelectionVector2 if it exits
   * and construct the vectors and add them to a vector container
   * @param input the InputStream to read from
   * @throws IOException
   */
  @Override
  public void readFromStream(InputStream input) throws IOException {
    final VectorContainer container = new VectorContainer();
    final UserBitShared.RecordBatchDef batchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(input);
    recordCount = batchDef.getRecordCount();
    if (batchDef.hasCarriesTwoByteSelectionVector() && batchDef.getCarriesTwoByteSelectionVector()) {

      if (sv2 == null) {
        sv2 = new SelectionVector2(allocator);
      }
      sv2.allocateNew(recordCount * SelectionVector2.RECORD_SIZE);
      sv2.getBuffer().setBytes(0, input, recordCount * SelectionVector2.RECORD_SIZE);
      svMode = BatchSchema.SelectionVectorMode.TWO_BYTE;
    }
    final List<ValueVector> vectorList = Lists.newArrayList();
    final List<SerializedField> fieldList = batchDef.getFieldList();
    for (SerializedField metaData : fieldList) {
      final int dataLength = metaData.getBufferLength();
      final Field field = SerializedFieldHelper.create(metaData);
      final ArrowBuf buf = allocator.buffer(dataLength);
      final ValueVector vector;
      try {
        readIntoArrowBuf(input, buf, dataLength, tmpBuffer);
        vector = TypeHelper.getNewVector(field, allocator);
        TypeHelper.load(vector, metaData, buf);
      } finally {
        buf.release();
      }
      vectorList.add(vector);
    }
    container.addCollection(vectorList);
    container.buildSchema(svMode);
    container.setRecordCount(recordCount);
    va = container;
  }

  /**
   * Serializes the VectorAccessible va and writes it to an output stream
   * @param output the OutputStream to write to
   * @throws IOException
   */
  @Override
  public void writeToStream(OutputStream output) throws IOException {
    Preconditions.checkNotNull(output);
    final Timer.Context timerContext = metrics.timer(WRITER_TIMER).time();

    final ArrowBuf[] incomingBuffers = batch.getBuffers();
    final UserBitShared.RecordBatchDef batchDef = batch.getDef();

    /* ArrowBuf associated with the selection vector */
    ArrowBuf svBuf = null;
    Integer svCount =  null;

    if (svMode == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      svCount = sv2.getCount();
      svBuf = sv2.getBuffer(); //this calls retain() internally
    }

    try {
      /* Write the metadata to the file */
      batchDef.writeDelimitedTo(output);

      /* If we have a selection vector, dump it to file first */
      if (svBuf != null) {
        writeBuf(svBuf, output);
        sv2.setBuffer(svBuf);
        svBuf.release(); // sv2 now owns the buffer
        sv2.setRecordCount(svCount);
      }

      /* Dump the array of ByteBuf's associated with the value vectors */
      for (ArrowBuf buf : incomingBuffers) {
        /* dump the buffer into the OutputStream */
        writeBuf(buf, output);
      }

      output.flush();

      timerContext.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      clear();
    }
  }

  public void clear() {
    if (!retain) {
      batch.clear();
      if (sv2 != null) {
        sv2.clear();
      }
    }
  }

  public VectorContainer get() {
    return va;
  }

  public SelectionVector2 getSv2() {
    return sv2;
  }


  /**
   * Helper method that reads into <code>outputBuffer</code> from <code>inputStream</code>. It reads until
   * <code>numBytesToRead</code> is reached. If an EOF is reached before then an {@link EOFException} is thrown.
   * @param inputStream
   * @param outputBuffer
   * @param numBytesToRead
   * @throws IOException
   */
  public static void readIntoArrowBuf(InputStream inputStream, ArrowBuf outputBuffer, int numBytesToRead, byte[] buffer)
      throws IOException {
//  Disabling direct reads for this since we have to be careful to avoid issues with compatibilityutil where it caches failure or success in direct reading. Direct reading will fail for LocalFIleSystem. As such, if we enable this path, we will non-direct reading for all sources (including HDFS)
//    if(inputStream instanceof FSDataInputStream){
//      readFromStream((FSDataInputStream) inputStream, outputBuffer, numBytesToRead);
//      return;
//    }

    while(numBytesToRead > 0) {
      int len = Math.min(buffer.length, numBytesToRead);

      final int numBytesRead = inputStream.read(buffer, 0, len);
      if (numBytesRead == -1 && numBytesToRead > 0) {
        throw new EOFException("Unexpected end of stream while reading.");
      }

      // NOTE: when read into outputBuffer, writerIndex in ArrowBuf is incremented appropriately.
      outputBuffer.writeBytes(buffer, 0, numBytesRead);
      numBytesToRead -= numBytesRead;
    }
  }

  public static void readFromStream(FSDataInputStream input, final ArrowBuf outputBuffer, final int bytesToRead) throws IOException{
    final ByteBuffer directBuffer = outputBuffer.nioBuffer(0, bytesToRead);
    int lengthLeftToRead = bytesToRead;
    while (lengthLeftToRead > 0) {
      final int bytesRead = CompatibilityUtil.getBuf(input, directBuffer, lengthLeftToRead);;
      if (bytesRead == -1 && lengthLeftToRead > 0) {
        throw new EOFException("Unexpected end of stream while reading.");
      }
      lengthLeftToRead -= bytesRead;
    }
    outputBuffer.writerIndex(bytesToRead);
  }
}
