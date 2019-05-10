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
package com.dremio.exec.cache;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.io.SeekableInputStream;
import org.xerial.snappy.Snappy;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.dremio.common.AutoCloseables.RollbackCloseable;
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
import io.netty.util.internal.PlatformDependent;

/**
 * A wrapper around a VectorAccessible. Will serialize a VectorAccessible and write to an OutputStream, or can read
 * from an InputStream and construct a new VectorContainer.
 */
public class VectorAccessibleSerializable extends AbstractStreamSerializable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorAccessibleSerializable.class);
  static final MetricRegistry metrics = Metrics.getInstance();
  static final String WRITER_TIMER = MetricRegistry.name(VectorAccessibleSerializable.class, "writerTime");

  static final int COMPRESSED_LENGTH_BYTES = 4;
  public static final int RAW_CHUNK_SIZE_TO_COMPRESS = 32*1024;

  /*
   * A reusable buffer for I/O operations to avoid GC churn by creating too many byte arrays
   */
  private static final ThreadLocal<byte[]> REUSABLE_LARGE_BUFFER = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[RAW_CHUNK_SIZE_TO_COMPRESS*2];
    }
  };

  /*
   * A reusable buffer for int to array conversions
   */
  private static final ThreadLocal<byte[]> REUSABLE_SMALL_BUFFER = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[Integer.SIZE / Byte.SIZE];
    }
  };

  private VectorContainer va;
  private WritableBatch batch;
  private BufferAllocator allocator;
  private int recordCount = -1;
  private BatchSchema.SelectionVectorMode svMode = BatchSchema.SelectionVectorMode.NONE;
  private SelectionVector2 sv2;
  private final boolean useCodec;
  /* a separate allocator to be used for allocating buffers for decompressing spill files */
  private BufferAllocator decompressAllocator;

  private boolean retain = false;

  /**
   * De-serialize the batch
   * @param allocator
   */
  public VectorAccessibleSerializable(BufferAllocator allocator) {
    this.allocator = allocator;
    va = new VectorContainer();
    this.useCodec = false;
  }

  /**
   * Decompress the spill files when de-serializing the spilled batch
   * @param allocator
   * @param useCodec
   * @param decompressAllocator
   */
  public VectorAccessibleSerializable(BufferAllocator allocator, boolean useCodec, BufferAllocator decompressAllocator) {
    this.allocator = allocator;
    va = new VectorContainer();
    this.useCodec = useCodec;
    this.decompressAllocator = decompressAllocator;
    if (useCodec) {
      Preconditions.checkArgument((decompressAllocator != null), "decompress allocator can't be null when compressing spill files");
    }
  }

  /**
   * Creates a wrapper around batch for writing to a stream. Batch won't be compressed.
   * @param batch
   * @param allocator
   */
  public VectorAccessibleSerializable(WritableBatch batch, BufferAllocator allocator) {
    this(batch, null, allocator, false);
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
    /* Use current thread buffer (safe to do since I/O operation is blocking) */
    final byte[] tmpBuffer = REUSABLE_LARGE_BUFFER.get();
    for (int posn = 0; posn < bufLength; posn += tmpBuffer.length) {
      int len = Math.min(tmpBuffer.length, bufLength - posn);
      buf.getBytes(posn, tmpBuffer, 0, len);
      output.write(tmpBuffer, 0, len);
    }
  }

  /* compress the output buffers in 32KB chunks and serialize them as follows
   *
   *    <length bytes(4), data bytes, length bytes(4), data bytes ....>
   *
   * for every (length,data) pair, first 4 bytes represent the physical length of compressed data.
   * subsequent bytes represent the actual compressed data.
   */
  private void writeCompressedBuf(ArrowBuf buf, OutputStream output) throws IOException {
    int rawLength = buf.readableBytes();
    for (int posn = 0; posn < rawLength; posn += RAW_CHUNK_SIZE_TO_COMPRESS) {
      /* we compress 32KB chunks at a time; the last chunk might be smaller than 32KB */
      int lengthToCompress = Math.min(RAW_CHUNK_SIZE_TO_COMPRESS, rawLength - posn);

      /* allocate direct buffers to hold raw and compressed data */
      ByteBuffer rawDirectBuffer = buf.nioBuffer(posn, lengthToCompress);
      /* Since we don't know the exact size of compressed data, we can
       * allocate the compressed buffer of same size as raw data. However,
       * there could be cases where Snappy does not compress the data and the
       * compressed stream is of size larger (raw data + compression metadata)
       * than raw data. To handle these cases, we allocate compressed buffer
       * slightly larger than raw buffer. If we don't do this, Snappy.compress
       * will segfault.
       */
      final int maxCompressedLength = Snappy.maxCompressedLength(lengthToCompress);
      try (ArrowBuf cBuf = allocator.buffer(maxCompressedLength)) {
        ByteBuffer compressedDirectBuffer = cBuf.nioBuffer(0, maxCompressedLength);
        rawDirectBuffer.order(ByteOrder.LITTLE_ENDIAN);
        compressedDirectBuffer.order(ByteOrder.LITTLE_ENDIAN);

        /* compress */
        int compressedLength = Snappy.compress(rawDirectBuffer, compressedDirectBuffer);

        /* get compressed data into byte array for serializing to output stream */
        /* Use current thread buffer (safe to do since I/O operation is blocking) */
        final byte[] tmpBuffer = REUSABLE_LARGE_BUFFER.get();
        compressedDirectBuffer.get(tmpBuffer, 0, compressedLength);

        /* serialize the length of compressed data */
        output.write(getByteArrayFromLEInt(REUSABLE_SMALL_BUFFER.get(), compressedLength));
        /* serialize the compressed data */
        output.write(tmpBuffer, 0, compressedLength);
      }
    }
  }

  /**
   * Creates a wrapper around batch and sv2 for writing to a stream. sv2 will never be released by this class, and ownership
   * is maintained by caller. Also indicates whether compression is to be used for spilling the batch.
   * @param batch
   * @param sv2
   * @param allocator
   */
  public VectorAccessibleSerializable(WritableBatch batch, SelectionVector2 sv2, BufferAllocator allocator, boolean useCodec) {
    this.allocator = allocator;
    this.batch = batch;
    if (sv2 != null) {
      this.sv2 = sv2;
      svMode = BatchSchema.SelectionVectorMode.TWO_BYTE;
    }
    this.useCodec = useCodec;
  }

  /**
   * Reads from an InputStream and parses a RecordBatchDef. From this, we construct a SelectionVector2 if it exits
   * and construct the vectors and add them to a vector container
   * @param input the InputStream to read from
   * @throws IOException
   */
  @Override
  public void readFromStream(InputStream input) throws IOException {
    try (RollbackCloseable rollback = new RollbackCloseable()) {
      final VectorContainer container = rollback.add(new VectorContainer());
      final UserBitShared.RecordBatchDef batchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(input);
      recordCount = batchDef.getRecordCount();
      if (batchDef.hasCarriesTwoByteSelectionVector() && batchDef.getCarriesTwoByteSelectionVector()) {

        if (sv2 == null) {
          sv2 = rollback.add(new SelectionVector2(allocator));
        }
        sv2.allocateNew(recordCount * SelectionVector2.RECORD_SIZE);
        sv2.getBuffer().setBytes(0, input, recordCount * SelectionVector2.RECORD_SIZE);
        svMode = BatchSchema.SelectionVectorMode.TWO_BYTE;
      }
      final List<ValueVector> vectorList = Lists.newArrayList();
      final List<SerializedField> fieldList = batchDef.getFieldList();
      for (SerializedField metaData : fieldList) {
        final int rawDataLength = metaData.getBufferLength();
        final Field field = SerializedFieldHelper.create(metaData);
        ArrowBuf buf = null;
        final ValueVector vector;
        try {
          buf = allocator.buffer(rawDataLength);
          if (useCodec) {
            readAndUncompressIntoArrowBuf(input, buf, rawDataLength);
          } else {
            readIntoArrowBuf(input, buf, rawDataLength);
          }
          vector = TypeHelper.getNewVector(field, allocator);
          /*
           * Even though vector container is rollbackable, we don't add
           * anything to vector container until later when all the vectors
           * have been successfully allocated and that implies complete
           * I/O was successfully done. However, if we fail in the middle of
           * read where some calls to readFromArrowBuf() have already completed,
           * we still need to release memory for those successfully allocated
           * vectors. The finally block below will take care only of the current buffer
           * that we allocated just before the read call that failed.
           */
          rollback.add(vector);
          TypeHelper.load(vector, metaData, buf);
        } finally {
          if (buf != null) {
            buf.release();
          }
        }
        vectorList.add(vector);
      }
      container.addCollection(vectorList);
      container.buildSchema(svMode);
      container.setRecordCount(recordCount);
      va = container;

      rollback.commit();
    } catch (Exception e) {
      throw new IOException("Failed to deserialize on-disk vector batch", e);
    }
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
        if (useCodec) {
          /* if we are serializing the spilled data, compress the ArrowBufs */
          writeCompressedBuf(buf, output);
        }
        else {
          writeBuf(buf, output);
        }
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
  public static void readIntoArrowBuf(InputStream inputStream, ArrowBuf outputBuffer, int numBytesToRead)
      throws IOException {
//  Disabling direct reads for this since we have to be careful to avoid issues with compatibilityutil where it caches failure or success in direct reading. Direct reading will fail for LocalFIleSystem. As such, if we enable this path, we will non-direct reading for all sources (including HDFS)
//    if(inputStream instanceof FSDataInputStream){
//      readFromStream((FSDataInputStream) inputStream, outputBuffer, numBytesToRead);
//      return;
//    }

    /* Use current thread buffer (safe to do since I/O operation is blocking) */
    final byte[] buffer = REUSABLE_LARGE_BUFFER.get();
    while(numBytesToRead > 0) {
      int len = Math.min(buffer.length, numBytesToRead);

      final int numBytesRead = inputStream.read(buffer, 0, len);
      if (numBytesRead == -1 && numBytesToRead > 0) {
        throw new EOFException("Unexpected end of stream while reading.");
      }

      /* NOTE: when read into outputBuffer, writerIndex in ArrowBuf is incremented automatically */
      outputBuffer.writeBytes(buffer, 0, numBytesRead);
      numBytesToRead -= numBytesRead;
    }
  }

  /* writeCompressedBuf compressed data in 32KB chunks and serialized them in the following manner
   *
   *   <length bytes(4), data bytes, length bytes(4), data bytes .......>
   *
   * so when reading back compressed data from input stream, we will first read 4 bytes
   * as this represents the compressed length of a chunk and then we will read the subsequent
   * bytes (compressed data), uncompress them and append into the output ArrowBuf.
   */
  private void readAndUncompressIntoArrowBuf(InputStream inputStream, ArrowBuf outputBuffer, int rawDataLength)
    throws IOException {
    int bufferPos = 0;

    /* Use current thread buffer (safe to do since I/O operation is blocking) */
    final byte[] buffer = REUSABLE_LARGE_BUFFER.get();
    while(rawDataLength > 0) {
      /* read the first 4 bytes to get the length of subsequent compressed bytes */
      int numBytesToRead = COMPRESSED_LENGTH_BYTES;
      while(numBytesToRead > 0) {
        final int numBytesRead = inputStream.read(buffer, COMPRESSED_LENGTH_BYTES - numBytesToRead, numBytesToRead);
        if (numBytesRead == -1 && numBytesToRead > 0) {
          throw new EOFException("Unexpected end of stream while reading.");
        }
        numBytesToRead -= numBytesRead;
      }

      /* convert the bytes read above to LE Int to get the actual length of compressed data */
      int compressedLengthToRead = getLEIntFromByteArray(buffer);
      Preconditions.checkArgument(buffer.length >= compressedLengthToRead, "bad compressed length");

      /* allocate a direct buffer to hold the compressed data */
      try (ArrowBuf cBuf = decompressAllocator.buffer(compressedLengthToRead)) {
        ByteBuffer compressedDirectBuffer = cBuf.nioBuffer(0, compressedLengthToRead);
        compressedDirectBuffer.order(ByteOrder.LITTLE_ENDIAN);

        /* read the compressed bytes */
        int compressedOffset = 0;
        while (compressedLengthToRead > 0) {
          final int numCompressedBytesRead = inputStream.read(buffer, compressedOffset, compressedLengthToRead);
          if (numCompressedBytesRead == -1) {
            throw new IOException("ERROR: total length of compressed data read is less than expected");
          }
          compressedLengthToRead -= numCompressedBytesRead;
          compressedOffset += numCompressedBytesRead;
        }
        if(compressedOffset != compressedDirectBuffer.limit()) {
          throw new IOException("ERROR: total length of compressed data read is less than expected");
        }

        /* load the compressed data from byte array into direct buffer */
        compressedDirectBuffer.put(buffer, 0, compressedOffset);
        compressedDirectBuffer.position(0);

        /* for each chunk we decompress, the raw length should be 32KB or less (for the last chunk) */
        final int rawBufferSize = (rawDataLength >= RAW_CHUNK_SIZE_TO_COMPRESS) ? RAW_CHUNK_SIZE_TO_COMPRESS : rawDataLength;

        /* get the direct buffer to store the uncompressed data */
        ByteBuffer rawDirectBuffer = outputBuffer.nioBuffer(bufferPos, rawBufferSize);
        rawDirectBuffer.order(ByteOrder.LITTLE_ENDIAN);

        /* uncompress */
        int uncompressedLength = Snappy.uncompress(compressedDirectBuffer, rawDirectBuffer);

        /* update state */
        rawDataLength -= uncompressedLength;
        bufferPos += uncompressedLength;
      }
    }
    outputBuffer.writerIndex(bufferPos);
  }

  private int getLEIntFromByteArray(byte[] array) {
    return PlatformDependent.getInt(array, 0);
  }

  private byte[] getByteArrayFromLEInt(byte[] array, int value) {
    PlatformDependent.putInt(array, 0, value);
    return array;
  }

  public static void readFromStream(SeekableInputStream input, final ArrowBuf outputBuffer, final int bytesToRead) throws IOException{
    final ByteBuffer directBuffer = outputBuffer.nioBuffer(0, bytesToRead);
    input.readFully(directBuffer);
    outputBuffer.writerIndex(bytesToRead);
  }
}
