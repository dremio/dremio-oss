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
package com.dremio.exec.cache;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import com.dremio.exec.record.ArrowRecordBatchLoader;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.LocalSyncableFileSystem.WritesArrowBuf;
import com.dremio.io.FSInputStream;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;
import io.netty.util.internal.PlatformDependent;

/*
 * Wrapper class to serializes a VectorContainer and write to OutputStream, or deserializes an input stream and create
 * a VectorContainer.
 *
 * Serialization Format:
 * [0:4] Schema Length [Todo]
 * [4:8] ArrowRecordBatch Length
 * [8:12] ArrowBuffers Length
 * [12:16] SV Mode [Todo]
 * Followed by respective serialized structures.
 */
public class VectorAccessibleFlatBufSerializable extends AbstractStreamSerializable {

  private VectorAccessible va;
  private BufferAllocator allocator;
  private final OperatorStats operatorStats;

  private boolean writeDirect;

  private long bytesWritten;

  private final byte[] heapMoveBuffer = new byte[64*1024];

  public VectorAccessibleFlatBufSerializable(VectorAccessible va, BufferAllocator allocator) {
    this(va, allocator, null);
  }

  public VectorAccessibleFlatBufSerializable(VectorAccessible va, BufferAllocator allocator, OperatorStats operatorStats) {
    this.va = va;
    this.allocator = allocator;
    this.operatorStats = operatorStats;
  }

  public VectorAccessible get() {
    return va;
  }

  public void setWriteDirect(boolean writeDirect) {
    this.writeDirect = writeDirect;
  }

  @Override
  public void readFromStream(InputStream input) throws IOException {
    try {
      byte[] lengths = new byte[16];
      readFully(lengths, input);
      int schemaLen = PlatformDependent.getInt(lengths, 0);
      int headerLen = PlatformDependent.getInt(lengths, 4);
      int bodyLen = PlatformDependent.getInt(lengths, 8);
      int sv2Present = PlatformDependent.getInt(lengths, 12);

      Preconditions.checkArgument(schemaLen == 0 && ( va != null && va.getSchema() != null));
      Preconditions.checkArgument(sv2Present == 0);

      // read header (RecordBatch)
      byte[] header = new byte[headerLen];
      readFully(header, input);
      RecordBatch recordBatch = RecordBatch.getRootAsRecordBatch(ByteBuffer.wrap(header));

      // read body
      try(ArrowBuf body = allocator.buffer(bodyLen)) {
        read(body, bodyLen, input);

        ArrowRecordBatchLoader.load(recordBatch, va, body);
      }
      int recordCount = (int) recordBatch.length();
      ((VectorContainer) va).setAllCount(recordCount);
    } catch (Exception ex) {
      throw new IOException("Failed to load data into vector container", ex);
    }
  }

  private void readFully(byte[] target, InputStream input) throws IOException {
    try(OperatorStats.WaitRecorder waitRecorder = OperatorStats.getWaitRecorder(operatorStats)) {
      readFully(target, 0, target.length, input);
    }
  }

  private void readFully(byte[] target, int offset, int len, InputStream input) throws IOException {
    org.apache.arrow.util.Preconditions.checkArgument(offset >= 0 && offset < target.length);
    org.apache.arrow.util.Preconditions.checkArgument(len <= target.length);

    while(len > 0) {
      int read = input.read(target, offset, len);
      len -= read;
      offset += read;
    }
  }

  private void readUsingHeapBuffer(ArrowBuf outputBuffer, long numBytesToRead, InputStream input) throws IOException {
    final byte[] heapMoveBuffer = this.heapMoveBuffer;
    while(numBytesToRead > 0) {
      int len = (int) Math.min(heapMoveBuffer.length, numBytesToRead);

      int numBytesRead = -1;
      try(OperatorStats.WaitRecorder waitRecorder = OperatorStats.getWaitRecorder(operatorStats)) {
        numBytesRead = input.read(heapMoveBuffer, 0, len);
      }
      if (numBytesRead == -1) {
        throw new EOFException("Unexpected end of stream while reading.");
      }

      /* NOTE: when read into outputBuffer, writerIndex in ArrowBuf is incremented automatically */
      outputBuffer.writeBytes(heapMoveBuffer, 0, numBytesRead);
      numBytesToRead -= numBytesRead;
    }
  }

  private void read(ArrowBuf outputBuffer, long numBytesToRead, InputStream input) throws IOException {
    if (input instanceof FSInputStream) {
      FSInputStream fsInputStream = (FSInputStream)input;
      ByteBuf readByteBuf = NettyArrowBuf.unwrapBuffer(outputBuffer);
      ByteBuffer readBuffer = readByteBuf.nioBuffer(0, (int) numBytesToRead);
      int totalRead = 0;
      readBuffer.clear();
      readBuffer.limit((int)numBytesToRead);

      try (OperatorStats.WaitRecorder waitRecorder = OperatorStats.getWaitRecorder(operatorStats)) {
        while (totalRead < numBytesToRead) {
          final int nRead = fsInputStream.read(readBuffer);
          if (nRead < 0) {
            break;
          }
          totalRead += nRead;
        }
      }
    } else {
      readUsingHeapBuffer(outputBuffer, numBytesToRead, input);
    }
  }

  @Override
  public void writeToStream(OutputStream output) throws IOException {
    try (ArrowRecordBatch recordBatch = getRecordBatch(va)) {
      FlatBufferBuilder fbbuilder = new FlatBufferBuilder();
      fbbuilder.finish(recordBatch.writeTo(fbbuilder));
      ByteBuffer header = fbbuilder.dataBuffer();

      int headerLen = header.remaining();
      int bodyLen = (int) recordBatch.getBuffers().stream().mapToLong(ArrowBuf::readableBytes).sum();

      byte[] lengths = new byte[16];
      PlatformDependent.putInt(lengths, 0, 0);
      PlatformDependent.putInt(lengths, 4, headerLen);
      PlatformDependent.putInt(lengths, 8, bodyLen);
      PlatformDependent.putInt(lengths, 12, 0);
      output.write(lengths);

      long len = write(header, output);
      if (writeDirect && output instanceof WritesArrowBuf) {
        for (ArrowBuf buf : recordBatch.getBuffers()) {
          len += ((WritesArrowBuf) output).write(buf);
        }
      } else {
        for (ArrowBuf buf : recordBatch.getBuffers()) {
          len += writeViaCopy(buf, output);
        }
      }

      Preconditions.checkArgument(len == headerLen + bodyLen,
        "Unexpected write length.");
      bytesWritten = 16 + len;
    }
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  private long write(ByteBuffer buf, OutputStream output) throws IOException {
    long bufLength = buf.remaining();
    while (buf.remaining() > 0) {
      int len = Math.min(heapMoveBuffer.length, buf.remaining());
      buf.get(heapMoveBuffer, 0, len);
      output.write(heapMoveBuffer, 0, len);
    }
    return bufLength;
  }

  private long writeViaCopy(ArrowBuf buf, OutputStream output) throws IOException {
    long bufLength = buf.readableBytes();
    for (long posn = 0; posn < bufLength; posn += heapMoveBuffer.length) {
      int len = (int) Math.min(heapMoveBuffer.length, bufLength - posn);
      buf.getBytes(posn, heapMoveBuffer, 0, len);
      output.write(heapMoveBuffer, 0, len);
    }
    return bufLength;
  }

  private ArrowRecordBatch getRecordBatch(VectorAccessible va) {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();
    List<FieldVector> vectors = StreamSupport.stream(va.spliterator(), false).map(vw -> ((FieldVector)vw.getValueVector())).collect(Collectors.toList());

    for (FieldVector vector : vectors) {
      appendNodes(vector, nodes, buffers);
    }
    return new ArrowRecordBatch(va.getRecordCount(), nodes, buffers, NoCompressionCodec.DEFAULT_BODY_COMPRESSION, false);
  }

  private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    nodes.add(new ArrowFieldNode(vector.getValueCount(), -1));
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    buffers.addAll(fieldBuffers);
    for (FieldVector child : vector.getChildrenFromFields()) {
      appendNodes(child, nodes, buffers);
    }
  }
}
