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
package com.dremio.sabot.rpc.user;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.WritableBatch;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for common functionality used by a backwards compatibility handler. The subclasses
 * implement specific functionality for patching buffers/data etc before exchanging with clients
 * over the wire.
 */
abstract class BaseBackwardsCompatibilityHandler {

  static final class QueryBatch {
    private final QueryData header;
    private final ByteBuf[] buffers;

    public QueryBatch(QueryData header, ByteBuf... buffers) {
      this.header = header;
      this.buffers = buffers;
      for (ByteBuf b : buffers) {
        Preconditions.checkNotNull(b);
      }
    }

    public ByteBuf[] getBuffers() {
      return buffers;
    }

    public long getByteCount() {
      long n = 0;
      for (ByteBuf buf : buffers) {
        n += buf.readableBytes();
      }
      return n;
    }

    public QueryData getHeader() {
      return header;
    }

    @Override
    public String toString() {
      return "QueryBatch [header=" + header + ", buffers=" + Arrays.toString(buffers) + "]";
    }
  }

  private static final Logger logger =
      LoggerFactory.getLogger(BaseBackwardsCompatibilityHandler.class);
  private final BufferAllocator allocator;

  BaseBackwardsCompatibilityHandler(final BufferAllocator allocator) {
    this.allocator = allocator;
  }

  protected BufferAllocator getAllocator() {
    return allocator;
  }

  /** If we received this data off the network, we need to slice it so we can replace pieces. */
  protected ByteBuf[] sliceIfNecessary(QueryBatch batch) {
    ByteBuf[] buffers = batch.getBuffers();
    if (buffers != null && buffers.length != 1) {
      return buffers;
    }

    try (VectorContainer vc = new VectorContainer()) {
      final ArrowBuf buf = ((NettyArrowBuf) buffers[0]).arrowBuf();
      int bufOffset = 0;
      for (final SerializedField field : batch.getHeader().getDef().getFieldList()) {
        final Field fieldDef = SerializedFieldHelper.create(field);
        ValueVector vector = TypeHelper.getNewVector(fieldDef, allocator);
        if (field.getValueCount() == 0) {
          AllocationHelper.allocate(vector, 0, 0, 0);
        } else {
          TypeHelper.load(vector, field, buf.slice(bufOffset, field.getBufferLength()));
        }
        bufOffset += field.getBufferLength();
        vc.add(vector);
      }
      vc.buildSchema();
      vc.setAllCount(batch.getHeader().getRowCount());
      buf.close();
      return WritableBatch.get(vc).getBuffers();
    }
  }

  public QueryBatch makeBatchCompatible(QueryBatch batch) {
    QueryData.Builder header = batch.getHeader().toBuilder();
    List<SerializedField.Builder> fieldBuilders = header.getDefBuilder().getFieldBuilderList();
    ByteBuf[] buffers = sliceIfNecessary(batch);
    try {
      patchFields(fieldBuilders, buffers, 0, buffers.length, "$root$", "");
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Failure patching batch for compatibility. schema: "
              + batch.getHeader()
              + " buffers: "
              + sizesString(buffers, 0, buffers.length),
          e);
    }
    QueryData newHeader = header.build();
    return new QueryBatch(newHeader, buffers);
  }

  protected void patchFields(
      List<SerializedField.Builder> fields,
      ByteBuf[] oldBuffers,
      final int bufferStart,
      final int buffersLength,
      String parentName,
      String indent) {
    Preconditions.checkArgument(
        bufferStart >= 0
            && ((oldBuffers.length == 0 && bufferStart == 0) || bufferStart < oldBuffers.length)
            && buffersLength >= 0
            && (bufferStart + buffersLength) <= oldBuffers.length,
        "bufferStart: %s, buffersLength: %s, oldBuffers.length: %s",
        bufferStart,
        buffersLength,
        oldBuffers.length);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}fields: {}, buffers: {} for {}",
          indent,
          sizesString(fields),
          sizesString(oldBuffers, bufferStart, buffersLength),
          parentName);
    }

    int bufferIndex = bufferStart;
    int childBufferLength = buffersLength;
    for (SerializedField.Builder field : fields) {
      int fieldBuffersLength = fieldBuffersCount(field, oldBuffers, bufferIndex, childBufferLength);
      patch(field, oldBuffers, bufferIndex, fieldBuffersLength, parentName, indent);
      bufferIndex += fieldBuffersLength;
      childBufferLength -= fieldBuffersLength;
    }
    // skip empty trailing buffers
    while (bufferIndex < (bufferStart + buffersLength)
        && oldBuffers[bufferIndex].readableBytes() == 0) {
      ++bufferIndex;
    }
    if (bufferIndex != (bufferStart + buffersLength)) {
      throw new IllegalStateException(
          "Fields ("
              + sizeTreeString(fields)
              + ") should have consumed all the buffers: "
              + sizesString(oldBuffers, bufferStart, buffersLength)
              + " "
              + bufferIndex
              + " != "
              + (bufferStart + buffersLength)
              + " parent: "
              + parentName);
    }
  }

  public abstract void patch(
      SerializedField.Builder field,
      ByteBuf[] buffers,
      int bufferStart,
      int buffersLength,
      String parentName,
      String indent);

  /**
   * present the list of sizes for the designated buffers
   *
   * @param buffers
   * @param bufferStart
   * @param buffersLength
   * @return
   */
  protected static String sizesString(ByteBuf[] buffers, int bufferStart, int buffersLength) {
    StringBuilder buffersSizes =
        new StringBuilder("#").append(bufferStart).append("/ ").append(buffersLength).append("[ ");
    for (int i = 0; i < buffersLength; i++) {
      ByteBuf byteBuf = buffers[bufferStart + i];
      buffersSizes.append(byteBuf.readableBytes()).append(" ");
    }
    buffersSizes.append("]");
    return buffersSizes.toString();
  }

  protected static String sizesString(List<SerializedField.Builder> fields) {
    StringBuilder fieldsString = new StringBuilder(" ");
    for (SerializedField.Builder field : fields) {
      fieldsString.append(field.getBufferLength()).append(" ");
    }
    return fieldsString.toString();
  }

  protected static String sizeTreeString(List<SerializedField.Builder> fields) {
    StringBuilder fieldsString = new StringBuilder("(");
    boolean first = true;
    for (SerializedField.Builder field : fields) {
      if (first) {
        first = false;
      } else {
        fieldsString.append(", ");
      }
      fieldsString.append(field.getBufferLength());
      if (field.getChildCount() > 0) {
        fieldsString.append(sizeTreeString(field.getChildBuilderList()));
      }
    }
    fieldsString.append(")");
    return fieldsString.toString();
  }

  /**
   * finds the buffers belonging to the current field starting at buffersStart their total size is
   * equal to field.bufferLength
   *
   * @param field current field
   * @param buffers record batch buffers
   * @param buffersStart current position
   * @param buffersLength current buffer count for the parent
   * @return buffer count for this field starting at buffersStart
   */
  static int fieldBuffersCount(
      SerializedField.Builder field,
      ByteBuf[] buffers,
      final int buffersStart,
      final int buffersLength) {
    int totalBufferWidth = 0;
    int lastIndex = buffersStart;
    while (totalBufferWidth < field.getBufferLength() && lastIndex < buffersStart + buffersLength) {
      ByteBuf buf = buffers[lastIndex];
      totalBufferWidth += buf.readableBytes();
      ++lastIndex;
    }
    if (totalBufferWidth != field.getBufferLength()) {
      throw new IllegalStateException(
          "not enough buffers for field "
              + field.build()
              + " in "
              + sizesString(buffers, buffersStart, buffersLength)
              + " lastIndex = "
              + lastIndex
              + " bs="
              + buffersStart
              + " bl = "
              + buffersLength
              + " "
              + totalBufferWidth
              + " != "
              + field.getBufferLength());
    }
    return lastIndex - buffersStart;
  }

  public void close() throws Exception {
    allocator.close();
  }
}
