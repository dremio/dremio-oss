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
package com.dremio.sabot.rpc.user;

import static com.dremio.common.types.TypeProtos.DataMode.OPTIONAL;
import static com.dremio.common.types.TypeProtos.DataMode.REQUIRED;
import static com.dremio.common.types.TypeProtos.MinorType.BIT;
import static com.dremio.common.types.TypeProtos.MinorType.INTERVALDAY;
import static com.dremio.common.types.TypeProtos.MinorType.VARBINARY;
import static com.dremio.common.types.TypeProtos.MinorType.VARCHAR;
import static java.lang.String.format;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.rpc.OutboundRpcMessage;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.google.common.base.Preconditions;
import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * This encoder ensures that the data batches sent to the user are backwards compatible i.e. if necessary, the batches
 * are patched to be compatible with the old Drill protocol. So if the client on the session is using an older record
 * batch format, this encoder is added to the Netty pipeline.
 */
class BackwardsCompatibilityEncoder extends MessageToMessageEncoder<OutboundRpcMessage> {
  private static final Logger logger = LoggerFactory.getLogger(BackwardsCompatibilityEncoder.class);

  private final BufferAllocator allocator;

  BackwardsCompatibilityEncoder(final BufferAllocator allocator) {
    super(OutboundRpcMessage.class);
    this.allocator = allocator;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, OutboundRpcMessage msg, List<Object> out) throws Exception {
    if (msg.mode == RpcMode.REQUEST &&
        msg.rpcType == RpcType.QUERY_DATA.getNumber()) {

      final QueryData oldHeader = (QueryData) msg.getPBody();
      final ByteBuf[] oldData = msg.getDBodies();

      final QueryWritableBatch oldBatch = new QueryWritableBatch(oldHeader, oldData);
      final QueryWritableBatch newBatch = makeBatchCompatible(oldBatch);

      out.add(new OutboundRpcMessage(msg.mode, RpcType.QUERY_DATA, msg.coordinationId,
          newBatch.getHeader(), newBatch.getBuffers()));
    } else {
      out.add(msg);
    }
  }

  /**
   * If we received this data off the network, we need to slice it so we can replace pieces.
   */
  private ByteBuf[] sliceIfNecessary(QueryWritableBatch batch) {
    ByteBuf[] buffers = batch.getBuffers();
    if (buffers != null && buffers.length != 1) {
      return buffers;
    }

    try (VectorContainer vc = new VectorContainer()) {
      final ArrowBuf buf = (ArrowBuf) buffers[0];
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
      buf.release();
      return WritableBatch.get(vc).getBuffers();
    }
  }

  private QueryWritableBatch makeBatchCompatible(QueryWritableBatch batch) {
    QueryData.Builder header = batch.getHeader().toBuilder();
    List<SerializedField.Builder> fieldBuilders = header.getDefBuilder().getFieldBuilderList();
    ByteBuf[] buffers = sliceIfNecessary(batch);
    try {
      patchFields(fieldBuilders, buffers, 0, buffers.length, "$root$", "");
    } catch (RuntimeException e) {
      throw new IllegalStateException("Failure patching batch for compatibility. schema: " + batch.getHeader()
          + " buffers: " + sizesString(buffers, 0, buffers.length), e);
    }
    QueryData newHeader = header.build();
    return new QueryWritableBatch(newHeader, buffers);
  }

  private void patchFields(List<SerializedField.Builder> fields, ByteBuf[] oldBuffers, final int bufferStart,
                           final int buffersLength, String parentName, String indent) {
    Preconditions.checkArgument(
        bufferStart >= 0
            && ((oldBuffers.length == 0 && bufferStart == 0) || bufferStart < oldBuffers.length)
            && buffersLength >= 0
            && (bufferStart + buffersLength) <= oldBuffers.length,
        "bufferStart: %s, buffersLength: %s, oldBuffers.length: %s", bufferStart, buffersLength, oldBuffers.length);

    if (logger.isDebugEnabled()) {
      logger.debug(format("%sfields: %s, buffers: %s for %s", indent, sizesString(fields),
          sizesString(oldBuffers, bufferStart, buffersLength), parentName));
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
    while (bufferIndex < (bufferStart + buffersLength) && oldBuffers[bufferIndex].readableBytes() == 0) {
      ++bufferIndex;
    }
    if (bufferIndex != (bufferStart + buffersLength)) {
      throw new IllegalStateException("Fields (" + sizeTreeString(fields)
          + ") should have consumed all the buffers: " + sizesString(oldBuffers, bufferStart, buffersLength)
          + " " + bufferIndex + " != " + (bufferStart + buffersLength) + " parent: " + parentName);
    }
  }

  private void patch(SerializedField.Builder field, ByteBuf[] buffers, int bufferStart,
                     int buffersLength, String parentName, String indent) {
    DataMode mode = field.getMajorType().getMode();
    MinorType minor = field.getMajorType().getMinorType();
    String name = field.getNamePart().getName();
    boolean changed = false;
    if (logger.isDebugEnabled()) {
      logger.debug(format("%sBEFORE PATCH: buffers %s for field %s.%s: %s %s expecting %s", indent,
          sizesString(buffers, bufferStart, buffersLength), parentName, name, mode, minor, field.getBufferLength()));
    }
    if ("$values$".equals(name)) {
      // the values vectors inside the NullableVectors used to be called the same as their parent.
      // now they are just "$values$"
      // the drill load code will check this
      field.getNamePartBuilder().setName(parentName);
    }

    List<SerializedField.Builder> children = field.getChildBuilderList();
    // when there are no values we don't need to patch data and some buffers are omitted
    if (field.getValueCount() != 0) {
      // need to patch the bit vector
      if (mode == OPTIONAL) {
        // neither the offset vector nor the bitVector are empty if there is more than one value.
        // an empty buffer is a leftover from a previous value vector.
        while (buffers[bufferStart].readableBytes() == 0) {
          ++bufferStart;
          --buffersLength;
        }
        // most nullable fields put their bitVector first
        // List has it after the offset vector
        int bitsIndex = minor == MinorType.LIST ? 1 : 0;

        // patch the bit buffer for compatibility
        SerializedField.Builder bitsField = children.get(bitsIndex);
        ArrowBuf bitsBuffer = (ArrowBuf) buffers[bufferStart + bitsIndex];
        // the bit vectors need to be converted from nodes to bytes
        if (bitsField.getMajorType().getMinorType() != BIT
            || !"$bits$".equals(bitsField.getNamePart().getName())
            || bitsField.getMajorType().getMode() != REQUIRED) {
          throw new IllegalStateException("bit vector should be called $bits$ and have type REQUIRED BIT." +
              " Found field: " + field.build());
        }
        ArrowBuf newBuf = convertBitsToBytes(allocator, bitsField, bitsBuffer);
        buffers[bufferStart + bitsIndex] = newBuf;
        changed = true;
      }
      // need to patch the value width
      if (minor == INTERVALDAY && mode == REQUIRED) {
        // IntervalDay in Drill as an extra unused 4 bytes
        // type width is wrong in ValueVectorTypes.tdd
        // padding each value
        ArrowBuf newBuf = padValues(allocator, field, buffers[bufferStart], 8, 12);
        buffers[bufferStart] = newBuf;
      }
    }
    if (children.size() > 0) {
      if ((minor == VARCHAR || minor == VARBINARY) && mode == REQUIRED) {
        int childrenBufferLength = 0;
        for (SerializedField.Builder child : children) {
          childrenBufferLength += child.getBufferLength();
        }
        int newBuffersLength;
        // the VARCHAR/VARBINARY vectors have their values buffer in the end unless it is empty
        if (childrenBufferLength == field.getBufferLength()) {
          newBuffersLength = buffersLength;
        } else {
          newBuffersLength = buffersLength - 1;
          if (childrenBufferLength + buffers[bufferStart + newBuffersLength].readableBytes()
              != field.getBufferLength()) {
            throw new IllegalStateException("bufferLength mismatch for field " + field.build()
                + " children: " + sizesString(buffers, bufferStart, buffersLength));
          }
        }
        patchFields(children, buffers, bufferStart, newBuffersLength, name, indent + "  ");
      } else {
        // others have children only
        patchFields(children, buffers, bufferStart, buffersLength, name, indent + "  ");
      }
    }

    int bufferLength = 0;
    for (int i = 0; i < buffersLength; i++) {
      ByteBuf buffer = buffers[i + bufferStart];
      bufferLength += buffer.readableBytes();
    }
    // if children sizes have changed parent must be updated too
    if (field.getBufferLength() != bufferLength) {
      field.setBufferLength(bufferLength);
      changed = true;
    }
    if (logger.isDebugEnabled() && changed) {
      logger.debug(format("%sAFTER PATCH: buffers %s for field %s.%s: %s %s expecting %s", indent,
          sizesString(buffers, bufferStart, buffersLength), parentName, name, mode, minor, field.getBufferLength()));
    }
  }

  /**
   * present the list of sizes for the designated buffers
   *
   * @param buffers
   * @param bufferStart
   * @param buffersLength
   * @return
   */
  private static String sizesString(ByteBuf[] buffers, int bufferStart, int buffersLength) {
    StringBuilder buffersSizes = new StringBuilder("#")
        .append(bufferStart)
        .append("/ ")
        .append(buffersLength)
        .append("[ ");
    for (int i = 0; i < buffersLength; i++) {
      ByteBuf byteBuf = buffers[bufferStart + i];
      buffersSizes.append(byteBuf.readableBytes()).append(" ");
    }
    buffersSizes.append("]");
    return buffersSizes.toString();
  }

  private static String sizesString(List<SerializedField.Builder> fields) {
    StringBuilder fieldsString = new StringBuilder(" ");
    for (SerializedField.Builder field : fields) {
      fieldsString.append(field.getBufferLength()).append(" ");
    }
    return fieldsString.toString();
  }

  private static String sizeTreeString(List<SerializedField.Builder> fields) {
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
   * finds the buffers belonging to the current field starting at buffersStart
   * their total size is equal to field.bufferLength
   *
   * @param field         current field
   * @param buffers       record batch buffers
   * @param buffersStart  current position
   * @param buffersLength current buffer count for the parent
   * @return buffer count for this field starting at buffersStart
   */
  static int fieldBuffersCount(SerializedField.Builder field, ByteBuf[] buffers,
                               final int buffersStart, final int buffersLength) {
    int totalBufferWidth = 0;
    int lastIndex = buffersStart;
    while (totalBufferWidth < field.getBufferLength() && lastIndex < buffersStart + buffersLength) {
      ByteBuf buf = buffers[lastIndex];
      totalBufferWidth += buf.readableBytes();
      ++lastIndex;
    }
    if (totalBufferWidth != field.getBufferLength()) {
      throw new IllegalStateException("not enough buffers for field " + field.build()
          + " in " + sizesString(buffers, buffersStart, buffersLength) + " lastIndex = " + lastIndex
          + " bs=" + buffersStart + " bl = " + buffersLength + " " + totalBufferWidth
          + " != " + field.getBufferLength());
    }
    return lastIndex - buffersStart;
  }

  static ArrowBuf convertBitsToBytes(BufferAllocator allocator,
                                     SerializedField.Builder fieldBuilder, ArrowBuf oldBuf) {
    ArrowBuf newBuf;
    try (BitVector bitVector = new BitVector("$bits$", allocator);
         UInt1Vector byteVector = new UInt1Vector("$bits$", allocator);) {
      TypeHelper.load(bitVector, fieldBuilder.build(), oldBuf);
      BitVector.Accessor bitsAccessor = bitVector.getAccessor();
      int valueCount = bitsAccessor.getValueCount();
      UInt1Vector.Mutator bytesMutator = byteVector.getMutator();
      byteVector.allocateNew(valueCount);
      for (int i = 0; i < valueCount; i++) {
        bytesMutator.set(i, bitsAccessor.get(i));
      }
      bytesMutator.setValueCount(valueCount);
      SerializedField newField = TypeHelper.getMetadata(byteVector);
      fieldBuilder.setMajorType(newField.getMajorType());
      fieldBuilder.setBufferLength(newField.getBufferLength());
      newBuf = byteVector.getBuffer();
      // TODO?
      oldBuf.release();
      newBuf.retain();
    }
    return newBuf;
  }

  static ArrowBuf padValues(BufferAllocator allocator, SerializedField.Builder fieldBuilder, ByteBuf oldBuf,
                            int originalTypeByteWidth, int targetTypeByteWidth) {
    if (targetTypeByteWidth <= originalTypeByteWidth) {
      throw new IllegalArgumentException("the target width must be larger than the original one. "
          + targetTypeByteWidth + " is not larger than " + originalTypeByteWidth);
    }
    if (oldBuf.readableBytes() % originalTypeByteWidth != 0) {
      throw new IllegalArgumentException("the buffer size must be a multiple of the type width. "
          + oldBuf.readableBytes() + " is not a multiple of " + originalTypeByteWidth);
    }
    int valueCount = oldBuf.readableBytes() / originalTypeByteWidth;
    int newBufferLength = targetTypeByteWidth * valueCount;
    ArrowBuf newBuf = allocator.buffer(newBufferLength);
    for (int byteIndex = 0; byteIndex < originalTypeByteWidth; byteIndex++) {
      for (int i = 0; i < valueCount; i++) {
        int oldIndex = i * originalTypeByteWidth + byteIndex;
        int newIndex = i * targetTypeByteWidth + byteIndex;
        newBuf.setByte(newIndex, oldBuf.getByte(oldIndex));
      }
    }
    newBuf.writerIndex(newBufferLength);
    fieldBuilder.setBufferLength(newBufferLength);
    oldBuf.release();
    return newBuf;
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    super.handlerRemoved(ctx);
    allocator.close();
  }
}
