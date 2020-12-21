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

import static com.dremio.common.types.TypeProtos.DataMode.OPTIONAL;
import static com.dremio.common.types.TypeProtos.DataMode.REQUIRED;
import static com.dremio.common.types.TypeProtos.MinorType.BIT;
import static com.dremio.common.types.TypeProtos.MinorType.DECIMAL;
import static com.dremio.common.types.TypeProtos.MinorType.INTERVALDAY;
import static com.dremio.common.types.TypeProtos.MinorType.VARBINARY;
import static com.dremio.common.types.TypeProtos.MinorType.VARCHAR;
import static java.lang.String.format;

import java.math.BigDecimal;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.slf4j.Logger;

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.proto.UserBitShared.SerializedField;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

/**
 * This encoder ensures that the data batches sent to the user are backwards compatible i.e. if necessary, the batches
 * are patched to be compatible with the old Drill protocol. So if the client on the session is using an older record
 * batch format, this encoder is added to the Netty pipeline.
 */
class DrillBackwardsCompatibilityHandler extends BaseBackwardsCompatibilityHandler {

  /* format for DECIMAL38_SPARSE in Drill */
  public static final int NUMBER_DECIMAL_DIGITS = 6;

  DrillBackwardsCompatibilityHandler(final BufferAllocator allocator) {
    super(allocator);
  }

  @Override
  public void patch(SerializedField.Builder field, ByteBuf[] buffers, int bufferStart,
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
      // the values vectors inside the Vectors used to be called the same as their parent.
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
        NettyArrowBuf bitsBuffer = (NettyArrowBuf) buffers[bufferStart + bitsIndex];
        // the bit vectors need to be converted from nodes to bytes
        if (bitsField.getMajorType().getMinorType() != BIT
            || !"$bits$".equals(bitsField.getNamePart().getName())
            || bitsField.getMajorType().getMode() != REQUIRED) {
          throw new IllegalStateException("bit vector should be called $bits$ and have type REQUIRED BIT." +
              " Found field: " + field.build());
        }
        NettyArrowBuf newBuf = convertBitsToBytes(allocator, bitsField, bitsBuffer);
        buffers[bufferStart + bitsIndex] = newBuf;
        changed = true;

        /* patch the decimal buffer for compatibility.
         * Dremio (and Arrow) have moved to Little Endian Decimal
         * format and we need to patch the data buffer of decimal vector
         * to ensure backwards compatibility
         */
        if (minor == DECIMAL) {
          /* DecimalVector: {validityBuffer, dataBuffer} */
          final int decimalBufferIndex = 1;
          SerializedField.Builder decimalField = children.get(decimalBufferIndex);
          final NettyArrowBuf decimalBuffer = (NettyArrowBuf)buffers[bufferStart + decimalBufferIndex];
          if (decimalField.getMajorType().getMinorType() != DECIMAL
              || decimalField.getMajorType().getMode() != REQUIRED) {
            throw new IllegalStateException("Found incorrect decimal field: " + field.build());
          }
          final ByteBuf newBuffer = patchDecimal(allocator, decimalBuffer, field, decimalField);
          buffers[bufferStart + decimalBufferIndex] = newBuffer;
        }
      }
      // need to patch the value width
      if (minor == INTERVALDAY && mode == REQUIRED) {
        // IntervalDay in Drill as an extra unused 4 bytes
        // type width is wrong in ValueVectorTypes.tdd
        // padding each value
        NettyArrowBuf newBuf = padValues(allocator, field, buffers[bufferStart], 8, 12);
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

  static NettyArrowBuf convertBitsToBytes(BufferAllocator allocator,
                                          SerializedField.Builder fieldBuilder,
                                          NettyArrowBuf oldBuf) {
    int valueCount = fieldBuilder.getValueCount();
    NettyArrowBuf newBuf;
    try {
      /**
       * Create a new buffer, loop all the bit value in oldBuf, and then convert to byte value in new buffer
       */
      newBuf = NettyArrowBuf.unwrapBuffer(allocator.buffer(valueCount));
      for (int i = 0; i < valueCount; i++) {
        int byteIndex = i >> 3;
        byte b = oldBuf.getByte(byteIndex);
        int bitIndex = i & 7;
        newBuf.setByte(i, Long.bitCount((long) b & 1L << bitIndex));
      }
      newBuf.writerIndex(valueCount);
      fieldBuilder.setMajorType(Types.required(MinorType.UINT1));
      fieldBuilder.setBufferLength(valueCount);

      oldBuf.release();
    } catch (Exception e) {
      throw e;
    }
    return newBuf;
  }

  static NettyArrowBuf padValues(BufferAllocator allocator, SerializedField.Builder fieldBuilder, ByteBuf oldBuf,
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
    NettyArrowBuf newBuf = NettyArrowBuf.unwrapBuffer(allocator.buffer(newBufferLength));
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

  /**
   * Dremio has 16 byte little endian decimal format. When sending data to Drill clients
   * we map our data to 24 byte DECIMAL38SPARSE format in Drill.
   * @param dataBuffer data buffer of decimal vector
   */
  static ByteBuf patchDecimal(BufferAllocator allocator, final NettyArrowBuf dataBuffer,
                              final SerializedField.Builder decimalField, final SerializedField.Builder childDecimalField) {
    final int decimalLength = DecimalVector.TYPE_WIDTH;
    final int startPoint = dataBuffer.readerIndex();
    final int valueCount = dataBuffer.readableBytes()/decimalLength;
    final ByteBuf drillBuffer = NettyArrowBuf.unwrapBuffer(allocator.buffer(dataBuffer.readableBytes() + 8*valueCount));
    int length = 0;
    for (int i = startPoint; i < startPoint + dataBuffer.readableBytes() - 1; i+=decimalLength) {
      final BigDecimal arrowDecimal = DecimalUtility.getBigDecimalFromArrowBuf(dataBuffer.arrowBuf(), i/16,
        decimalField.getMajorType().getScale(), DecimalVector.TYPE_WIDTH);
      final int startIndex = (i == startPoint) ? i : i + length;
      DecimalHelper.getSparseFromBigDecimal(arrowDecimal, drillBuffer, startIndex, decimalField.getMajorType().getScale(), NUMBER_DECIMAL_DIGITS);
      length += 8;
    }

    TypeProtos.MajorType.Builder majorTypeBuilder = TypeProtos.MajorType.newBuilder()
      .setMinorType(MinorType.DECIMAL38SPARSE)
      .setMode(DataMode.OPTIONAL)
      .setPrecision(decimalField.getMajorType().getPrecision())
      .setScale(decimalField.getMajorType().getScale());

    decimalField.setMajorType(majorTypeBuilder.build());
    decimalField.setBufferLength(decimalField.getBufferLength() + 8*valueCount);
    drillBuffer.writerIndex(dataBuffer.readableBytes() + 8*valueCount);
    childDecimalField.setMajorType(com.dremio.common.types.Types.required(MinorType.DECIMAL38SPARSE));
    childDecimalField.setBufferLength(childDecimalField.getBufferLength() + 8*valueCount);
    dataBuffer.release();

    return drillBuffer;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }
}
