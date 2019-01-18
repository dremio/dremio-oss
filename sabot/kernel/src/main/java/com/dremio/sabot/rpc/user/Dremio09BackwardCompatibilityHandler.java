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
package com.dremio.sabot.rpc.user;

import static com.dremio.common.types.TypeProtos.DataMode.OPTIONAL;
import static com.dremio.common.types.TypeProtos.DataMode.REQUIRED;
import static com.dremio.common.types.TypeProtos.MinorType.DECIMAL;
import static java.lang.String.format;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.proto.UserBitShared.SerializedField;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

/**
 * If Dremio Jdbc client on the session is using record batch format older than 1.4,
 * we use this encoder to patch the decimal buffers sent by Dremio server to the client.
 * Dremio has moved to LE decimal format from version 1.4 onwards so this encoder
 * ensures backward compatibility with older clients.
 */
class Dremio09BackwardCompatibilityHandler extends BaseBackwardsCompatibilityHandler {
  private static final Logger logger = LoggerFactory.getLogger(Dremio09BackwardCompatibilityHandler.class);

  Dremio09BackwardCompatibilityHandler (final BufferAllocator allocator) {
    super(allocator);
  }

  /* patch the decimal buffers */
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

    List<SerializedField.Builder> children = field.getChildBuilderList();

    /* don't patch when there is no data */
    if (field.getValueCount() != 0) {
      if (mode == OPTIONAL) {
        while (buffers[bufferStart].readableBytes() == 0) {
          ++bufferStart;
          --buffersLength;
        }
        /* patch the decimal buffer for compatibility.
         * Dremio (and Arrow) have moved to Little Endian Decimal
         * format and we need to patch the data buffer of decimal vector
         * to ensure backwards compatibility
         */
        if (minor == DECIMAL) {
          /* DecimalVector: {validityBuffer, dataBuffer} */
          final int decimalBufferIndex = 1;
          SerializedField.Builder decimalField = children.get(decimalBufferIndex);
          final ArrowBuf decimalBuffer = (ArrowBuf)buffers[bufferStart + decimalBufferIndex];
          if (decimalField.getMajorType().getMinorType() != DECIMAL
            || decimalField.getMajorType().getMode() != REQUIRED) {
            throw new IllegalStateException("Found incorrect decimal field: " + field.build());
          }
          patchDecimal(decimalBuffer);
          changed = true;
        }
      }
    }

    int bufferLength = 0;
    for (int i = 0; i < buffersLength; i++) {
      ByteBuf buffer = buffers[i + bufferStart];
      bufferLength += buffer.readableBytes();
    }

    /* we only swap bytes, so length should not change */
    if (field.getBufferLength() != bufferLength) {
      throw new IllegalStateException("Length of data in buffer should not have changed");
    }

    if (logger.isDebugEnabled() && changed) {
      logger.debug(format("%sAFTER PATCH: buffers %s for field %s.%s: %s %s expecting %s", indent,
        sizesString(buffers, bufferStart, buffersLength), parentName, name, mode, minor, field.getBufferLength()));
    }
  }

  /**
   * swap the bytes in place to get the BE byte order in NullableDecimalVector
   * @param dataBuffer data buffer of decimal vector
   */
  static void patchDecimal(final ArrowBuf dataBuffer) {
    final int decimalLength = DecimalVector.TYPE_WIDTH;
    int startPoint = dataBuffer.readerIndex();
    final int valueCount = dataBuffer.readableBytes()/decimalLength;
    for (int i = 0; i < valueCount; i++) {
      for (int j = startPoint, k = startPoint + decimalLength - 1; j < k; j++, k--) {
        final byte firstByte = dataBuffer.getByte(j);
        final byte lastByte = dataBuffer.getByte(k);
        dataBuffer.setByte(j, lastByte);
        dataBuffer.setByte(k, firstByte);
      }
      startPoint += decimalLength;
    }
  }

  @Override
  public Logger getLogger() {
    return logger;
  }
}
