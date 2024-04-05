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
import static com.dremio.common.types.TypeProtos.MinorType.LIST;
import static com.dremio.common.types.TypeProtos.MinorType.MAP;

import com.dremio.common.types.TypeProtos;
import com.dremio.exec.proto.UserBitShared;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If Dremio client on the session is using record batch format older than 23.0, we use this encoder
 * to patch the map vectors sent by Dremio server to the client.
 */
class Dremio14BackwardCompatibilityHandler extends BaseBackwardsCompatibilityHandler {
  private static final Logger logger =
      LoggerFactory.getLogger(Dremio14BackwardCompatibilityHandler.class);

  public Dremio14BackwardCompatibilityHandler(BufferAllocator bcAllocator) {
    super(bcAllocator);
  }

  @Override
  public void patch(
      UserBitShared.SerializedField.Builder field,
      ByteBuf[] buffers,
      int bufferStart,
      int buffersLength,
      String parentName,
      String indent) {
    TypeProtos.DataMode mode = field.getMajorType().getMode();
    TypeProtos.MinorType minor = field.getMajorType().getMinorType();
    String name = field.getNamePart().getName();
    boolean changed = false;
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} BEFORE PATCH: buffers {} for field {}.{}: {} {} expecting {}",
          indent,
          sizesString(buffers, bufferStart, buffersLength),
          parentName,
          name,
          mode,
          minor,
          field.getBufferLength());
    }
    if (field.getValueCount() != 0 && mode == OPTIONAL) {
      while (buffers[bufferStart].readableBytes() == 0) {
        ++bufferStart;
        --buffersLength;
      }
    }
    if (mode == OPTIONAL && minor == MAP) {
      field.getMajorTypeBuilder().setMinorType(LIST);
      changed = true;
    }
    if (logger.isDebugEnabled() && changed) {
      logger.debug(
          "{} AFTER PATCH: buffers {} for field {}.{}: {} {} expecting {}",
          indent,
          sizesString(buffers, bufferStart, buffersLength),
          parentName,
          name,
          mode,
          minor,
          field.getBufferLength());
    }
  }
}
