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
package com.dremio.exec.rpc;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.OutOfMemoryOrResourceExceptionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.memory.MemoryDebugInfo;
import com.dremio.exec.proto.GeneralRPCProtos.CompleteRpcMessage;
import com.dremio.exec.proto.GeneralRPCProtos.RpcHeader;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.PromisingMessageToMessageEncoder;
import java.io.OutputStream;
import java.util.List;
import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

/** Converts an RPCMessage into wire format. */
class RpcEncoder extends PromisingMessageToMessageEncoder<OutboundRpcMessage> {
  final org.slf4j.Logger logger;

  static final int HEADER_TAG =
      makeTag(CompleteRpcMessage.HEADER_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  static final int PROTOBUF_BODY_TAG =
      makeTag(CompleteRpcMessage.PROTOBUF_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  static final int RAW_BODY_TAG =
      makeTag(CompleteRpcMessage.RAW_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  static final int HEADER_TAG_LENGTH = getRawVarintSize(HEADER_TAG);
  static final int PROTOBUF_BODY_TAG_LENGTH = getRawVarintSize(PROTOBUF_BODY_TAG);
  static final int RAW_BODY_TAG_LENGTH = getRawVarintSize(RAW_BODY_TAG);

  public RpcEncoder(String name) {
    super(OutboundRpcMessage.class);
    this.logger =
        org.slf4j.LoggerFactory.getLogger(RpcEncoder.class.getCanonicalName() + "-" + name);
  }

  @Override
  protected void encode(
      ChannelHandlerContext ctx, OutboundRpcMessage msg, List<Object> out, ChannelPromise promise)
      throws Exception {
    if (RpcConstants.EXTRA_DEBUGGING) {
      logger.debug("Rpc Encoder called with msg {}", msg);
    }

    if (!ctx.channel().isOpen()) {
      // output.add(ctx.alloc().buffer(0));
      logger.debug("Channel closed, skipping encode.");
      msg.release();
      return;
    }

    try {
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Encoding outbound message {}", msg);
      }
      // first we build the RpcHeader
      final RpcHeader header =
          RpcHeader.newBuilder()
              .setMode(msg.mode)
              .setCoordinationId(msg.coordinationId)
              .setRpcType(msg.rpcType)
              .build();

      // figure out the full length
      final int headerLength = header.getSerializedSize();
      final int protoBodyLength = msg.pBody.getSerializedSize();
      final int rawBodyLength = msg.getRawBodySize();
      final int withoutRawLength =
          HEADER_TAG_LENGTH
              + getRawVarintSize(headerLength)
              + headerLength
              + PROTOBUF_BODY_TAG_LENGTH
              + getRawVarintSize(protoBodyLength)
              + protoBodyLength;

      final int fullLength;
      if (rawBodyLength > 0) {
        fullLength =
            withoutRawLength
                + (RAW_BODY_TAG_LENGTH + getRawVarintSize(rawBodyLength) + rawBodyLength);
      } else {
        fullLength = withoutRawLength;
      }

      // use a direct buffer if this message includes a raw body. Otherwise use
      // the heap so we can avoid off-heap memory allocation failures causing
      // command message instability.
      final ByteBuf withoutRawMessage;
      if (rawBodyLength > 0) {
        try {
          withoutRawMessage = ctx.alloc().buffer(fullLength + 5);
        } catch (OutOfMemoryException | OutOfMemoryError ex) {
          msg.release();
          String oomDetails = "Out of memory while encoding data. ";
          UserException.Builder uexBuilder = UserException.memoryError(ex);
          if (ErrorHelper.isDirectMemoryException(ex)) {
            ByteBufAllocator byteBufAllocator = ctx.alloc();
            if (byteBufAllocator instanceof ArrowByteBufAllocator) {
              BufferAllocator bufferAllocator = ((ArrowByteBufAllocator) byteBufAllocator).unwrap();
              oomDetails =
                  oomDetails
                      + (ex instanceof OutOfMemoryException
                          ? MemoryDebugInfo.getDetailsOnAllocationFailure(
                              (OutOfMemoryException) ex, bufferAllocator)
                          : MemoryDebugInfo.getSummaryFromRoot(bufferAllocator));
            }
            uexBuilder.setAdditionalExceptionContext(
                new OutOfMemoryOrResourceExceptionContext(
                    OutOfMemoryOrResourceExceptionContext.MemoryType.DIRECT_MEMORY, oomDetails));
          } else {
            uexBuilder.setAdditionalExceptionContext(
                new OutOfMemoryOrResourceExceptionContext(
                    OutOfMemoryOrResourceExceptionContext.MemoryType.HEAP_MEMORY, oomDetails));
          }
          promise.setFailure(uexBuilder.buildSilently());
          return;
        }
      } else {
        withoutRawMessage = UnpooledByteBufAllocator.DEFAULT.heapBuffer(fullLength + 5);
      }

      final OutputStream os = new ByteBufOutputStream(withoutRawMessage);
      final CodedOutputStream cos = CodedOutputStream.newInstance(os);

      // write full length first (this is length delimited stream).
      cos.writeRawVarint32(fullLength);

      // write header
      cos.writeRawVarint32(HEADER_TAG);
      cos.writeRawVarint32(headerLength);
      header.writeTo(cos);

      // write protobuf body length and body
      cos.writeRawVarint32(PROTOBUF_BODY_TAG);
      cos.writeRawVarint32(protoBodyLength);
      msg.pBody.writeTo(cos);

      // if exists, write data body and tag.
      if (msg.getRawBodySize() > 0) {
        if (RpcConstants.EXTRA_DEBUGGING) {
          logger.debug("Writing raw body of size {}", msg.getRawBodySize());
        }

        cos.writeRawVarint32(RAW_BODY_TAG);
        cos.writeRawVarint32(rawBodyLength);
        cos.flush(); // need to flush so that dbody goes after if cos is caching.

        CompositeByteBuf cbb =
            new CompositeByteBuf(withoutRawMessage.alloc(), true, msg.dBodies.length + 1);
        cbb.addComponent(withoutRawMessage);
        int bufLength = withoutRawMessage.readableBytes();
        for (ByteBuf b : msg.dBodies) {
          cbb.addComponent(b);
          bufLength += b.readableBytes();
        }
        cbb.writerIndex(bufLength);
        out.add(cbb);
      } else {
        cos.flush();
        out.add(withoutRawMessage);
      }

      if (RpcConstants.SOME_DEBUGGING) {
        logger.debug(
            "Wrote message length {}:{} bytes (head:body).  Message: " + msg,
            getRawVarintSize(fullLength),
            fullLength);
      }
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Sent message.  Ending writer index was {}.", withoutRawMessage.writerIndex());
      }
    } finally {
      // FIXME: why is the finally block commented out?
      // make sure to release Rpc Messages underlying byte buffers.
      // msg.release();
    }
  }

  /**
   * Makes a tag value given a field number and wire type, copied from WireFormat since it isn't
   * public.
   */
  static int makeTag(final int fieldNumber, final int wireType) {
    return (fieldNumber << 3) | wireType;
  }

  public static int getRawVarintSize(int value) {
    int count = 0;
    while (true) {
      if ((value & ~0x7F) == 0) {
        count++;
        return count;
      } else {
        count++;
        value >>>= 7;
      }
    }
  }
}
