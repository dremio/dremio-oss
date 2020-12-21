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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.NettyArrowBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.memory.MemoryDebugInfo;
import com.dremio.exec.proto.GeneralRPCProtos.RpcHeader;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.google.protobuf.CodedInputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

/**
 * Frame decoder that decodes the rpc header of each message as it is available.
 * Expected to only allocate on heap for message only items. Can hit OOM for
 * message + trailing bytes messages but simply responds to sender that message
 * cannot be accepted due to out of memory. Should be resilient to all off-heap
 * OOM situations.
 */
public class MessageDecoder extends ByteToMessageDecoder {
  private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  private BufferAllocator allocator;
  private final AtomicLong messageCounter = new AtomicLong();

  public MessageDecoder(BufferAllocator allocator) {
    super();
    setCumulator(COMPOSITE_CUMULATOR);
    this.allocator = allocator;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (!ctx.channel().isOpen()) {
      if (in.readableBytes() > 0) {
        logger.info("Channel is closed, discarding remaining {} byte(s) in buffer.", in.readableBytes());
      }
      in.skipBytes(in.readableBytes());
      return;
    }

    in.markReaderIndex();

    /**
     *  a variable-width message length can be up to five bytes in length. read bytes until we have a length.
     */
    final byte[] buf = new byte[5];
    int length = 0;
    for (int i = 0; i < buf.length; i++) {
      if (!in.isReadable()) {
        in.resetReaderIndex();
        return;
      }

      buf[i] = in.readByte();
      if (buf[i] >= 0) {

        length = CodedInputStream.newInstance(buf, 0, i + 1).readRawVarint32();

        if (length < 0) {
          throw new CorruptedFrameException("negative length: " + length);
        }
        if (length == 0) {
          throw new CorruptedFrameException("Received a message of length 0.");
        }

        if (in.readableBytes() < length) {
          in.resetReaderIndex();
          return;
        } else {
          // complete message in buffer.
          break;
        }
      }
    }

    final ByteBuf frame = in.slice(in.readerIndex(), length);
    try {
      final InboundRpcMessage message = decodeMessage(ctx, frame, length);
      if (message != null) {
        out.add(message);
      }
    } finally {
      in.skipBytes(length);
    }
  }

  /**
   * We decode the message in the same context as the length decoding to better
   * manage running out of memory. We decode the rpc header using heap memory
   * (expected to always be available) so that we can propagate a error message
   * with correct coordination id to sender rather than failing the channel.
   *
   * @param ctx The channel context.
   * @param frame The Frame of the message we're processing.
   * @param length The length of the frame.
   * @throws Exception Code should only throw corrupt channel messages, causing the channel to close.
   */
  private InboundRpcMessage decodeMessage(final ChannelHandlerContext ctx, final ByteBuf frame, final int length) throws Exception {
    // now, we know the entire message is in the buffer and the buffer is constrained to this message. Additionally,
    // this process should avoid reading beyond the end of this buffer so we inform the ByteBufInputStream to throw an
    // exception if be go beyond readable bytes (as opposed to blocking).
    final ByteBufInputStream is = new ByteBufInputStream(frame, length);

    // read the rpc header, saved in delimited format.
    checkTag(is, RpcEncoder.HEADER_TAG);
    final RpcHeader header = RpcHeader.parseDelimitedFrom(is);


    // read the protobuf body into a buffer.
    checkTag(is, RpcEncoder.PROTOBUF_BODY_TAG);
    final int pBodyLength = readRawVarint32(is);

    final byte[] pBody = new byte[pBodyLength];
    frame.readBytes(pBody);

    ByteBuf dBody = null;

    // read the data body.
    if (frame.readableBytes() > 0) {

      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Reading raw body, buffer has {} bytes available.", frame.readableBytes());
      }

      checkTag(is, RpcEncoder.RAW_BODY_TAG);
      final int dBodyLength = readRawVarint32(is);

      if (frame.readableBytes() != dBodyLength) {
        throw new CorruptedFrameException(String.format("Expected to receive a raw body of %d bytes but received a buffer with %d bytes.", dBodyLength, frame.readableBytes()));
      }


      try {
        dBody = NettyArrowBuf.unwrapBuffer(allocator.buffer(dBodyLength));
        // need to make buffer copy, otherwise netty will try to refill this buffer if we move the readerIndex forward...
        // TODO: Can we avoid this copy?
        dBody.writeBytes(frame.nioBuffer(frame.readerIndex(), dBodyLength));

      } catch (OutOfMemoryException e) {
        sendOutOfMemory(e, ctx, header.getCoordinationId());
        return null;
      }

      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Read raw body of length ", dBodyLength);
      }

    }else{
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("No need to read raw body, no readable bytes left.");
      }
    }

    InboundRpcMessage m = new InboundRpcMessage(header.getMode(), header.getRpcType(), header.getCoordinationId(),
        pBody, dBody);
    return m;
  }

  private void sendOutOfMemory(OutOfMemoryException e, final ChannelHandlerContext ctx, int coordinationId){
    final UserException uex = UserException.memoryError(e)
        .message("Out of memory while receiving data.")
        .addContext(MemoryDebugInfo.getDetailsOnAllocationFailure(e, allocator))
        .build(logger);

    final OutboundRpcMessage outMessage = new OutboundRpcMessage(
        RpcMode.RESPONSE_FAILURE,
        0,
        coordinationId,
        uex.getOrCreatePBError(false)
        );

    if (RpcConstants.EXTRA_DEBUGGING) {
      logger.debug("Adding message to outbound buffer. {}", outMessage);
    }

    ChannelFuture future = ctx.writeAndFlush(outMessage);
    // if we were unable to report back the failure make sure we close the channel otherwise we may cause the sender
    // to block undefinitely waiting for an ACK on this message
    future.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

private void checkTag(ByteBufInputStream is, int expectedTag) throws IOException {
  int actualTag = readRawVarint32(is);
  if (actualTag != expectedTag) {
    throw new CorruptedFrameException(String.format("Expected to read a tag of %d but actually received a value of %d.  Happened after reading %d message.", expectedTag, actualTag, messageCounter.get()));
  }
}

// Taken from CodedInputStream and modified to enable ByteBufInterface.
public static int readRawVarint32(ByteBufInputStream is) throws IOException {
  byte tmp = is.readByte();
  if (tmp >= 0) {
    return tmp;
  }
  int result = tmp & 0x7f;
  if ((tmp = is.readByte()) >= 0) {
    result |= tmp << 7;
  } else {
    result |= (tmp & 0x7f) << 7;
    if ((tmp = is.readByte()) >= 0) {
      result |= tmp << 14;
    } else {
      result |= (tmp & 0x7f) << 14;
      if ((tmp = is.readByte()) >= 0) {
        result |= tmp << 21;
      } else {
        result |= (tmp & 0x7f) << 21;
        result |= (tmp = is.readByte()) << 28;
        if (tmp < 0) {
          // Discard upper 32 bits.
          for (int i = 0; i < 5; i++) {
            if (is.readByte() >= 0) {
              return result;
            }
          }
          throw new CorruptedFrameException("Encountered a malformed varint.");
        }
      }
    }
  }
  return result;
}


  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.fireChannelReadComplete();
  }

}
