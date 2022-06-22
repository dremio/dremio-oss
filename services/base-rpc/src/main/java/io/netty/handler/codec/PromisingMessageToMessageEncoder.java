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
package io.netty.handler.codec;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.TypeParameterMatcher;

/**
 * A copy of netty's message to message encoder that also passes in the promise for the message.
 */
public abstract class PromisingMessageToMessageEncoder<I> extends ChannelOutboundHandlerAdapter {

  private final TypeParameterMatcher matcher;

  /**
   * Create a new instance
   *
   * @param outboundMessageType   The type of messages to match and so encode
   */
  protected PromisingMessageToMessageEncoder(Class<? extends I> outboundMessageType) {
      matcher = TypeParameterMatcher.get(outboundMessageType);
  }

  /**
   * Returns {@code true} if the given message should be handled. If {@code false} it will be passed to the next
   * {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
   */
  public boolean acceptOutboundMessage(Object msg) throws Exception {
      return matcher.match(msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      CodecOutputList out = null;
      try {
          if (acceptOutboundMessage(msg)) {
              out = CodecOutputList.newInstance();
              @SuppressWarnings("unchecked")
              I cast = (I) msg;
              try {
                  encode(ctx, cast, out, promise);
              } finally {
                  ReferenceCountUtil.release(cast);
              }

              if (out.isEmpty()) {
                  out.recycle();
                  out = null;
                  return;
              }
          } else {
              ctx.write(msg, promise);
          }
      } catch (EncoderException e) {
          throw e;
      } catch (Throwable t) {
          throw new EncoderException(t);
      } finally {
          if (out != null) {
              final int sizeMinusOne = out.size() - 1;
              if (sizeMinusOne == 0) {
                  ctx.write(out.get(0), promise);
              } else if (sizeMinusOne > 0) {
                  // Check if we can use a voidPromise for our extra writes to reduce GC-Pressure
                  // See https://github.com/netty/netty/issues/2525
                  ChannelPromise voidPromise = ctx.voidPromise();
                  boolean isVoidPromise = promise == voidPromise;
                  for (int i = 0; i < sizeMinusOne; i ++) {
                      ChannelPromise p;
                      if (isVoidPromise) {
                          p = voidPromise;
                      } else {
                          p = ctx.newPromise();
                      }
                      ctx.write(out.getUnsafe(i), p);
                  }
                  ctx.write(out.getUnsafe(sizeMinusOne), promise);
              }
              out.recycle();
          }
      }
  }

  /**
   * Encode from one message to an other. This method will be called for each written message that can be handled
   * by this encoder.
   *
   * @param ctx           the {@link ChannelHandlerContext} which this {@link MessageToMessageEncoder} belongs to
   * @param msg           the message to encode to an other one
   * @param out           the {@link List} into which the encoded msg should be added
   *                      needs to do some kind of aggragation
   * @throws Exception    is thrown if an error accour
   */
  protected abstract void encode(ChannelHandlerContext ctx, I msg, List<Object> out, ChannelPromise promise) throws Exception;

}
