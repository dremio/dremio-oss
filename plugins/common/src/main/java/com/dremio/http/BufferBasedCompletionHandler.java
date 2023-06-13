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

package com.dremio.http;

import java.nio.ByteBuffer;

import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * Response processor for async http
 */
public class BufferBasedCompletionHandler extends AsyncCompletionHandlerBase {
  private static final Logger logger = LoggerFactory.getLogger(BufferBasedCompletionHandler.class);
  private final ByteBuf outputBuffer;
  private final int dstOffset;
  private boolean requestFailed = false;

  public BufferBasedCompletionHandler(ByteBuf outputBuffer, int dstOffset) {
    this.outputBuffer = outputBuffer;
    this.outputBuffer.writerIndex(dstOffset);
    this.dstOffset = dstOffset;
  }

  public boolean isRequestFailed() {
    return requestFailed;
  }

  protected ByteBuffer getBodyBytes() {
    return outputBuffer.nioBuffer(dstOffset, outputBuffer.writerIndex() - dstOffset);
  }

  /**
   * Reset the buffer to its original state to make it usable for a retry.
   */
  public void reset() {
    this.requestFailed = false;
    this.outputBuffer.writerIndex(this.dstOffset);
  }

  @Override
  public State onStatusReceived(HttpResponseStatus status) throws Exception {
    // The REST service provides error information as part of the response
    // body when the response code is 400 or greater.
    requestFailed = (status.getStatusCode() >= 400);
    return super.onStatusReceived(status);
  }

  @Override
  public AsyncHandler.State onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
    if (requestFailed) {
      return super.onBodyPartReceived(content);
    }
    outputBuffer.writeBytes(content.getBodyByteBuffer());
    return AsyncHandler.State.CONTINUE;
  }

  @Override
  public Response onCompleted(Response response) throws Exception {
    if (requestFailed) {
      logger.error("Error response received {} {}", response.getStatusCode(), response.getResponseBody());
      throw new RuntimeException(response.getResponseBody());
    }
    return response;
  }
}
