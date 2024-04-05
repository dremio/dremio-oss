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
package com.dremio.service.grpc;

import io.grpc.Drainable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Custom Stream that enables setting the grpc buffers directly from this stream. */
public class DrainableByteBufInputStream extends ByteBufInputStream implements Drainable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DrainableByteBufInputStream.class);
  private final ByteBuf buf;
  private volatile boolean isClosed = false;

  public DrainableByteBufInputStream(ByteBuf buffer) {
    super(buffer, buffer.readableBytes(), true);
    this.buf = buffer;
  }

  @Override
  public int drainTo(OutputStream target) throws IOException {
    int size = this.buf.readableBytes();
    if (!ByteBufToStreamCopier.add(this.buf, target)) {
      logger.debug("could not do the fast path copy");
      this.buf.getBytes(0, target, size);
    }

    return size;
  }

  @Override
  public void close() {
    if (!isClosed) {
      if (this.buf.refCnt() > 0) {
        this.buf.release();
      }
      isClosed = true;
    }
  }

  public boolean isClosed() {
    return isClosed;
  }
}
