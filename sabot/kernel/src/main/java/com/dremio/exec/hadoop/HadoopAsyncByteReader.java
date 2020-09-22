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
package com.dremio.exec.hadoop;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.Path;

import io.netty.buffer.ByteBuf;

/**
 * Async wrapper over the hadoop sync APIs.
 */
public class HadoopAsyncByteReader implements AsyncByteReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HadoopAsyncByteReader.class);
  private static final ExecutorService threadPool = Executors.newCachedThreadPool(new NamedThreadFactory("hadoop-read-"));

  private final Path path;
  private final FSInputStream inputStream;
  private final String threadName;

  public HadoopAsyncByteReader(final Path path, final FSInputStream inputStream) {
    this.path = path;
    this.inputStream = inputStream;
    this.threadName = Thread.currentThread().getName();
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dstBuf, int dstOffset, int len) {

    return CompletableFuture.runAsync(() -> {
      try {
        readFully(inputStream, offset, dstBuf.nioBuffer(dstOffset, len));
        logger.debug("[{}] Completed request for path {} for offset {} len {}", threadName, path, offset, len);
      } catch (Exception e) {
        logger.error("[{}] Failed request for path {} for offset {} len {}", threadName, path, offset, len, e);
        throw new CompletionException(e);
      }
    }, threadPool);
  }

  private void readFully(FSInputStream in, long offset, ByteBuffer buf) throws Exception {
    int remainingBytes = buf.remaining();
    int bytesRead;
    do {
      bytesRead = in.read(offset, buf);
      if (bytesRead > 0) {
        offset += bytesRead;
        remainingBytes -= bytesRead;
      }
    } while (remainingBytes > 0 && bytesRead >= 0);
  }

  @Override
  public void close() throws Exception {
    inputStream.close();
  }
}
