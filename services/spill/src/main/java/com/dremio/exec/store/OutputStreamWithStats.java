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
package com.dremio.exec.store;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

/**
 * An output stream that keeps track of write and close time, as well as bytes.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class OutputStreamWithStats extends FilterOutputStream {

  /**
   * Write stopwatch
   */
  protected final Stopwatch write = Stopwatch.createUnstarted();

  /**
   * Read stopwatch
   */
  protected final Stopwatch close = Stopwatch.createUnstarted();

  /**
   * Number of bytes read.
   */
  protected long bytes;

  public OutputStreamWithStats(OutputStream out) {
    super(out);
  }

  @Override
  public void write(int b) throws IOException {
    write.start();
    try {
    out.write(b);
    } finally {
      write.stop();
    }
    bytes++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    write.start();
    try {
      out.write(b, off, len);
    } finally {
      write.stop();
    }
    bytes += len;
  }

  @Override
  public void close() throws IOException {
    close.start();
    try {
      out.close();
    } finally {
      close.stop();
    }
  }

  public long getWriteNanos() {
    return write.elapsed(TimeUnit.NANOSECONDS);
  }

  public long getWriteBytes() {
    return bytes;
  }

  public long getCloseNanos() {
    return close.elapsed(TimeUnit.NANOSECONDS);
  }

}
