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

import com.google.common.base.Stopwatch;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/** An input stream that keeps track of read time and read bytes. */
public class InputStreamWithStats extends FilterInputStream {

  private final Stopwatch read = Stopwatch.createUnstarted();
  private long bytes;

  public InputStreamWithStats(InputStream in) throws FileNotFoundException {
    super(in);
  }

  @Override
  public int read() throws IOException {
    read.start();
    try {
      bytes++;
      return in.read();
    } finally {
      read.stop();
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    read.start();
    try {
      int bytesRead = in.read(b, off, len);
      bytes += bytesRead;
      return bytesRead;
    } finally {
      read.stop();
    }
  }

  @Override
  public long skip(long n) throws IOException {
    read.start();
    try {
      return in.skip(n);
    } finally {
      read.stop();
    }
  }

  public long getReadNanos() {
    return read.elapsed(TimeUnit.NANOSECONDS);
  }

  public long getReadBytes() {
    return bytes;
  }
}
