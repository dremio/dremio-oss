/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;

import com.dremio.sabot.exec.context.OperatorStats;


/**
 * Wrapper around FSDataInputStream to collect IO Stats.
 */
class FSDataInputStreamWithStatsWrapper extends FSDataInputStreamWrapper {
  private final OperatorStats operatorStats;

  public FSDataInputStreamWithStatsWrapper(FSDataInputStream in, OperatorStats operatorStats) throws IOException {
    super(in, new WrappedInputStream(in, operatorStats));
    this.operatorStats = operatorStats;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    operatorStats.startWait();
    try {
      return super.read(position, buffer, offset, length);
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    operatorStats.startWait();
    try {
      super.readFully(position, buffer, offset, length);
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    operatorStats.startWait();
    try {
      super.readFully(position, buffer);
    } finally {
      operatorStats.stopWait();
    }
  }


  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts) throws IOException, UnsupportedOperationException {
    operatorStats.startWait();
    try {
      return super.read(bufferPool, maxLength, opts);
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public synchronized void seek(long desired) throws IOException {
    operatorStats.startWait();
    try {
      super.seek(desired);
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public long skip(long n) throws IOException {
    operatorStats.startWait();
    try {
      return super.skip(n);
    } finally {
      operatorStats.stopWait();
    }
  }

  /**
   * We need to wrap the FSDataInputStream inside a InputStream, because read() method in InputStream is
   * overridden in FilterInputStream (super class of FSDataInputStream) as final, so we can not override in
   * FSDataInputStreamWrapper.
   */
  private static class WrappedInputStream extends FSDataInputStreamWrapper.WrappedInputStream {
    private final OperatorStats operatorStats;

    WrappedInputStream(FSDataInputStream is, OperatorStats operatorStats) {
      super(is);
      this.operatorStats = operatorStats;
    }

    /*
     * Most of the read are going to be block reads which use {@link #read(byte[], int,
     * int)}. So not adding stats for single byte reads.
     */
//    @Override
//    public int read() throws IOException {
//      return super.read();
//    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      operatorStats.startWait();
      try {
        return super.read(b, off, len);
      } finally {
        operatorStats.stopWait();
      }
    }

    @Override
    public int read(byte[] b) throws IOException {
      operatorStats.startWait();
      try {
        return super.read(b);
      } finally {
        operatorStats.stopWait();
      }
    }
  }
}
