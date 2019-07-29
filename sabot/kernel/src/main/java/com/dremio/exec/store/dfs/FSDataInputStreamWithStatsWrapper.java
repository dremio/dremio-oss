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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;

import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.context.OperatorStats.WaitRecorder;


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
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.read(position, buffer, offset, length);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.readFully(position, buffer, offset, length);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.readFully(position, buffer);
    }
  }


  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts) throws IOException, UnsupportedOperationException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.read(bufferPool, maxLength, opts);
    }
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.read(buf);
    }
  }

  @Override
  public synchronized void seek(long desired) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.seek(desired);
    }
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.seekToNewSource(targetPos);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.skip(n);
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
      try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        return super.read(b, off, len);
      }
    }

    @Override
    public int read(byte[] b) throws IOException {
      try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        return super.read(b);
      }
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        return super.read(position, buffer, offset, length);
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        super.readFully(position, buffer, offset, length);
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        super.readFully(position, buffer);
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
        return super.seekToNewSource(targetPos);
      }
    }
  }
}
