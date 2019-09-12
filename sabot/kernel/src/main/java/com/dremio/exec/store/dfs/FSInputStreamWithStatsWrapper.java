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

import com.dremio.io.FSInputStream;
import com.dremio.io.FilterFSInputStream;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.context.OperatorStats.WaitRecorder;


/**
 * Wrapper around FSDataInputStream to collect IO Stats.
 */
public class FSInputStreamWithStatsWrapper extends FilterFSInputStream {
  private final OperatorStats operatorStats;

  public FSInputStreamWithStatsWrapper(FSInputStream in, OperatorStats operatorStats) throws IOException {
    super(in);
    this.operatorStats = operatorStats;
  }

  @Override
  public int read() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.read();
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.read(b);
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.read(b, off, len);
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.read(dst);
    }
  }

  @Override
  public void setPosition(long position) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.setPosition(position);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return super.skip(n);
    }
  }

  @Override
  public void close() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.close();
    }
  }
}
