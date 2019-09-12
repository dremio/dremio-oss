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
package com.dremio.exec.store.hive.exec.dfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.context.OperatorStats.WaitRecorder;

/**
 * Wrapper around FSDataOutputStream to collect IO Stats.
 */
public class FSDataOutputStreamWithStatsWrapper extends FilterFSOutputStream {
  private final OperatorStats operatorStats;

  public FSDataOutputStreamWithStatsWrapper(FSDataOutputStream os, OperatorStats operatorStats) throws IOException {
    super(os);
    this.operatorStats = operatorStats;
  }

  @Override
  public void write(int b) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.write(b);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.flush();
    }
  }

  @Override
  public void close() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      super.close();
    }
  }
}
