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
import java.util.concurrent.TimeUnit;

import com.dremio.io.FSInputStream;
import com.dremio.io.FilterFSInputStream;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.context.OperatorStats.WaitRecorder;
import com.google.common.base.Stopwatch;


/**
 * Wrapper around FSInputStream to collect IO Stats.
 */
public class FSInputStreamWithStatsWrapper extends FilterFSInputStream {
  private final OperatorStats operatorStats;
  private final OperatorStats waitStats;
  private final String filePath;

  public FSInputStreamWithStatsWrapper(FSInputStream in, OperatorStats operatorStats, boolean recordWaitTimes, String filePath) throws IOException {
    super(in);
    this.operatorStats = operatorStats;
    this.waitStats = recordWaitTimes ? operatorStats : null;
    this.filePath = filePath;
    operatorStats.createReadIOStats();
  }

  @Override
  public int read() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(waitStats)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      int n = super.read();
      stopwatch.stop();
      operatorStats.updateReadIOStats(stopwatch.elapsed(TimeUnit.NANOSECONDS), filePath, n == -1 ? 0 : 1, getPosition());
      return n;
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    try (WaitRecorder waitRecorder = OperatorStats.getWaitRecorder(waitStats)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      int n = super.read(b);
      stopwatch.stop();
      operatorStats.updateReadIOStats(stopwatch.elapsed(TimeUnit.NANOSECONDS), filePath, n, getPosition());
      return n;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(waitStats)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      int n = super.read(b, off, len);
      stopwatch.stop();
      operatorStats.updateReadIOStats(stopwatch.elapsed(TimeUnit.NANOSECONDS), filePath, n, getPosition());
      return n;
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(waitStats)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      int n = super.read(dst);
      stopwatch.stop();
      operatorStats.updateReadIOStats(stopwatch.elapsed(TimeUnit.NANOSECONDS), filePath, n, getPosition());
      return n;
    }
  }

  @Override
  public int read(long position, ByteBuffer dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(waitStats)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      int n = super.read(position, dst);
      stopwatch.stop();
      operatorStats.updateReadIOStats(stopwatch.elapsed(TimeUnit.NANOSECONDS), filePath,  n, position);
      return n;
    }
  }

  @Override
  public void setPosition(long position) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(waitStats)) {
      super.setPosition(position);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(waitStats)) {
      return super.skip(n);
    }
  }

  @Override
  public void close() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(waitStats)) {
      super.close();
    }
  }
}
