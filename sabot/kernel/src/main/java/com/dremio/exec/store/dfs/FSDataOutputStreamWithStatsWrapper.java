/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.hadoop.fs.FSDataOutputStream;

import com.dremio.sabot.exec.context.OperatorStats;

/**
 * Wrapper around FSDataOutputStream to collect IO Stats.
 */
public class FSDataOutputStreamWithStatsWrapper extends FSDataOutputStreamWrapper {
  private final OperatorStats operatorStats;

  public FSDataOutputStreamWithStatsWrapper(FSDataOutputStream os, OperatorStats operatorStats) throws IOException {
    super(os);
    this.operatorStats = operatorStats;
  }

  @Override
  public void write(int b) throws IOException {
    operatorStats.startWait();
    try {
      super.write(b);
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    operatorStats.startWait();
    try {
      super.write(b);
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    operatorStats.startWait();
    try {
      super.write(b, off, len);
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void flush() throws IOException {
    operatorStats.startWait();
    try {
      super.flush();
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void close() throws IOException {
    operatorStats.startWait();
    try {
      super.close();
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  @Deprecated
  public void sync() throws IOException {
    operatorStats.startWait();
    try {
      super.sync();
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void hflush() throws IOException {
    operatorStats.startWait();
    try {
      super.hflush();
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void hsync() throws IOException {
    operatorStats.startWait();
    try {
      super.hsync();
    } finally {
      operatorStats.stopWait();
    }
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException {
    operatorStats.startWait();
    try {
      super.setDropBehind(dropBehind);
    } finally {
      operatorStats.stopWait();
    }
  }
}
