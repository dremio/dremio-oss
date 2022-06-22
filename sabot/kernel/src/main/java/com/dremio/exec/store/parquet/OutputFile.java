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
package com.dremio.exec.store.parquet;

import java.io.IOException;

import org.apache.parquet.io.PositionOutputStream;

import com.dremio.exec.hadoop.FSDataOutputStreamWithStatsWrapper;
import com.dremio.exec.hadoop.FSDataOutputStreamWrapper;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorStats;

public final class OutputFile implements org.apache.parquet.io.OutputFile {
  private final FileSystem fs;
  private final Path path;
  private final org.apache.hadoop.fs.FileSystem hadoopFs;
  final OperatorStats operatorStats;


  private OutputFile(FileSystem fs, Path path, org.apache.hadoop.fs.FileSystem hadoopFs, OperatorStats operatorStats) {
    this.fs = fs;
    this.path = path;
    this.hadoopFs = hadoopFs;
    this.operatorStats = operatorStats;

  }

  public static OutputFile of(FileSystem fs, Path path, org.apache.hadoop.fs.FileSystem hadoopFs, OperatorStats operatorStats) {
    return new OutputFile(fs, path, hadoopFs, operatorStats);
  }

  private FSOutputStream getFSOutputStreamWithStats(OperatorStats operatorStats, boolean overwrite) throws IOException {
    org.apache.hadoop.fs.Path fsPath = new org.apache.hadoop.fs.Path(path.toString());
    FSOutputStream fsOutputStream = new FSDataOutputStreamWrapper(hadoopFs.create(fsPath, overwrite));
    if (operatorStats != null) {
      fsOutputStream = new FSDataOutputStreamWithStatsWrapper(fsOutputStream, operatorStats, path.toString());
    }
    return fsOutputStream;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return new PositionOutputStreamWrapper(getFSOutputStreamWithStats(operatorStats, false));
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return new PositionOutputStreamWrapper(getFSOutputStreamWithStats(operatorStats, true));
    }
  }

  @Override
  public boolean supportsBlockSize() {
    return defaultBlockSize() > 0;
  }

  @Override
  public long defaultBlockSize() {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public String toString() {
    return path.toString();
  }

  private static final class PositionOutputStreamWrapper extends PositionOutputStream {
    private final FSOutputStream os;

    public PositionOutputStreamWrapper(FSOutputStream os) {
      this.os = os;
    }

    @Override
    public long getPos() throws IOException {
      return os.getPosition();
    }

    @Override
    public void write(int b) throws IOException {
      os.write(b);

    }

    @Override
    public void write(byte[] b) throws IOException {
      os.write(b);
    }


    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      os.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      os.flush();
    }

    @Override
    public void close() throws IOException {
      os.close();
    }
  }


}
