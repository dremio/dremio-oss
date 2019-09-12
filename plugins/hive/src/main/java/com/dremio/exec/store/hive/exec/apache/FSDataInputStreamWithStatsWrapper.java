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
package com.dremio.exec.store.hive.exec.apache;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;

import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.context.OperatorStats.WaitRecorder;
import com.dremio.sabot.op.scan.ScanOperator;


/**
 * Wrapper around FSDataInputStream to collect IO Stats.
 */
class FSDataInputStreamWithStatsWrapper extends FSDataInputStreamWrapper {
  private static final Class<?> HDFS_DATA_INPUT_STREAM_CLASS = getClass("org.apache.hadoop.hdfs.client.HdfsDataInputStream");
  private static final Class<?> DFS_INPUT_STREAM_CLASS = getClass("org.apache.hadoop.hdfs.DFSInputStream");
  private static final Class<?> READ_STATISTICS_CLASS = getClass("org.apache.hadoop.hdfs.DFSInputStream$ReadStatistics");

  private static final Method HDFS_DATA_INPUT_STREAM_READ_STATISTICS_METHOD = getClassMethod(HDFS_DATA_INPUT_STREAM_CLASS, "getReadStatistics");
  private static final Method DFS_INPUT_STREAM_READ_STATISTICS_METHOD = getClassMethod(DFS_INPUT_STREAM_CLASS, "getReadStatistics");

  private static final Method GET_TOTAL_BYTES_READ_METHOD = getClassMethod(READ_STATISTICS_CLASS,"getTotalBytesRead");
  private static final Method GET_TOTAL_LOCAL_BYTES_READ_METHOD = getClassMethod(READ_STATISTICS_CLASS,"getTotalLocalBytesRead");
  private static final Method GET_TOTAL_SHORT_CIRCUIT_BYTES_READ_METHOD = getClassMethod(READ_STATISTICS_CLASS,"getTotalShortCircuitBytesRead");

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

  @Override
  public void close() throws IOException {
    try {
      final FSDataInputStream is = getUnderlyingStream();
      final Object inputStream;
      final Method readStatsMethod;
      if (isInstanceOfHdfsDataInputStream(is)) {
        inputStream = is;
        readStatsMethod = HDFS_DATA_INPUT_STREAM_READ_STATISTICS_METHOD;
      } else if (isInstanceOfHdfsDataInputStream(is.getWrappedStream())) {
        inputStream = is.getWrappedStream();
        readStatsMethod = HDFS_DATA_INPUT_STREAM_READ_STATISTICS_METHOD;
      } else if (isInstanceOfDFSInputStream(is.getWrappedStream().getClass())) {
        inputStream = is.getWrappedStream();
        readStatsMethod = DFS_INPUT_STREAM_READ_STATISTICS_METHOD;
      } else {
        inputStream = null;
        readStatsMethod = null;
      }

      if (inputStream == null || readStatsMethod == null) {
        return;
      }

      try {
        Object readStatistics = readStatsMethod.invoke(inputStream);

        addLongStat(ScanOperator.Metric.TOTAL_BYTES_READ, readStatistics, GET_TOTAL_BYTES_READ_METHOD);
        addLongStat(ScanOperator.Metric.LOCAL_BYTES_READ, readStatistics, GET_TOTAL_LOCAL_BYTES_READ_METHOD);
        addLongStat(ScanOperator.Metric.SHORT_CIRCUIT_BYTES_READ, readStatistics, GET_TOTAL_SHORT_CIRCUIT_BYTES_READ_METHOD);
      } catch (IllegalAccessException | InvocationTargetException e) {
        // suppress and continue with other streams
      }
    } finally {
      super.close();
    }
  }

  private void addLongStat(MetricDef metric, Object readStatistics, Method method) throws IllegalAccessException, InvocationTargetException {
    if (readStatistics == null || method == null) {
      return;
    }
    operatorStats.addLongStat(metric, (Long) method.invoke(readStatistics));
  }

  private static Class<?> getClass(String clsName) {
    try {
      return Class.forName(clsName);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static Method getClassMethod(Class<?> cls, String methodName) {
    if (cls == null) {
      return null;
    }

    try {
      return cls.getMethod(methodName);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private static boolean isInstanceOfHdfsDataInputStream(Object o) {
    if (HDFS_DATA_INPUT_STREAM_CLASS == null) {
      return false;
    }

    return HDFS_DATA_INPUT_STREAM_CLASS.isInstance(o);
  }

  private static boolean isInstanceOfDFSInputStream(Object o) {
    if (DFS_INPUT_STREAM_CLASS == null) {
      return false;
    }

    return DFS_INPUT_STREAM_CLASS.isInstance(o);
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
