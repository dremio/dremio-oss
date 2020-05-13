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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import org.apache.hadoop.fs.FSDataInputStream;

import com.dremio.exec.store.dfs.FSInputStreamWithStatsWrapper;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;

/**
 * Wrapper for Hadoop {@code FSDataInputStream} which captures some extra stats
 */
public final class FSDataInputStreamWithStatsWrapper extends FSInputStreamWithStatsWrapper {
  private static final Class<?> HDFS_DATA_INPUT_STREAM_CLASS = getClass("org.apache.hadoop.hdfs.client.HdfsDataInputStream");
  private static final Class<?> DFS_INPUT_STREAM_CLASS = getClass("org.apache.hadoop.hdfs.DFSInputStream");
  private static final Class<?> READ_STATISTICS_CLASS = getClass("org.apache.hadoop.hdfs.ReadStatistics");

  private static final Method HDFS_DATA_INPUT_STREAM_READ_STATISTICS_METHOD = getClassMethod(HDFS_DATA_INPUT_STREAM_CLASS, "getReadStatistics");
  private static final Method DFS_INPUT_STREAM_READ_STATISTICS_METHOD = getClassMethod(DFS_INPUT_STREAM_CLASS, "getReadStatistics");

  private static final Method GET_TOTAL_BYTES_READ_METHOD = getClassMethod(READ_STATISTICS_CLASS,"getTotalBytesRead");
  private static final Method GET_TOTAL_LOCAL_BYTES_READ_METHOD = getClassMethod(READ_STATISTICS_CLASS,"getTotalLocalBytesRead");
  private static final Method GET_TOTAL_SHORT_CIRCUIT_BYTES_READ_METHOD = getClassMethod(READ_STATISTICS_CLASS,"getTotalShortCircuitBytesRead");

  private final FSDataInputStream is;
  private final OperatorStats operatorStats;

  private FSDataInputStreamWithStatsWrapper(FSDataInputStream is, OperatorStats operatorStats, boolean recordWaitTime, String filePath)
      throws IOException {
    super(FSDataInputStreamWrapper.of(is), operatorStats, recordWaitTime, filePath);
    this.is = Objects.requireNonNull(is);
    this.operatorStats = Objects.requireNonNull(operatorStats);

  }

  public static FSDataInputStreamWithStatsWrapper of(FSDataInputStream is, OperatorStats operatorStats, boolean recordWaitTime, String filePath) throws IOException {
    return new FSDataInputStreamWithStatsWrapper(is, operatorStats, recordWaitTime, filePath);
  }

  @Override
  public void close() throws IOException {
    try {
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
}
