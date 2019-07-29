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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;

import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;

abstract class FSDataStreamInputStreamProvider implements InputStreamProvider {
  final OperatorStats stats;
  private static final Class hdfsDataInputStreamClazz;
  private static final Class dfsInputStreamClazz;
  private static final Class readStatsClazz;
  private static final Method getTotalBytesReadMethod;
  private static final Method getLocalBytesReadMethod;
  private static final Method getShortCircuitBytesReadMethod;

  FSDataStreamInputStreamProvider(OperatorStats stats) {
    this.stats = stats;
  }

  private static Class getClass(String clsName) {
    try {
      return Class.forName(clsName);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static Method getClassMethod(Class cls, String methodName) {
    if (cls == null) {
      return null;
    }

    try {
      return cls.getMethod(methodName);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  static {
    hdfsDataInputStreamClazz = getClass("org.apache.hadoop.hdfs.client.HdfsDataInputStream");
    dfsInputStreamClazz = getClass("org.apache.hadoop.hdfs.DFSInputStream");
    readStatsClazz = getClass("org.apache.hadoop.hdfs.DFSInputStream$ReadStatistics");

    getTotalBytesReadMethod = getClassMethod(readStatsClazz,"getTotalBytesRead");
    getLocalBytesReadMethod = getClassMethod(readStatsClazz,"getTotalLocalBytesRead");
    getShortCircuitBytesReadMethod = getClassMethod(readStatsClazz,"getTotalShortCircuitBytesRead");
  }

  private boolean isHdfsDataInputStream(Class cls) {
    if (hdfsDataInputStreamClazz == null) {
      return false;
    }

    return hdfsDataInputStreamClazz.isAssignableFrom(cls);
  }

  private boolean isDFSInputStream(Class cls) {
    if (dfsInputStreamClazz == null) {
      return false;
    }

    return dfsInputStreamClazz.isAssignableFrom(cls);
  }

  void populateStats(List<FSDataInputStream> streams) {
    if (stats == null) {
      return;
    }

    for(FSDataInputStream stream : streams) {
      if (stream == null) {
        continue;
      }

      Object inputStream = null;
      if (isHdfsDataInputStream(stream.getClass())) {
        inputStream = stream;
      } else if (stream.getWrappedStream() != null) {
        if (isHdfsDataInputStream(stream.getWrappedStream().getClass())) {
          inputStream = stream.getWrappedStream();
        } else if (isDFSInputStream(stream.getWrappedStream().getClass())) {
          inputStream = stream.getWrappedStream();
        }
      }

      if (inputStream == null) {
        continue;
      }

      try {
        Method readStatsMethod = inputStream.getClass().getMethod("getReadStatistics");
        Object readStatsObj = readStatsMethod.invoke(inputStream);

        if (getTotalBytesReadMethod != null) {
          stats.addLongStat(ScanOperator.Metric.TOTAL_BYTES_READ, (Long) getTotalBytesReadMethod.invoke(readStatsObj));
        }
        if (getLocalBytesReadMethod != null) {
          stats.addLongStat(ScanOperator.Metric.LOCAL_BYTES_READ, (Long) getLocalBytesReadMethod.invoke(readStatsObj));
        }
        if (getShortCircuitBytesReadMethod != null) {
          stats.addLongStat(ScanOperator.Metric.SHORT_CIRCUIT_BYTES_READ, (Long) getShortCircuitBytesReadMethod.invoke(readStatsObj));
        }
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        // suppress and continue with other streams
      }
    }
  }
}
