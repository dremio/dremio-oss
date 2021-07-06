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
package com.dremio.exec.store.cache;

import static com.dremio.sabot.op.scan.ScanOperator.Metric.BLOCK_AFFINITY_CACHE_HITS;
import static com.dremio.sabot.op.scan.ScanOperator.Metric.BLOCK_AFFINITY_CACHE_MISSES;

import java.io.IOException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorStats;

/**
 * A reader-writer for cache which also tracks metrics
 */
class RecordingCacheReaderWriter implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordingCacheReaderWriter.class);

  private final FileSystem fileSystem;
  private final RocksDbBroker rocksDbBroker;
  private final OperatorStats operatorStats;

  private long cacheHits;
  private long cacheMisses;
  private boolean closed;

  RecordingCacheReaderWriter(FileSystem fileSystem, RocksDbBroker rocksDbBroker, OperatorStats operatorStats) {
    this.fileSystem = fileSystem;
    this.operatorStats = operatorStats;
    this.rocksDbBroker = rocksDbBroker;
  }

  byte[] get(byte[] key, boolean recordMetrics) {
    byte[] value = rocksDbBroker.get(key);
    if (recordMetrics) {
      if (value == null) {
        cacheMisses++;
      } else {
        cacheHits++;
      }
    }
    return value;
  }

  byte[] put(byte[] key, String filePath, long fileSize) {
    Iterable<FileBlockLocation> blockLocations = getBlockLocations(filePath, fileSize);
    byte[] value = KeyValueSerDe.serializeValue(blockLocations);
    rocksDbBroker.put(key, value);
    return value;
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      operatorStats.addLongStat(BLOCK_AFFINITY_CACHE_MISSES, cacheMisses);
      operatorStats.addLongStat(BLOCK_AFFINITY_CACHE_HITS, cacheHits);
    }
  }

  private Iterable<FileBlockLocation> getBlockLocations(String filePath, long fileSize) {
    Iterable<FileBlockLocation> blockLocations;
    try {
      blockLocations = fileSystem.getFileBlockLocations(com.dremio.io.file.Path.of(filePath), 0, fileSize);
    } catch (IOException e) {
      LOGGER.debug("getFileBlockLocations resulted in exception", e);
      return Collections.emptyList();
    }
    return blockLocations;
  }
}
