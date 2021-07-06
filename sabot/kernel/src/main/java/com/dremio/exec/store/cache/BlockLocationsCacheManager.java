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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.VM;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocationsList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;

/**
 * Manager for setting up a block locations cache and creating cache entries
 */
public class BlockLocationsCacheManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockLocationsCacheManager.class);
  private static final String DB_DIR_NAME = "block-locations";
  private static final long REPEAT_LOG_THRESHOLD = Duration.ofMinutes(30).toMillis();
  private static final Striped<Lock> IN_PROGRESS_FILE_LOCKS = Striped.lazyWeakLock(VM.availableProcessors());

  private static long lastLoggedSetupExceptionTime;
  private static volatile RocksDbBroker rocksDbBroker;

  private final String pluginId;
  private final RecordingCacheReaderWriter readerWriter;

  private BlockLocationsCacheManager(String pluginId, RecordingCacheReaderWriter readerWriter) {
    this.pluginId = pluginId;
    this.readerWriter = readerWriter;
  }

  public static BlockLocationsCacheManager newInstance(FileSystem fs, String pluginId, OperatorContext operatorContext) {
    return newInstance(fs, pluginId, operatorContext, DremioConfig.create(null, operatorContext.getConfig()),
      (broker) -> new RecordingCacheReaderWriter(fs, broker, operatorContext.getStats()));
  }

  @VisibleForTesting
  static BlockLocationsCacheManager newInstance(FileSystem fileSystem, String pluginId,
                                                OperatorContext operatorContext, DremioConfig dremioConfig,
                                                Function<RocksDbBroker, RecordingCacheReaderWriter> readerWriter) {
    boolean featureEnabled = operatorContext.getOptions().getOption(ExecConstants.HADOOP_BLOCK_CACHE_ENABLED);
    if (!fileSystem.supportsBlockAffinity() || !featureEnabled) {
      LOGGER.debug("Not creating block locations cache on {} filesystem as prerequisites are not met", fileSystem);
      return null;
    }

    if (rocksDbBroker == null) {
      synchronized (BlockLocationsCacheManager.class) {
        if (rocksDbBroker == null) {
          Path dbDirectory = null;
          try {
            dbDirectory = createDirectories(dremioConfig, operatorContext.getNodeEndPoint());
          } catch (IOException e) {
            logJudiciously(e);
          }
          if (dbDirectory == null) {
            return null;
          }

          RocksDbBroker rocksDbBroker = null;
          try {
            rocksDbBroker = RocksDbBroker.getInstance(dbDirectory.toString());
          } catch (RuntimeRocksDBException e) {
            logJudiciously(e);
          }
          if (rocksDbBroker == null) {
            return null;
          }

          BlockLocationsCacheManager.rocksDbBroker = rocksDbBroker;
        }
      }
    }

    return new BlockLocationsCacheManager(pluginId, readerWriter.apply(rocksDbBroker));
  }

  /**
   * Initialize the cache for the given file.
   * @param filePath path for the file whose block locations are to be cached
   * @param fileSize size of the file in bytes
   * @return the value of the created cache entry
   */
  public BlockLocationsList createIfAbsent(String filePath, long fileSize) {
    byte[] key = KeyValueSerDe.serializeKey(filePath, pluginId);
    byte[] currentVal = readerWriter.get(key, true);
    if (currentVal != null) {
      return KeyValueSerDe.deserializeValue(currentVal);
    }
    return putOnce(key, filePath, fileSize);
  }

  @Override
  public void close() throws Exception {
    readerWriter.close();
  }

  @VisibleForTesting
  BlockLocationsList putOnce(byte[] key, String filePath, long fileSize) {
    final Lock fileLock = IN_PROGRESS_FILE_LOCKS.get(filePath);
    try {
      fileLock.lock();
      byte[] value = readerWriter.get(key, false);
      if (value == null) {
        value = readerWriter.put(key, filePath, fileSize);
        if (value == null) {
          return null;
        }
      }
      return KeyValueSerDe.deserializeValue(value);
    } finally {
      fileLock.unlock();
    }
  }

  @VisibleForTesting
  RecordingCacheReaderWriter getReaderWriter() {
    return readerWriter;
  }

  @VisibleForTesting
  static void resetRocksDbBroker() {
    rocksDbBroker.resetInstance();
    rocksDbBroker = null;
  }

  private static Path createDirectories(DremioConfig dremioConfig, NodeEndpoint nodeEndPoint) throws IOException {
    Path baseDir = Paths.get(dremioConfig.getString(DremioConfig.CACHE_DB_PATH));
    boolean isYarnDeployment = dremioConfig.getBoolean(DremioConfig.YARN_ENABLED_BOOL);
    if (isYarnDeployment) {
      baseDir = baseDir.resolve(Integer.toString(nodeEndPoint.getFabricPort()));
    }

    Path dbDirPath = baseDir.resolve(DB_DIR_NAME);
    if (Files.exists(dbDirPath)) {
      if (!Files.isDirectory(dbDirPath)) {
        throw new IOException(dbDirPath + " is not a directory");
      }
      if (!Files.isReadable(dbDirPath)) {
        throw new IOException(dbDirPath + " is not readable");
      }
      if (!Files.isWritable(dbDirPath)) {
        throw new IOException(dbDirPath + " is not writable");
      }
    } else {
      LOGGER.info("Creating DB directory at: {}", dbDirPath);
      Files.createDirectories(dbDirPath);
    }
    return dbDirPath;
  }

  private static void logJudiciously(Exception e) {
    // write judiciously to prevent flooding the logs with multiple such messages
    long currTimeMillis = System.currentTimeMillis();
    long diff = currTimeMillis - lastLoggedSetupExceptionTime;
    if (diff > REPEAT_LOG_THRESHOLD) {
      LOGGER.error("Caught exception during setup: ", e);
      lastLoggedSetupExceptionTime = currTimeMillis;
    }
  }
}
