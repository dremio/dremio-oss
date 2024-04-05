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

import static com.dremio.io.file.UriSchemes.HDFS_SCHEME;

import com.dremio.common.VM;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.common.InitializationException;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocationsList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import org.eclipse.jetty.util.URIUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manager for setting up a block locations cache and creating cache entries */
public class BlockLocationsCacheManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockLocationsCacheManager.class);
  private static final String DB_DIR_NAME = "block-locations";
  private static final Striped<Lock> IN_PROGRESS_FILE_LOCKS =
      Striped.lazyWeakLock(VM.availableProcessors());

  private static volatile RocksDbBroker rocksDbBroker;

  private final String pluginId;
  private final RecordingCacheReaderWriter readerWriter;

  private BlockLocationsCacheManager(String pluginId, RecordingCacheReaderWriter readerWriter) {
    this.pluginId = pluginId;
    this.readerWriter = readerWriter;
  }

  public static BlockLocationsCacheManager newInstance(
      FileSystem fs, String pluginId, DremioConfig dremioConfig, OperatorContext operatorContext)
      throws InitializationException {
    return newInstance(
        fs,
        pluginId,
        operatorContext,
        dremioConfig,
        broker -> new RecordingCacheReaderWriter(fs, broker, operatorContext.getStats()));
  }

  @VisibleForTesting
  static BlockLocationsCacheManager newInstance(
      FileSystem fileSystem,
      String pluginId,
      OperatorContext operatorContext,
      DremioConfig dremioConfig,
      Function<RocksDbBroker, RecordingCacheReaderWriter> readerWriter)
      throws InitializationException {
    boolean featureEnabled =
        operatorContext.getOptions().getOption(ExecConstants.HADOOP_BLOCK_CACHE_ENABLED);
    if (!fileSystem.supportsBlockAffinity() || !featureEnabled) {
      LOGGER.debug(
          "Not creating block locations cache on {} filesystem as prerequisites are not met",
          fileSystem);
      return null;
    }

    if (rocksDbBroker == null) {
      synchronized (BlockLocationsCacheManager.class) {
        if (rocksDbBroker == null) {
          Path dbDirectory;
          try {
            dbDirectory = createDirectories(dremioConfig, operatorContext.getNodeEndPoint());
          } catch (IOException e) {
            throw new InitializationException(e);
          }

          try {
            rocksDbBroker = RocksDbBroker.getInstance(dbDirectory.toString());
          } catch (RocksDBException e) {
            throw new InitializationException(e);
          }
        }
      }
    }

    return new BlockLocationsCacheManager(pluginId, readerWriter.apply(rocksDbBroker));
  }

  /**
   * Initialize the cache for the given file.
   *
   * @param filePath path for the file whose block locations are to be cached
   * @param fileSize size of the file in bytes
   * @return the value of the created cache entry
   */
  public BlockLocationsList createIfAbsent(String filePath, long fileSize) {
    if (!hasNoOrHdfsScheme(filePath)) {
      return null;
    }

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
    fileLock.lock();
    try {
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

  private static Path createDirectories(DremioConfig dremioConfig, NodeEndpoint nodeEndPoint)
      throws IOException {
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

  private boolean hasNoOrHdfsScheme(String filePath) {
    String encodedPath = URIUtil.encodePath(filePath); // needed to handle whitespaces in path
    URI uri = URI.create(encodedPath);
    String scheme = uri.getScheme();
    if (scheme == null) {
      return true;
    }
    return scheme.equals(HDFS_SCHEME);
  }
}
