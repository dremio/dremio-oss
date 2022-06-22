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
package com.dremio.exec.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.HadoopFsCacheKey;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 *
 * This class is wrapper for the cache which holds the FileSystem objects created using dremio class loader
 */
public class HadoopFsCacheWrapperDremioClassLoader implements HadoopFsSupplierProviderDremioClassLoader {
  private static final Logger logger = LoggerFactory.getLogger(HadoopFsCacheWrapperDremioClassLoader.class);
  private LoadingCache<HadoopFsCacheKey, FileSystem> cache =  CacheBuilder.newBuilder()
    .softValues()
      .removalListener(new RemovalListener<HadoopFsCacheKey, FileSystem>() {
    @Override
    public void onRemoval(RemovalNotification<HadoopFsCacheKey, FileSystem> notification) {
      try {
        notification.getValue().close();
      } catch (IOException e) {
        // Ignore
        logger.error("Failed to remove fs in HadoopFsCacheWrapperDremioClassLoader" , e);
      }
    }
  })
    .build(new CacheLoader<HadoopFsCacheKey, FileSystem>() {
    @Override
    public org.apache.hadoop.fs.FileSystem load(HadoopFsCacheKey key) throws Exception {
      try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
        final String disableCacheName = String.format("fs.%s.impl.disable.cache", key.getUri().getScheme());
        // Clone the conf and set cache to disable, so that a new instance is created rather than returning an existing
        final Configuration cloneConf = new Configuration(key.getConf());
        cloneConf.set(disableCacheName, "true");
        return org.apache.hadoop.fs.FileSystem.get(key.getUri(), cloneConf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  });

  @Override
  public Supplier<FileSystem> getHadoopFsSupplierDremioClassLoader(String path, Iterable<Map.Entry<String, String>> conf) {
      return () -> {
        try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
          return  cache.get(new HadoopFsCacheKey(new Path(path).toUri(), conf));
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      };
  }

  @Override
  public void close() throws Exception {
    // Empty cache
    cache.invalidateAll();
    cache.cleanUp();
  }
}
