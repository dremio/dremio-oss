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
package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 *
 * This class is wrapper for the cache which holds the FileSystem objects created using plugin class loader
 */
public class HadoopFsCacheWrapperPluginClassLoader implements HadoopFsSupplierProviderPluginClassLoader {
  private static final Logger logger = LoggerFactory.getLogger(HadoopFsCacheWrapperPluginClassLoader.class);
  private LoadingCache<HadoopFsCacheKeyPluginClassLoader, FileSystem> cache =  CacheBuilder.newBuilder()
    .softValues()
    .removalListener(new RemovalListener<HadoopFsCacheKeyPluginClassLoader, FileSystem>() {
      @Override
      public void onRemoval(RemovalNotification<HadoopFsCacheKeyPluginClassLoader, FileSystem> notification) {
        try {
          notification.getValue().close();
        } catch (IOException e) {
          // Ignore
          logger.error("Unable to clean FS from HadoopFsCacheKeyPluginClassLoader", e);
        }
      }
    })
    .build(new CacheLoader<HadoopFsCacheKeyPluginClassLoader, FileSystem>() {
      @Override
      public org.apache.hadoop.fs.FileSystem load(HadoopFsCacheKeyPluginClassLoader key) throws Exception {
        try {
          final String disableCacheName = String.format("fs.%s.impl.disable.cache", key.getUri().getScheme());
          // Clone the conf and set cache to disable, so that a new instance is created rather than returning an existing
          final Configuration cloneConf = new Configuration(key.getConf());
          cloneConf.set(disableCacheName, "true");
          return org.apache.hadoop.fs.FileSystem.get(key.getUri(), key.getConf());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    });

  @Override
  public Supplier<FileSystem> getHadoopFsSupplierPluginClassLoader(String path, Iterable<Map.Entry<String, String>> conf) {
    return () -> {
      try {
        return  cache.get(new HadoopFsCacheKeyPluginClassLoader(new Path(path).toUri(), conf));
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
