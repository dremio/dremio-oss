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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.HadoopFsCacheKey;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class HadoopFsCacheWrapper implements  HadoopFsSupplierProvider {
  private LoadingCache<HadoopFsCacheKey, FileSystem> cache =  CacheBuilder.newBuilder()
    .softValues()
      .removalListener(new RemovalListener<HadoopFsCacheKey, FileSystem>() {
    @Override
    public void onRemoval(RemovalNotification<HadoopFsCacheKey, FileSystem> notification) {
      try {
        notification.getValue().close();
      } catch (IOException e) {
        // Ignore
      }
    }
  })
    .build(new CacheLoader<HadoopFsCacheKey, FileSystem>() {
    @Override
    public org.apache.hadoop.fs.FileSystem load(HadoopFsCacheKey key) throws Exception {
      try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
        return org.apache.hadoop.fs.FileSystem.get(key.getUri(), key.getConf(), key.getQueryUsername());
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
  });

  @Override
  public Supplier<FileSystem> getHadoopFsSupplier(String path, Iterable<Map.Entry<String, String>> conf, String queryUser) {
      return () -> {
        try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
          return  cache.get(new HadoopFsCacheKey(new Path(path).toUri(), conf, queryUser, null));
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
