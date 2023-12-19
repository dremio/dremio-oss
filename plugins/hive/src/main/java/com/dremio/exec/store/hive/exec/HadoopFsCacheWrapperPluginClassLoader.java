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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
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
  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
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
        final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        final UserGroupInformation ugi;
        if (key.getUserName().equals(loginUser.getUserName()) || SYSTEM_USERNAME.equals(key.getUserName())) {
          ugi = loginUser;
        } else {
          ugi = UserGroupInformation.createProxyUser(key.getUserName(), loginUser);
        }

        final PrivilegedExceptionAction<org.apache.hadoop.fs.FileSystem> fsFactory = () -> {
          final String disableCacheName = String.format("fs.%s.impl.disable.cache", key.getUri().getScheme());
          // Clone the conf and set cache to disable, so that a new instance is created rather than returning an existing
          final Configuration cloneConf = new Configuration(key.getConf());
          cloneConf.set(disableCacheName, "true");
          return org.apache.hadoop.fs.FileSystem.get(key.getUri(), key.getConf());
        };

        try {
          return ugi.doAs(fsFactory);
        } catch (IOException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

  @Override
  public Supplier<FileSystem> getHadoopFsSupplierPluginClassLoader(String path, Iterable<Map.Entry<String, String>> conf, String userName) {
    return () -> {
      try {
        return  cache.get(new HadoopFsCacheKeyPluginClassLoader(new Path(path).toUri(), conf, userName));
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

  @VisibleForTesting
  protected LoadingCache<HadoopFsCacheKeyPluginClassLoader, FileSystem> getCache() {
      return cache;
  }
}
