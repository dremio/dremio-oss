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
package com.dremio.plugins.icebergcatalog.dfs;

import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_FILE_SYSTEM_EXPIRE_AFTER_WRITE_MINUTES;
import static com.dremio.exec.store.hive.exec.FileSystemConfUtil.AZURE_FILE_SYSTEM;
import static com.dremio.exec.store.hive.exec.FileSystemConfUtil.GCS_FILE_SYSTEM;
import static com.dremio.exec.store.hive.exec.FileSystemConfUtil.S3_FILE_SYSTEM;
import static com.dremio.io.file.UriSchemes.DREMIO_AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_S3_SCHEME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.hive.exec.FileSystemConfUtil;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is wrapper for the cache which holds the FileSystem objects */
public class HadoopFileSystemCache implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(HadoopFileSystemCache.class);

  private final LoadingCache<HadoopFileSystemCacheKey, FileSystem> cache;

  public HadoopFileSystemCache(OptionManager optionManager) {
    this.cache =
        Caffeine.newBuilder()
            .expireAfterWrite(
                optionManager.getOption(RESTCATALOG_PLUGIN_FILE_SYSTEM_EXPIRE_AFTER_WRITE_MINUTES),
                TimeUnit.MINUTES)
            .removalListener(
                (RemovalListener<HadoopFileSystemCacheKey, FileSystem>)
                    (key, value, cause) -> {
                      try {
                        if (value != null) {
                          value.close();
                        }
                      } catch (IOException e) {
                        // Ignore
                        logger.error("Unable to clean FS from HadoopFileSystemCache", e);
                      }
                    })
            .build(
                key -> {
                  final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
                  final UserGroupInformation ugi;
                  if (key.getUserName().equals(loginUser.getUserName())
                      || SYSTEM_USERNAME.equals(key.getUserName())) {
                    ugi = loginUser;
                  } else {
                    ugi = UserGroupInformation.createProxyUser(key.getUserName(), loginUser);
                  }

                  final PrivilegedExceptionAction<FileSystem> fsFactory =
                      () -> {
                        // Do not use FileSystem#newInstance(Configuration) as it adds filesystem
                        // into the Hadoop cache :(
                        // Mimic instead Hadoop FileSystem#createFileSystem() method
                        final Class<? extends FileSystem> fsClass =
                            FileSystem.getFileSystemClass(key.getScheme(), key.getConf());
                        final FileSystem fs = ReflectionUtils.newInstance(fsClass, key.getConf());
                        fs.initialize(key.getUri(), key.getConf());
                        return fs;
                      };

                  try {
                    return ugi.doAs(fsFactory);
                  } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                });
  }

  @VisibleForTesting
  protected HadoopFileSystemCache(LoadingCache<HadoopFileSystemCacheKey, FileSystem> cache) {
    this.cache = cache;
  }

  public FileSystem load(String filePath, Configuration conf, String userName) {
    try {
      Path path = Path.of(filePath);
      URI uri = path.toURI();
      URI modifiedURI = uri;
      String scheme = uri.getScheme();
      if (scheme == null || "file".equalsIgnoreCase(scheme)) {
        modifiedURI = HadoopFileSystem.getLocal(conf).makeQualified(path).toURI();
      } else {
        scheme = scheme.toLowerCase(Locale.ROOT);
        if (S3_FILE_SYSTEM.contains(scheme)) {
          // The authority duplication here is intentional, it will break if removed.
          modifiedURI =
              new URI(
                  DREMIO_S3_SCHEME,
                  uri.getRawAuthority(),
                  "/" + uri.getRawAuthority() + uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
        } else if (AZURE_FILE_SYSTEM.contains(scheme)) {
          modifiedURI =
              new URI(
                  DREMIO_AZURE_SCHEME,
                  uri.getRawAuthority(),
                  "/" + uri.getUserInfo() + uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
          conf.set("old_scheme", scheme);
          conf.set("authority", uri.getRawAuthority());
        } else if (GCS_FILE_SYSTEM.contains(scheme)) {
          modifiedURI =
              new URI(
                  DREMIO_GCS_SCHEME,
                  uri.getRawAuthority(),
                  "/" + uri.getRawAuthority() + uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
        }
        // else if: HDFS: no URI manipulation required
        FileSystemConfUtil.initializeConfiguration(modifiedURI, conf);
      }
      return cache.get(new HadoopFileSystemCacheKey(modifiedURI, conf, userName));
    } catch (IOException | URISyntaxException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  @Override
  public void close() throws Exception {
    // Empty cache
    cache.invalidateAll();
    cache.cleanUp();
  }

  @VisibleForTesting
  protected LoadingCache<HadoopFileSystemCacheKey, FileSystem> getCache() {
    return cache;
  }
}
