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
package com.dremio.exec.store.dfs;

import java.util.List;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;

public abstract class FileSystemConf<C extends FileSystemConf<C, P>, P extends FileSystemPlugin<C>> extends ConnectionConf<C, P>{
  public abstract Path getPath();

  public abstract boolean isImpersonationEnabled();

  public abstract List<Property> getProperties();

  public abstract String getConnection();

  public abstract SchemaMutability getSchemaMutability();

  /**
   * Whether the plugin should automatically create the requested path if it doesn't already exist.
   * @return {@code true} if a missing path should be created.
   */
  public boolean createIfMissing() {
    return false;
  }

  /**
   * Indicates that the plugin should use asynchronous reads when possible.
   * (This means the user has enabled async for the source and the underlying FileSystem supports
   * asynchronous reads).
   */
  public boolean isAsyncEnabled() {
    return false;
  }

  /**
   * Interface for plugin implementations to communicate cache properties to underlying system.
   */
  public interface CacheProperties {
    /**
     * Indicates that the plugin requests caching whenever possible.
     * (Note that even if plugin requests caching, underlying Dremio system must support caching and the source
     * must have async reads enabled.)
     * @return {@code true} if caching is requested.
     */
    default boolean isCachingEnabled(final OptionManager optionManager) {
      return false;
    }

    /**
     * If caching is enabled and this feature is supported by underlying Dremio system, this controls the max amount
     * of disk space that can be used to cache data for this source.
     * @return {@code percentage} of max disk space to be used for this source, default is 100%.
     */
    default int cacheMaxSpaceLimitPct() {
      return 100;
    }
  }

  public CacheProperties getCacheProperties() {
    return new CacheProperties() {};
  }


  /**
   * Defines the constants used for cloud file systems.
   */
  public enum CloudFileSystemScheme {
    S3_FILE_SYSTEM_SCHEME("dremioS3"),
    ADL_FILE_SYSTEM_SCHEME("dremioAdl"),
    AZURE_STORAGE_FILE_SYSTEM_SCHEME("dremioAzureStorage://") ;

    private String scheme;
    CloudFileSystemScheme(String scheme) {
      this.scheme = scheme;
    }

    public String getScheme() {
      return scheme;
    }
  }
  /**
   * Is the scheme in path belongs to a cloud file system.
   * @param connectionOrScheme connection string or scheme
   * @return true if scheme is found in CloudFileSystemScheme.
   */
  public static boolean isCloudFileSystemScheme(String connectionOrScheme) {
    for(CloudFileSystemScheme cloudFS: CloudFileSystemScheme.values()) {
      if (connectionOrScheme.startsWith(cloudFS.getScheme())) {
        return true;
      }
    }
    return false;
  }
}
