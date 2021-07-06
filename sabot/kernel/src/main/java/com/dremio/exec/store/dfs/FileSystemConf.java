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

public abstract class FileSystemConf<C extends FileSystemConf<C, P>, P extends FileSystemPlugin<C>> extends ConnectionConf<C, P> implements AsyncStreamConf {
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
   * Defines the constants used for cloud file systems.
   */
  public enum CloudFileSystemScheme {
    S3_FILE_SYSTEM_SCHEME("dremioS3"),
    ADL_FILE_SYSTEM_SCHEME("dremioAdl"),
    AZURE_STORAGE_FILE_SYSTEM_SCHEME("dremioAzureStorage://"),
    GOOGLE_CLOUD_FILE_SYSTEM("dremiogcs");

    private String scheme;
    CloudFileSystemScheme(String scheme) {
      this.scheme = scheme;
    }

    public String getScheme() {
      return scheme;
    }

    /**
     * Create a CacheProperties object
     * @param enabled Whether it should be enabled.
     * @param limitPct The limit pct.
     * @return The CacheProperties object.
     */
    public static CacheProperties of(boolean enabled, int limitPct) {
      return new CacheProperties() {
        @Override
        public int cacheMaxSpaceLimitPct() {
          return limitPct;
        }};
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
