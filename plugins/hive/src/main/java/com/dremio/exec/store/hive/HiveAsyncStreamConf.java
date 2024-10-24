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
package com.dremio.exec.store.hive;

import java.util.Set;

import com.dremio.exec.store.hive.exec.FileSystemConfUtil;
import org.apache.hadoop.mapred.JobConf;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.dfs.AsyncStreamConf;
import com.dremio.exec.store.dfs.CacheProperties;
import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableSet;

/**
 * Provide async conf from hive jobconf
 */
public final class HiveAsyncStreamConf implements AsyncStreamConf {
  private final String fsScheme;
  private final JobConf jobConf;
  private final OptionManager optionManager;
  private final Set<String> azureStorageV1Schemes = ImmutableSet.of("wasb", "wasbs");

  public static AsyncStreamConf from(String scheme, JobConf jobConf, OptionManager optionManager) {
    return new HiveAsyncStreamConf(scheme, jobConf, optionManager);
  }

  @Override
  public boolean isAsyncEnabled() {
    // hive-async disabled globally
    if (!optionManager.getOption(ExecConstants.ENABLE_HIVE_ASYNC)) {
      return false;
    }

    // hive-async disabled for this source
    if (!jobConf.getBoolean(HiveConfFactory.HIVE_ENABLE_ASYNC, true)) {
      return false;
    }

    if (FileSystemConfUtil.S3_FILE_SYSTEM.contains(fsScheme)) {
      // s3 async option defined in com.dremio.plugins.s3.store.S3Options
      return optionManager.getOption("store.s3.async").getBoolVal();
    }

    if (FileSystemConfUtil.AZURE_FILE_SYSTEM.contains(fsScheme)) {
      // azure storage async option defined in com.dremio.plugins.azure.AzureStorageOptions
      return !azureStorageV1Schemes.contains(fsScheme) &&
              optionManager.getOption("store.azure.async").getBoolVal();
    }

    if (FileSystemConfUtil.GCS_FILE_SYSTEM.contains(fsScheme)) {
      // gcs storage async option defined in com.dremio.plugins.gcs.GCSOptions
      return optionManager.getOption("store.gcs.async").getBoolVal();
    }

    return jobConf.getBoolean(HiveConfFactory.HIVE_ENABLE_ASYNC, true);
  }
  @Override
  public CacheProperties getCacheProperties() {
    return new HiveCacheProperties();
  }

  private final class HiveCacheProperties implements CacheProperties {

    @Override
    public boolean isCachingEnabled(OptionManager optionManager) {

      if (FileSystemConfUtil.HDFS_FILE_SYSTEM.contains(fsScheme)) {
        return jobConf.getBoolean(HiveConfFactory.HIVE_ENABLE_CACHE_FOR_HDFS, false);
      }

      if (FileSystemConfUtil.S3_FILE_SYSTEM.contains(fsScheme)) {
        return jobConf.getBoolean(HiveConfFactory.HIVE_ENABLE_CACHE_FOR_S3_AND_AZURE_STORAGE, true);
      }

      if (FileSystemConfUtil.AZURE_FILE_SYSTEM.contains(fsScheme)) {
        return jobConf.getBoolean(HiveConfFactory.HIVE_ENABLE_CACHE_FOR_S3_AND_AZURE_STORAGE, true);
      }

      if (FileSystemConfUtil.GCS_FILE_SYSTEM.contains(fsScheme)) {
        return jobConf.getBoolean(HiveConfFactory.HIVE_ENABLE_CACHE_FOR_GCS, true);
      }

      return false;
    }

    @Override
    public int cacheMaxSpaceLimitPct() {
      return jobConf.getInt(HiveConfFactory.HIVE_MAX_HIVE_CACHE_SPACE, 100);
    }
  }

  private HiveAsyncStreamConf(String fsScheme, JobConf jobConf, OptionManager optionManager) {
    this.fsScheme = fsScheme.toLowerCase();
    this.jobConf = jobConf;
    this.optionManager = optionManager;
  }
}
