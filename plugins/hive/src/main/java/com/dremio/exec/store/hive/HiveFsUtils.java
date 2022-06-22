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

import static com.dremio.io.file.UriSchemes.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.dremio.exec.store.hive.exec.dfs.HadoopFsWrapperWithCachePluginClassLoader;
import com.google.common.collect.ImmutableList;

/**
 * This class provides apis required modify Hive conf required for creating HadoopFs
 */
public class HiveFsUtils {

  public static final List<String> SUPPORTED_SCHEME_LIST = ImmutableList.of(
    "hdfs", DREMIO_S3_SCHEME, "s3a", S3_SCHEME,"s3n", DREMIO_S3_SCHEME,
    DREMIO_GCS_SCHEME, GCS_SCHEME, DREMIO_GCS_SCHEME,
    DREMIO_AZURE_SCHEME, AZURE_SCHEME, "wasb", "abfs", "abfss");

  public static final String OLD_FS_CLASS_FORMAT = "fs.%s.impl.dremio.original";
  public static final String UNIQUE_CONF_IDENTIFIER_PROPERTY_NAME = "dremio.store.plugin.conf.uniqueIdentifier";
  // this is boolean value
  public static final String USE_HIVE_PLUGIN_FS_CACHE = "dremio.hive.store.plugin.fs.cache";

  public static String getOldFSClassPropertyName(String scheme) {
    return String.format(OLD_FS_CLASS_FORMAT, scheme);
  }

  /**
   *  get cloned conf with adding Dremio Fs wrapper which gets cached FS instances
   * @param jobConf
   */
  public static Configuration  getClonedConfWithDremioWrapperFs(Configuration jobConf) {
    if (isFsPluginCacheEnabled(jobConf)) {
      JobConf clonedConf = new JobConf(jobConf);
      addHadoopFsWrapperProperty(clonedConf);
      return clonedConf;
    }
    return jobConf;
  }

  public static boolean isFsPluginCacheEnabled(Configuration jobConf) {
    return jobConf != null && jobConf.get(USE_HIVE_PLUGIN_FS_CACHE,  "False").toLowerCase().equals("true");
  }

  private static void addHadoopFsWrapperProperty(JobConf jobConf) {
    for(String scheme : SUPPORTED_SCHEME_LIST) {
      addHadoopFsWrapperProperty(jobConf, scheme, HadoopFsWrapperWithCachePluginClassLoader.class.getName());
    }
  }

  /**
   * This api adds the property for scheme,
   * which will be used to detect class which will be loaded for creating FileSystem for Scheme.
   * If there is already property provided, which willbe moved to another property generated using key OLD_FS_CLASS_FORMAT
   *  OLD_FS_CLASS_FORMAT, is used to get the actual FileSystem class if present
   * @param jobConf configuration
   * @param scheme scheme
   * @param wrapperName FileSystemWrapperClassName
   */
  public static void addHadoopFsWrapperProperty(JobConf jobConf, String scheme, String wrapperName) {
    String schemeFsClassKey  = "fs."+scheme+".impl";
    String originalFsClass  = jobConf.get(schemeFsClassKey, null);
    jobConf.setBoolean(schemeFsClassKey+".disable.cache", true);
    if(jobConf.get(getOldFSClassPropertyName(scheme), null) != null) {
      //  do not setup value as value is already set
      return;
    }

    if(originalFsClass != null) {
      jobConf.set(getOldFSClassPropertyName(scheme), originalFsClass);
    }
    jobConf.set(schemeFsClassKey, wrapperName);
  }
}
