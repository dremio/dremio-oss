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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.ImmutableSet;

/**
 * Used to modify URI to provide async reader implementations.
 */
public class AsyncReaderUtils {

  public static final Set<String> GCS_FILE_SYSTEM = ImmutableSet.of("gs","dremiogcs");
  public static final String DREMIO_GCS = "dremiogcs";
  public static final Set<String> S3_FILE_SYSTEM = ImmutableSet.of("s3a","s3","s3n", "dremios3");
  public static final Set<String> AZURE_FILE_SYSTEM = ImmutableSet.of("wasbs", "wasb", "abfs",
    "abfss");
  public static final Set<String> HDFS_FILE_SYSTEM = ImmutableSet.of("hdfs");
  public static final String DREMIO_S3 = "dremioS3";
  public static final String FS_DREMIO_S3_IMPL = "fs.dremioS3.impl";
  public static final String FS_DREMIO_GCS_IMPL = "fs.dremiogcs.impl";
  public static final String DREMIO_AZURE = "dremioAzureStorage";
  public static final String FS_DREMIO_AZURE_IMPL = "fs.dremioAzureStorage.impl";
  public static final String DREMIO_HDFS = "hdfs";
  public static final String FS_DREMIO_HDFS_IMPL = "fs.hdfs.impl";

  /**
   * Modify the scheme and map to wrapper file system to support async.
   * @param uri
   * @param jobConf
   * @return
   * @throws URISyntaxException
   */
  public static URI injectDremioConfigForAsyncRead(URI uri, JobConf jobConf) throws URISyntaxException {
    URI modifiedURI = uri;
    String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
    if (S3_FILE_SYSTEM.contains(scheme)) {
      modifiedURI = new URI(DREMIO_S3,  uri.getRawAuthority(), "/" + uri.getRawAuthority() +  uri.getPath(),
        uri.getQuery(), uri.getFragment());
      jobConf.set(FS_DREMIO_S3_IMPL, DremioFileSystem.class.getName());
    } else if (AZURE_FILE_SYSTEM.contains(scheme)) {
      modifiedURI = new URI(DREMIO_AZURE,  uri.getRawAuthority(), "/" + uri.getUserInfo() +  uri.getPath(),
        uri.getQuery(), uri.getFragment());
      jobConf.set("old_scheme", scheme);
      jobConf.set("authority", uri.getRawAuthority());
      jobConf.set(FS_DREMIO_AZURE_IMPL, DremioFileSystem.class.getName());
    } else if (HDFS_FILE_SYSTEM.contains(scheme)) {
      modifiedURI = new URI(DREMIO_HDFS,  uri.getHost(), uri.getPath(),
        uri.getFragment());
      jobConf.set(FS_DREMIO_HDFS_IMPL, DremioFileSystem.class.getName());
    } else if (GCS_FILE_SYSTEM.contains(scheme)) {
      modifiedURI = new URI(DREMIO_GCS,  uri.getRawAuthority(), "/" + uri.getRawAuthority() +  uri.getPath(),
              uri.getQuery(), uri.getFragment());
      jobConf.set(FS_DREMIO_GCS_IMPL, DremioFileSystem.class.getName());
    }
    return  modifiedURI;
  }

}
