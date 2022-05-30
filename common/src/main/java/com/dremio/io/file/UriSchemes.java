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
package com.dremio.io.file;

/**
 * Listing of schemes supported in Dremio.
 */
public interface UriSchemes {
  String HDFS_SCHEME = "hdfs";
  String MAPRFS_SCHEME = "maprfs";
  String WEBHDFS_SCHEME = "webhdfs";
  String FILE_SCHEME = "file";
  String S3_SCHEME = "s3";
  String COS_SCHEME = "cosn";
  String AZURE_SCHEME = "wasbs";
  String GCS_SCHEME = "gs";
  String ADL_SCHEME = "adl";

  String DREMIO_GCS_SCHEME = "dremiogcs";
  String DREMIO_S3_SCHEME = "dremioS3";
  String DREMIO_AZURE_SCHEME = "dremioAzureStorage";
  String DREMIO_HDFS_SCHEME = HDFS_SCHEME;
  String DREMIO_ADLS_SCHEME = "dremioAdl";

  String LOCALHOST_LOOPBACK = "localhost";
  String CLASS_PATH_FILE_SYSTEM = "classpath";
  String NAS_FILE_SYSTEM = "pdfs";

  String SCHEME_SEPARATOR = "://";
}
