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
package com.dremio.common;

/**
 * Common Constants
 */
public interface FSConstants {
  // AWS S3 access key
  String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";

  // AWS S3 secret key
  String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";

  // AWS Region
  String FS_S3A_REGION = "aws.region";

  String FS_S3A_FILE_STATUS_CHECK = "fs.s3a.create.file-status-check";

  // number of simultaneous connections to s3
  String MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum";
  // the maximum number of threads to allow in the pool used by TransferManager
  String MAX_THREADS = "fs.s3a.threads.max";

  // AZURE AAD client ID
  String AZURE_CLIENT_ID = "dremio.azure.clientId";


  // AZURE AAD token endpoint
  String AZURE_TOKEN_ENDPOINT = "dremio.azure.tokenEndpoint";


  // AZURE AAD client secret
  String AZURE_CLIENT_SECRET = "dremio.azure.clientSecret";


  // AZURE Account Name
  String AZURE_ACCOUNT = "dremio.azure.account";


  // AZURE Account Kind
  String AZURE_MODE = "dremio.azure.mode";


  // AZURE secure connection
  String AZURE_SECURE = "dremio.azure.secure";


  // AZURE Shared Access key
  String AZURE_KEY = "dremio.azure.key";
}
