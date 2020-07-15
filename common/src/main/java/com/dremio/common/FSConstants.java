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
}
