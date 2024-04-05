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
package com.dremio.plugins.dataplane;

public final class CredentialsProviderConstants {

  public static final String ACCESS_KEY_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  public static final String NONE_PROVIDER =
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
  public static final String ASSUME_ROLE_PROVIDER =
      "com.dremio.plugins.s3.store.STSCredentialProviderV1";
  public static final String EC2_METADATA_PROVIDER =
      "com.amazonaws.auth.InstanceProfileCredentialsProvider";
  public static final String AWS_PROFILE_PROVIDER =
      "com.dremio.plugins.s3.store.AWSProfileCredentialsProviderV1";

  private CredentialsProviderConstants() {}
}
