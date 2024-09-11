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
package com.dremio.plugins.util.awsauth;

import com.amazonaws.auth.AWSCredentialsProvider;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

/**
 * Factory to provide the appropriate AWSCredentialsProvider based on a Configuration.
 *
 * <p>Note: This package can hopefully be used in the future to consolidate very similar code in S3,
 * Hive, and Glue plugins, but that was out of scope for the current work.
 */
public final class DremioAWSCredentialsProviderFactory {
  public static final String ACCESS_KEY_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  public static final String GLUE_ACCESS_KEY_PROVIDER =
      "com.dremio.exec.store.hive.GlueAWSCredentialsProvider";
  public static final String ASSUMED_ROLE_CREDENTIALS_PROVIDER =
      "fs.s3a.assumed.role.credentials.provider";
  public static final String ASSUME_ROLE_PROVIDER =
      "com.dremio.plugins.s3.store.STSCredentialProviderV1";
  public static final String EC2_METADATA_PROVIDER =
      "com.amazonaws.auth.InstanceProfileCredentialsProvider";
  public static final String DREMIO_ASSUME_ROLE_PROVIDER =
      "com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV1";
  public static final String GLUE_DREMIO_ASSUME_ROLE_PROVIDER =
      "com.dremio.exec.store.hive.GlueDremioAssumeRoleCredentialsProviderV1";
  public static final String AWS_PROFILE_PROVIDER =
      "com.dremio.plugins.s3.store.AWSProfileCredentialsProviderV1";

  private DremioAWSCredentialsProviderFactory() {}

  /** Constructs and returns the appropriate AWSCredentialsProvider. */
  public static AWSCredentialsProvider getAWSCredentialsProvider(Configuration config) {
    switch (config.get(Constants.AWS_CREDENTIALS_PROVIDER)) {
      case ACCESS_KEY_PROVIDER:
      case GLUE_ACCESS_KEY_PROVIDER:
        return new GlueAWSCredentialsProvider(null, config);

      case EC2_METADATA_PROVIDER:
        return com.amazonaws.auth.InstanceProfileCredentialsProvider.getInstance();

      case DREMIO_ASSUME_ROLE_PROVIDER:
        return new DremioAssumeRoleCredentialsProviderV1();

      case GLUE_DREMIO_ASSUME_ROLE_PROVIDER:
        return new GlueDremioAssumeRoleCredentialsProviderV1();

      case ASSUME_ROLE_PROVIDER:
        try {
          return new STSCredentialProviderV1(config);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

      case AWS_PROFILE_PROVIDER:
        return new AWSProfileCredentialsProviderV1(config);

      default:
        throw new IllegalStateException(
            "Invalid AWSCredentialsProvider provided: "
                + config.get(Constants.AWS_CREDENTIALS_PROVIDER));
    }
  }
}
