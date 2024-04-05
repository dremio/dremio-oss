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
package com.dremio.plugins.util;

import static com.dremio.plugins.s3.store.S3StoragePlugin.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.AWS_PROFILE_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.DREMIO_ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.NONE_PROVIDER;

import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.plugins.s3.store.AWSProfileCredentialsProviderV2;
import com.dremio.plugins.s3.store.STSCredentialProviderV2;
import com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV2;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/** Utility for creating credential providers based on plugin configuration. */
public final class AwsCredentialProviderUtils {

  private AwsCredentialProviderUtils() {
    // Not to be instantiated
  }

  /**
   * Create credential provider from the configuration. AwsCredentialsProvider might also implement
   * SdkAutoCloseable. Make sure to close if using directly (or let client close it for you).
   */
  public static AwsCredentialsProvider getCredentialsProvider(Configuration config)
      throws IOException {
    switch (config.get(Constants.AWS_CREDENTIALS_PROVIDER)) {
      case ACCESS_KEY_PROVIDER:
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                new String(config.getPassword(Constants.ACCESS_KEY)),
                new String(config.getPassword(Constants.SECRET_KEY))));
      case EC2_METADATA_PROVIDER:
        return new SharedInstanceProfileCredentialsProvider();
      case NONE_PROVIDER:
        return AnonymousCredentialsProvider.create();
      case ASSUME_ROLE_PROVIDER:
        return new STSCredentialProviderV2(config);
      case DREMIO_ASSUME_ROLE_PROVIDER:
        return new DremioAssumeRoleCredentialsProviderV2();
      case AWS_PROFILE_PROVIDER:
        return new AWSProfileCredentialsProviderV2(config);
      default:
        throw new IllegalStateException(
            "Invalid AWSCredentialsProvider provided: "
                + config.get(Constants.AWS_CREDENTIALS_PROVIDER));
    }
  }
}
