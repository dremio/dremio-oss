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
package com.dremio.plugins.s3.store;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Assume role credential provider that supports aws sdk 2.X
 */
public class STSCredentialProviderV2 implements AwsCredentialsProvider {
  private StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider;

  public STSCredentialProviderV2(Configuration conf) {

    AwsCredentialsProvider awsCredentialsProvider = null;

    if (S3StoragePlugin.ACCESS_KEY_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
        conf.get(Constants.ACCESS_KEY), conf.get(Constants.SECRET_KEY)));
    } else if (S3StoragePlugin.EC2_METADATA_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = InstanceProfileCredentialsProvider.create();
    }

    StsClient stsClient = StsClient.builder()
      .credentialsProvider(awsCredentialsProvider)
      .region(S3FileSystem.getAwsRegionFromConfigurationOrDefault(conf))
      .build();

    AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
      .roleArn(conf.get(Constants.ASSUMED_ROLE_ARN))
      .roleSessionName(UUID.randomUUID().toString())
      .build();

    this.stsAssumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
      .refreshRequest(assumeRoleRequest)
      .stsClient(stsClient)
      .build();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return this.stsAssumeRoleCredentialsProvider.resolveCredentials();
  }

}
