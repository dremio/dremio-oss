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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

/**
 * Assume role credential provider that supports aws sdk 1.11.X - used by hadoop-aws
 */
public class STSCredentialProviderV1 implements AWSCredentialsProvider, Closeable {

  private final STSAssumeRoleSessionCredentialsProvider stsAssumeRoleSessionCredentialsProvider;

  public STSCredentialProviderV1(URI uri, Configuration conf) throws IOException {

    AWSCredentialsProvider awsCredentialsProvider = null;

    //TODO: Leverage S3AUtils createAwsCredentialProvider

    if (S3StoragePlugin.ACCESS_KEY_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = new SimpleAWSCredentialsProvider(uri, conf);
    } else if (S3StoragePlugin.EC2_METADATA_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = InstanceProfileCredentialsProvider.getInstance();
    }

    final String region = S3FileSystem.getAWSRegionFromConfigurationOrDefault(conf).toString();
    final AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard()
      .withCredentials(awsCredentialsProvider)
      .withClientConfiguration(S3AUtils.createAwsConf(conf, ""))
      .withRegion(region);
    S3FileSystem.getStsEndpoint(conf).ifPresent(e -> {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(e, region));
    });

    this.stsAssumeRoleSessionCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(
      conf.get(Constants.ASSUMED_ROLE_ARN), UUID.randomUUID().toString())
      .withStsClient(builder.build())
      .build();
  }

  @Override
  public AWSCredentials getCredentials() {
    return stsAssumeRoleSessionCredentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    this.stsAssumeRoleSessionCredentialsProvider.refresh();
  }

  @Override
  public void close() throws IOException {
    stsAssumeRoleSessionCredentialsProvider.close();
  }
}
