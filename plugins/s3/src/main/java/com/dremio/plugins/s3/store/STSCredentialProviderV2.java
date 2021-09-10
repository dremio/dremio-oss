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

import static com.dremio.plugins.s3.store.S3StoragePlugin.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.DREMIO_ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.EC2_METADATA_PROVIDER;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.common.exceptions.UserException;
import com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV2;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Assume role credential provider that supports aws sdk 2.X
 */
public class STSCredentialProviderV2 implements AwsCredentialsProvider, SdkAutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(STSCredentialProviderV2.class);
  private final StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider;

  public STSCredentialProviderV2(Configuration conf) {
    AwsCredentialsProvider awsCredentialsProvider = null;

    String assumeRoleProvider = conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER);
    logger.debug("assumed_role_credentials_provider: {}", assumeRoleProvider);
    switch(assumeRoleProvider) {
      case ACCESS_KEY_PROVIDER:
        awsCredentialsProvider = StaticCredentialsProvider.create(
          AwsBasicCredentials.create(conf.get(Constants.ACCESS_KEY), conf.get(Constants.SECRET_KEY)));
        break;
      case EC2_METADATA_PROVIDER:
        awsCredentialsProvider = new SharedInstanceProfileCredentialsProvider();
        break;
      case DREMIO_ASSUME_ROLE_PROVIDER:
        awsCredentialsProvider = new DremioAssumeRoleCredentialsProviderV2();
        break;
      default:
        throw new IllegalArgumentException("Assumed role credentials provided " + assumeRoleProvider + " is not supported.");
    }
    if (ACCESS_KEY_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
        conf.get(Constants.ACCESS_KEY), conf.get(Constants.SECRET_KEY)));
    } else if (EC2_METADATA_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = new SharedInstanceProfileCredentialsProvider();
    }

    final StsClientBuilder builder = StsClient.builder()
      .credentialsProvider(awsCredentialsProvider)
      .region(S3FileSystem.getAWSRegionFromConfigurationOrDefault(conf))
      .httpClientBuilder(ApacheHttpConnectionUtil.initConnectionSettings(conf));
    S3FileSystem.getStsEndpoint(conf).ifPresent(e -> {
      try {
        builder.endpointOverride(new URI(e));
      } catch (URISyntaxException use) {
        throw UserException.sourceInBadState(use).buildSilently();
      }
    });

    initUserAgent(builder, conf);

    final AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
      .roleArn(conf.get(Constants.ASSUMED_ROLE_ARN))
      .roleSessionName(UUID.randomUUID().toString())
      .build();

    this.stsAssumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
      .refreshRequest(assumeRoleRequest)
      .stsClient(builder.build())
      .build();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return this.stsAssumeRoleCredentialsProvider.resolveCredentials();
  }


  private static void initUserAgent(StsClientBuilder builder, Configuration conf) {
    String userAgent = "Hadoop " + VersionInfo.getVersion();
    final String userAgentPrefix = conf.getTrimmed(Constants.USER_AGENT_PREFIX, "");
    if (!userAgentPrefix.isEmpty()) {
      userAgent = userAgentPrefix + ", " + userAgent;
    }

    builder.overrideConfiguration(ClientOverrideConfiguration.builder()
      .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent)
      .build());
  }

  @Override
  public void close() {
    stsAssumeRoleCredentialsProvider.close();
  }
}
