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

import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactory.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactory.ASSUMED_ROLE_CREDENTIALS_PROVIDER;
import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactory.DREMIO_ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactory.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactory.GLUE_DREMIO_ASSUME_ROLE_PROVIDER;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.dremio.common.FSConstants;
import com.google.common.base.Preconditions;

/**
 * Assume role credential provider that supports aws sdk 1.11.X - used by hadoop-aws
 */
public class STSCredentialProviderV1 implements AWSCredentialsProvider, Closeable {
  private static final Logger logger = LoggerFactory.getLogger(STSCredentialProviderV1.class);

  private final STSAssumeRoleSessionCredentialsProvider stsAssumeRoleSessionCredentialsProvider;

  public STSCredentialProviderV1(Configuration conf) throws IOException {
    AWSCredentialsProvider awsCredentialsProvider;
    // TODO Leverage S3AUtils createAwsCredentialProvider

    String assumeRoleProvider = conf.get(ASSUMED_ROLE_CREDENTIALS_PROVIDER);
    logger.debug("assumed_role_credentials_provider: {}", assumeRoleProvider);

    switch(assumeRoleProvider) {
      case ACCESS_KEY_PROVIDER:
        awsCredentialsProvider = new SimpleAWSCredentialsProvider(null, conf);
        break;
      case EC2_METADATA_PROVIDER:
        awsCredentialsProvider = InstanceProfileCredentialsProvider.getInstance();
        break;
      case DREMIO_ASSUME_ROLE_PROVIDER:
        awsCredentialsProvider = new DremioAssumeRoleCredentialsProviderV1();
        break;
      case GLUE_DREMIO_ASSUME_ROLE_PROVIDER:
        awsCredentialsProvider = new GlueDremioAssumeRoleCredentialsProviderV1();
        break;
      default:
        throw new IllegalArgumentException("Assumed role credentials provided " + assumeRoleProvider + " is not supported.");
    }

    String region = conf.get(FSConstants.FS_S3A_REGION, "");
    String iamAssumedRole = conf.get(Constants.ASSUMED_ROLE_ARN, "");
    Preconditions.checkState(!iamAssumedRole.isEmpty(), "Unexpected empty IAM assumed role");
    Preconditions.checkState(!region.isEmpty(), "Unexpected empty region");

    ClientConfiguration clientConfiguration = S3AUtils.createAwsConf(conf, "")
        .withProtocol(Protocol.HTTPS); // Use HTTPS always for STS client

    AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
      .withCredentials(awsCredentialsProvider)
      .withClientConfiguration(clientConfiguration)
      .withRegion(region)
      .build();

    this.stsAssumeRoleSessionCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(
      iamAssumedRole, UUID.randomUUID().toString())
      .withStsClient(stsClient)
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
