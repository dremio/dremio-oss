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
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.dremio.common.FSConstants;
import com.google.common.base.Preconditions;
import com.dremio.exec.store.hive.GlueAWSCredentialsFactory;

/**
 * Assume role credential provider that supports aws sdk 1.11.X - used by hadoop-aws
 */
public class STSCredentialProviderV1 implements AWSCredentialsProvider, Closeable {

  private final STSAssumeRoleSessionCredentialsProvider stsAssumeRoleSessionCredentialsProvider;

  public STSCredentialProviderV1(Configuration conf) {
    AWSCredentialsProvider awsCredentialsProvider = GlueAWSCredentialsFactory.getAWSCredentialsProvider(conf);
    String region = conf.get(FSConstants.FS_S3A_REGION, "");
    String iamAssumedRole = conf.get(GlueAWSCredentialsFactory.ASSUMED_ROLE_ARN, "");
    Preconditions.checkState(!iamAssumedRole.isEmpty(), "Unexpected empty IAM assumed role");
    Preconditions.checkState(!region.isEmpty(), "Unexpected empty region");

    AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
      .withCredentials(awsCredentialsProvider)
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
