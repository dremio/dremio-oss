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

import static com.dremio.exec.store.hive.GlueAWSCredentialsFactory.ACCESS_KEY_PROVIDER;
import static com.dremio.exec.store.hive.GlueAWSCredentialsFactory.DREMIO_ASSUME_ROLE_PROVIDER;
import static com.dremio.exec.store.hive.GlueAWSCredentialsFactory.EC2_METADATA_PROVIDER;
import static com.dremio.exec.store.hive.GlueAWSCredentialsFactory.GLUE_DREMIO_ASSUME_ROLE_PROVIDER;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.dremio.common.FSConstants;
import com.dremio.exec.store.hive.GlueDremioAssumeRoleCredentialsProviderV1;
import com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV1;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assume role credential provider that supports aws sdk 1.11.X - used by hadoop-aws */
public class STSCredentialProviderV1 implements AWSCredentialsProvider, Closeable {
  private static final Logger logger = LoggerFactory.getLogger(STSCredentialProviderV1.class);

  private final STSAssumeRoleSessionCredentialsProvider stsAssumeRoleSessionCredentialsProvider;

  public STSCredentialProviderV1(Configuration conf) {
    AWSCredentialsProvider awsCredentialsProvider = null;
    // TODO: Leverage S3AUtils createAwsCredentialProvider

    String assumeRoleProvider = conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER);
    logger.debug("assumed_role_credentials_provider: {}", assumeRoleProvider);

    switch (assumeRoleProvider) {
      case ACCESS_KEY_PROVIDER:
        try {
          awsCredentialsProvider = new SimpleAWSCredentialsProvider(null, conf);
        } catch (IOException e) {
          // TODO: resolve this ^
          throw new RuntimeException(e);
        }
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
        throw new IllegalArgumentException(
            "Assumed role credentials provided " + assumeRoleProvider + " is not supported.");
    }

    String region = conf.get(FSConstants.FS_S3A_REGION, "");
    String iamAssumedRole = conf.get(Constants.ASSUMED_ROLE_ARN, "");
    Preconditions.checkState(!iamAssumedRole.isEmpty(), "Unexpected empty IAM assumed role");
    Preconditions.checkState(!region.isEmpty(), "Unexpected empty region");

    AWSSecurityTokenServiceClientBuilder stsClientBuilder =
        AWSSecurityTokenServiceClientBuilder.standard().withCredentials(awsCredentialsProvider);

    /*
     Support setting a custom STS endpoint. If the assume role STS endpoint
     is not provided, then just fall back to setting region.
    */
    String iamAssumedRoleEndpoint = conf.get(Constants.ASSUMED_ROLE_STS_ENDPOINT, "");
    if (!iamAssumedRoleEndpoint.isEmpty()) {
      AwsClientBuilder.EndpointConfiguration ec =
          new AwsClientBuilder.EndpointConfiguration(iamAssumedRoleEndpoint, region);
      stsClientBuilder.withEndpointConfiguration(ec);
    } else {
      stsClientBuilder.withRegion(region);
    }

    this.stsAssumeRoleSessionCredentialsProvider =
        new STSAssumeRoleSessionCredentialsProvider.Builder(
                iamAssumedRole, UUID.randomUUID().toString())
            .withStsClient(stsClientBuilder.build())
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
