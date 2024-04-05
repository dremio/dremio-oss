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
package com.dremio.exec.store.hive;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.glue.catalog.metastore.AWSCredentialsProviderFactory;
import com.dremio.plugins.s3.store.AWSProfileCredentialsProviderV1;
import com.dremio.plugins.s3.store.STSCredentialProviderV1;
import com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS Glue metastore client instantiates this class. It is responsible for returning an AWS
 * credential provider based on authentication type that user selects in glue source configuration
 */
public class GlueAWSCredentialsFactory implements AWSCredentialsProviderFactory {
  private static final Logger logger = LoggerFactory.getLogger(GlueAWSCredentialsFactory.class);

  public static final String ASSUMED_ROLE_ARN = "fs.s3a.assumed.role.arn";
  public static final String ACCESS_KEY_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
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

  public GlueAWSCredentialsFactory() {}

  public GlueAWSCredentialsFactory(HiveConf conf) {}

  public static AWSCredentialsProvider getAWSCredentialsProvider(Configuration conf) {
    logger.debug("aws_credentials_provider:{}", conf.get(Constants.AWS_CREDENTIALS_PROVIDER));
    switch (conf.get(Constants.AWS_CREDENTIALS_PROVIDER)) {
      case ACCESS_KEY_PROVIDER:
        return new org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider(conf);
      case EC2_METADATA_PROVIDER:
        return com.amazonaws.auth.InstanceProfileCredentialsProvider.getInstance();
      case DREMIO_ASSUME_ROLE_PROVIDER:
        return new DremioAssumeRoleCredentialsProviderV1();
      case GLUE_DREMIO_ASSUME_ROLE_PROVIDER:
        return new GlueDremioAssumeRoleCredentialsProviderV1();
      case ASSUME_ROLE_PROVIDER:
        return new STSCredentialProviderV1(conf);
      case AWS_PROFILE_PROVIDER:
        return new AWSProfileCredentialsProviderV1(conf);
      default:
        throw new IllegalStateException(
            "Invalid AWSCredentialsProvider provided: "
                + conf.get(Constants.AWS_CREDENTIALS_PROVIDER));
    }
  }

  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(
      org.apache.hadoop.hive.conf.HiveConf hiveConf) {

    String iamAssumedRole = hiveConf.get(ASSUMED_ROLE_ARN, "");
    if (iamAssumedRole.isEmpty()) {
      return getAWSCredentialsProvider(hiveConf);
    }

    return new STSCredentialProviderV1(hiveConf);
  }
}
