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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.glue.catalog.metastore.AWSCredentialsProviderFactory;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.plugins.s3.store.STSCredentialProviderV1;

/**
 * AWS Glue metastore client instantiates this class.
 * It is responsible for returning an AWS credential provider based on
 * authentication type that user selects in glue source configuration
 */
public class GlueAWSCredentialsFactory implements AWSCredentialsProviderFactory {
  public static final String ASSUMED_ROLE_ARN = "fs.s3a.assumed.role.arn";
  public GlueAWSCredentialsFactory() {
  }

  public GlueAWSCredentialsFactory(HiveConf conf) {
  }

  public static AWSCredentialsProvider getAWSCredentialsProvider(Configuration conf) {
    String glueAuthenticationProvider = conf.get("com.dremio.aws_credentials_provider", "ACCESS_KEY");
    AWSAuthenticationType authenticationType = AWSAuthenticationType.valueOf(glueAuthenticationProvider);

    switch (authenticationType) {
      case ACCESS_KEY:
        return new org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider(conf);
      case EC2_METADATA:
        return com.amazonaws.auth.InstanceProfileCredentialsProvider.getInstance();
      default:
        throw new AmazonClientException(
          "Failure creating AWS Glue connection. Invalid credentials type.");
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
