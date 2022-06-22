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

import org.apache.hadoop.conf.Configuration;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

/**
 * Wrapper for ProfileCredentialsProvider from the AWS SDK v2.x.
 * Required so that we can have a constructor that takes a Configuration object.
 */
public class AWSProfileCredentialsProviderV2 implements AwsCredentialsProvider, AutoCloseable {
  private final ProfileCredentialsProvider profileCredentialsProvider;

  public AWSProfileCredentialsProviderV2(Configuration conf) {
    String awsProfile = conf.get("com.dremio.awsProfile");
    this.profileCredentialsProvider = ProfileCredentialsProvider.create(awsProfile);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return profileCredentialsProvider.resolveCredentials();
  }

  @Override
  public void close() {
    this.profileCredentialsProvider.close();
  }
}
