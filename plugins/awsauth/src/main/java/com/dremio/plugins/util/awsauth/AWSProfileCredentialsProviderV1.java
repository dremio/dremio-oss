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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

/**
 * Wrapper for ProfileCredentialsProvider from the AWS SDK v1.x.
 * Required so that we can have a constructor that takes a Configuration object.
 */
public class AWSProfileCredentialsProviderV1 implements AWSCredentialsProvider {
  public static final String AWS_PROFILE_PROPERTY_KEY = "com.dremio.awsProfile";

  private final ProfileCredentialsProvider profileCredentialsProvider;

  public AWSProfileCredentialsProviderV1(Configuration conf) {
    String awsProfile = conf.get(AWS_PROFILE_PROPERTY_KEY);
    this.profileCredentialsProvider = new ProfileCredentialsProvider(awsProfile);
  }

  @Override
  public AWSCredentials getCredentials() {
    return profileCredentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    this.profileCredentialsProvider.refresh();
  }
}
