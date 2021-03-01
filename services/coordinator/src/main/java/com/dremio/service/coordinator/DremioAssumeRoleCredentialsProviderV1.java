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
package com.dremio.service.coordinator;

import javax.inject.Provider;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.AssumeRoleCredentialsProvider;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Gives an aws sdk v1 compatible version.
 */
public class DremioAssumeRoleCredentialsProviderV1 implements AWSCredentialsProvider,AutoCloseable {

  private static Provider<AssumeRoleCredentialsProvider> credentialsProvider;

  public static void setAssumeRoleProvider(Provider<AssumeRoleCredentialsProvider> credentialsProvider) {
    DremioAssumeRoleCredentialsProviderV1.credentialsProvider = credentialsProvider;
  }

  @Override
  public AWSCredentials getCredentials() {
    // wrap the v2 session credentials to a v1
    // TODO : this might cause class loader issues with glue
    // need to resolve with data source roles.
    final AwsSessionCredentials awsCredentials =
      (AwsSessionCredentials) credentialsProvider.get().resolveCredentials();

    return new AWSSessionCredentials() {
      @Override
      public String getSessionToken() {
        return awsCredentials.sessionToken();
      }

      @Override
      public String getAWSAccessKeyId() {
        return awsCredentials.accessKeyId();
      }

      @Override
      public String getAWSSecretKey() {
        return awsCredentials.secretAccessKey();
      }
    };
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(credentialsProvider.get());
  }

  @Override
  public void refresh() {
  }
}
