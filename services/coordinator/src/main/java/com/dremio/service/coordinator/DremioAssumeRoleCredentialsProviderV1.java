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

import java.util.HashMap;
import java.util.Map;

import javax.inject.Provider;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.AssumeRoleCredentialsProvider;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Gives an aws sdk v1 compatible version.
 */
public class DremioAssumeRoleCredentialsProviderV1 implements AWSCredentialsProvider,AutoCloseable {
  public static final String ACCESS_KEY = "fs.s3a.access.key";
  public static final String SECRET_KEY = "fs.s3a.secret.key";
  public static final String SESSION_TOKEN = "session.token";

  private static Provider<AssumeRoleCredentialsProvider> credentialsProvider;

  public static void setAssumeRoleProvider(Provider<AssumeRoleCredentialsProvider> credentialsProvider) {
    DremioAssumeRoleCredentialsProviderV1.credentialsProvider = credentialsProvider;
  }

  public Map<String, String> getCredentialsMap() {
    Map<String, String> map = new HashMap<>(2);
    AWSCredentials awsCredentials = getCredentials();
    map.put(ACCESS_KEY, awsCredentials.getAWSAccessKeyId());
    map.put(SECRET_KEY, awsCredentials.getAWSSecretKey());
    if (awsCredentials instanceof AWSSessionCredentials) {
      map.put(SESSION_TOKEN, ((AWSSessionCredentials) awsCredentials).getSessionToken());
    }
    return map;
  }

  @Override
  public AWSCredentials getCredentials() {
    // wrap the v2 session credentials to a v1
    final AwsCredentials awsCredentials = credentialsProvider.get().resolveCredentials();

    return new AWSSessionCredentials() {
      @Override
      public String getSessionToken() {
        if (awsCredentials instanceof AwsSessionCredentials) {
          return ((AwsSessionCredentials)awsCredentials).sessionToken();
        }
        return "";
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
