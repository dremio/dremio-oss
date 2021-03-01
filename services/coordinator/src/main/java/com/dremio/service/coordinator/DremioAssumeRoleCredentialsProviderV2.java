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

import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.AssumeRoleCredentialsProvider;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Wrapper around the credentials provider to be able to plugin to the hadoop
 * libraries.
 * Gives aws sdk2 compatible implementation.
 */
public class DremioAssumeRoleCredentialsProviderV2 implements AwsCredentialsProvider, AutoCloseable {

  private static Provider<AssumeRoleCredentialsProvider> credentialsProvider;

  public static void setAssumeRoleProvider(Provider<AssumeRoleCredentialsProvider> credentialsProvider) {
    DremioAssumeRoleCredentialsProviderV2.credentialsProvider = credentialsProvider;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentialsProvider.get().resolveCredentials();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(credentialsProvider.get());
  }
}
