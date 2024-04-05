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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.service.Pointer;
import java.util.Map;

/**
 * Glue specific wrapper to {@link DremioAssumeRoleCredentialsProviderV1} to avoid class loader
 * issues.
 */
public class GlueDremioAssumeRoleCredentialsProviderV1
    implements AWSCredentialsProvider, AutoCloseable {
  private Closeable swapClassLoader() {
    return ContextClassLoaderSwapper.swapClassLoader(HadoopFileSystem.class);
  }

  @Override
  public AWSCredentials getCredentials() {
    Pointer<String> accessKey = new Pointer<>("");
    Pointer<String> secretKey = new Pointer<>("");
    Pointer<String> sessionToken = new Pointer<>("");

    try (Closeable ignored = swapClassLoader()) {
      final com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV1 provider =
          new com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV1();
      Map<String, String> credentialsMap = provider.getCredentialsMap();
      accessKey.value = credentialsMap.get(DremioAssumeRoleCredentialsProviderV1.ACCESS_KEY);
      secretKey.value = credentialsMap.get(DremioAssumeRoleCredentialsProviderV1.SECRET_KEY);
      sessionToken.value = credentialsMap.get(DremioAssumeRoleCredentialsProviderV1.SESSION_TOKEN);
    }
    return new AWSSessionCredentials() {
      @Override
      public String getAWSAccessKeyId() {
        return accessKey.value;
      }

      @Override
      public String getAWSSecretKey() {
        return secretKey.value;
      }

      @Override
      public String getSessionToken() {
        return sessionToken.value;
      }
    };
  }

  @Override
  public void refresh() {}

  @Override
  public void close() throws Exception {}
}
