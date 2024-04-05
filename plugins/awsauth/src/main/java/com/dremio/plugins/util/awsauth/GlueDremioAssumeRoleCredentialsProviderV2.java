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

import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/** Glue specific wrapper */
public class GlueDremioAssumeRoleCredentialsProviderV2
    implements AwsCredentialsProvider, AutoCloseable {
  private Closeable swapClassLoader() {
    return ContextClassLoaderSwapper.swapClassLoader(HadoopFileSystem.class);
  }

  @Override
  public AwsCredentials resolveCredentials() {

    try (Closeable ignored = swapClassLoader()) {
      final com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV2 provider =
          new com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV2();
      return provider.resolveCredentials();
    }
  }

  @Override
  public void close() throws Exception {}
}
