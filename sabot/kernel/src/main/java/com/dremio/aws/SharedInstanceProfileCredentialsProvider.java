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
package com.dremio.aws;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;

/**
 * A shared instance of {@link InstanceProfileCredentialsProvider} to avoid creating too
 * many instances at once and sending too many queries to the metadata server.
 */
public class SharedInstanceProfileCredentialsProvider implements AwsCredentialsProvider {
  // Internal instance holder to lazy create the singleton
  // Singleton pattern was chosen as InstanceProfileCredentialsProvider automatically handles periodic
  // refresh of credentials so the number of HTTP requests to the metadata server are quite limited
  private static final class Holder {
    // This instance should be closed once unused but since its lifetime is bound to the jvm, nothing
    // special is done.
    private static final AwsCredentialsProvider INSTANCE = InstanceProfileCredentialsProvider.create();
  }

  public SharedInstanceProfileCredentialsProvider() {
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return Holder.INSTANCE.resolveCredentials();
  }
}
