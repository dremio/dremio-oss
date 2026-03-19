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
package com.dremio.plugins.util;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;

/** Tests for {@link AwsCredentialProviderUtils} */
public class AwsCredentialProviderUtilsTest {

  @Test
  public void testContainerCredentialsProvider() {
    Configuration config = new Configuration();
    config.set(
        Constants.AWS_CREDENTIALS_PROVIDER,
        "software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider");

    AwsCredentialsProvider provider = AwsCredentialProviderUtils.getCredentialsProvider(config);
    assertTrue(provider instanceof ContainerCredentialsProvider);
  }

  @Test
  public void testNoneProvider() {
    Configuration config = new Configuration();
    config.set(
        Constants.AWS_CREDENTIALS_PROVIDER,
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");

    AwsCredentialsProvider provider = AwsCredentialProviderUtils.getCredentialsProvider(config);
    assertTrue(provider instanceof AnonymousCredentialsProvider);
  }

  @Test
  public void testInvalidProviderThrowsException() {
    Configuration config = new Configuration();
    config.set(Constants.AWS_CREDENTIALS_PROVIDER, "com.invalid.Provider");

    assertThrows(
        IllegalStateException.class,
        () -> AwsCredentialProviderUtils.getCredentialsProvider(config));
  }
}
