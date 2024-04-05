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
package com.dremio.plugins.s3.store;

import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AWSCredentials;
import com.dremio.plugins.util.AwsProfile;
import com.dremio.test.TemporaryEnvironment;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for AWSProfileCredentialsProviderV1 */
public class TestAWSProfileCredentialsProviderV1 {
  @ClassRule public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TemporaryEnvironment temporaryEnvironment = new TemporaryEnvironment();

  private static final AwsProfile DEFAULT_PROFILE =
      new AwsProfile("default", "defaultId", "defaultSecret");
  private static final AwsProfile PROFILE_1 =
      new AwsProfile("profile1", "profile1Id", "profile1Secret");

  @BeforeClass
  public static void setup() throws IOException {
    // Create and write to credentials file
    temporaryFolder.create();
    File credentialsFile = temporaryFolder.newFile("credentials");
    try (FileWriter fileWriter = new FileWriter(credentialsFile)) {
      DEFAULT_PROFILE.write(fileWriter);
      PROFILE_1.write(fileWriter);
    }

    // Tell AWS SDK where to find credentials file
    temporaryEnvironment.setEnvironmentVariable(
        "AWS_CREDENTIAL_PROFILES_FILE", credentialsFile.getAbsolutePath());
  }

  @Test
  public void noProfileUsesDefaultProfile() {
    // Arrange
    Configuration conf = new Configuration();

    AWSProfileCredentialsProviderV1 provider = new AWSProfileCredentialsProviderV1(conf);

    // Act
    AWSCredentials credentials = provider.getCredentials();

    // Assert
    assertEquals(DEFAULT_PROFILE.getAwsAccessKeyId(), credentials.getAWSAccessKeyId());
    assertEquals(DEFAULT_PROFILE.getAwsSecretKey(), credentials.getAWSSecretKey());
  }

  @Test
  public void defaultProfile() {
    // Arrange
    Configuration conf = new Configuration();
    conf.set("com.dremio.awsProfile", DEFAULT_PROFILE.getProfileName());

    AWSProfileCredentialsProviderV1 provider = new AWSProfileCredentialsProviderV1(conf);

    // Act
    AWSCredentials credentials = provider.getCredentials();

    // Assert
    assertEquals(DEFAULT_PROFILE.getAwsAccessKeyId(), credentials.getAWSAccessKeyId());
    assertEquals(DEFAULT_PROFILE.getAwsSecretKey(), credentials.getAWSSecretKey());
  }

  @Test
  public void specifiedProfile() {
    // Arrange
    Configuration conf = new Configuration();
    conf.set("com.dremio.awsProfile", PROFILE_1.getProfileName());

    AWSProfileCredentialsProviderV1 provider = new AWSProfileCredentialsProviderV1(conf);

    // Act
    AWSCredentials credentials = provider.getCredentials();

    // Assert
    assertEquals(PROFILE_1.getAwsAccessKeyId(), credentials.getAWSAccessKeyId());
    assertEquals(PROFILE_1.getAwsSecretKey(), credentials.getAWSSecretKey());
  }
}
