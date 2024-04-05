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

import java.io.IOException;
import java.io.Writer;

/**
 * Struct-like object for an AwsProfile. Use write() to output a format suitable for an AWS
 * Credentials file.
 */
public final class AwsProfile {
  private final String profileName;
  private final String awsAccessKeyId;
  private final String awsSecretKey;

  public AwsProfile(String profileName, String awsAccessKeyId, String awsSecretKey) {
    this.profileName = profileName;
    this.awsAccessKeyId = awsAccessKeyId;
    this.awsSecretKey = awsSecretKey;
  }

  public String getProfileName() {
    return profileName;
  }

  public String getAwsAccessKeyId() {
    return awsAccessKeyId;
  }

  public String getAwsSecretKey() {
    return awsSecretKey;
  }

  /**
   * Warning: this will write the secret in plaintext.
   *
   * @param writer The writer.
   * @throws IOException if write files.
   */
  public void write(Writer writer) throws IOException {
    final String formatString =
        "[%s]\n" + "aws_access_key_id = %s\n" + "aws_secret_access_key = %s\n" + "\n";
    writer.write(String.format(formatString, profileName, awsAccessKeyId, awsSecretKey));
  }
}
