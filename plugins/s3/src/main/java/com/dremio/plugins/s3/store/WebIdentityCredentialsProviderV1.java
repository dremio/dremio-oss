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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;

import software.amazon.awssdk.core.SdkSystemSetting;

public class WebIdentityCredentialsProviderV1 implements AWSCredentialsProvider, Closeable {
  private final STSAssumeRoleWithWebIdentitySessionCredentialsProvider stsAssumeRoleWithWebIdentitySessionCredentialsProvider;
  private static final Logger logger = LoggerFactory.getLogger(WebIdentityCredentialsProviderV1.class);
  private final static String DREMIO_SESSION_NAME = "dremio-session";

  public WebIdentityCredentialsProviderV1(Configuration conf) throws IOException {
    String webIdentityTokenFile = System.getenv(SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE.name());
    String assumeRole = System.getenv(SdkSystemSetting.AWS_ROLE_ARN.name());
    String sessionName = System.getenv(SdkSystemSetting.AWS_ROLE_SESSION_NAME.name());
    if (sessionName == null) {
      sessionName = DREMIO_SESSION_NAME;
    }
    logger.debug(assumeRole, sessionName, webIdentityTokenFile);
    this.stsAssumeRoleWithWebIdentitySessionCredentialsProvider = new STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder(assumeRole, sessionName, webIdentityTokenFile).build();
  }

  @Override
  public AWSCredentials getCredentials() {
    return stsAssumeRoleWithWebIdentitySessionCredentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    this.stsAssumeRoleWithWebIdentitySessionCredentialsProvider.refresh();
  }

  @Override
  public void close() throws IOException {
    stsAssumeRoleWithWebIdentitySessionCredentialsProvider.close();
  }
}
