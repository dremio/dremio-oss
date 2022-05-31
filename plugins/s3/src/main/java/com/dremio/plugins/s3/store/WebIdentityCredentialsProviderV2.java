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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.SdkAutoCloseable;

public class WebIdentityCredentialsProviderV2 implements AwsCredentialsProvider, SdkAutoCloseable {
  private final WebIdentityTokenFileCredentialsProvider webIdentityTokenCredentialsProvider;
  private static final Logger logger = LoggerFactory.getLogger(WebIdentityCredentialsProviderV2.class);

  public WebIdentityCredentialsProviderV2(Configuration conf) {
    this.webIdentityTokenCredentialsProvider = WebIdentityTokenFileCredentialsProvider.create();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return webIdentityTokenCredentialsProvider.resolveCredentials();
  }

  @Override
  public void close() {
    IoUtils.closeIfCloseable(webIdentityTokenCredentialsProvider, logger);
  }
}
