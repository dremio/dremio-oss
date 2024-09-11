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

import com.dremio.hadoop.security.alias.DremioCredentialProviderFactory;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Glue specific implementation of {@link AwsCredentialsProvider} to avoid class loader issues. Uses
 * DremioCredentialProviderFactory to resolve secrets.
 */
public class GlueAwsCredentialsProviderV2 implements AwsCredentialsProvider {
  private final Configuration config;
  private final String accessKey;
  private final String secretKey;
  private final Supplier<AwsCredentials> credentialsSupplier;

  public GlueAwsCredentialsProviderV2(final URI uri, final Configuration config) {
    this.config = config;
    this.accessKey = Constants.ACCESS_KEY;
    this.secretKey = Constants.SECRET_KEY;
    this.credentialsSupplier =
        Suppliers.memoizeWithExpiration(this::resolveCredentialsInternal, 5, TimeUnit.MINUTES);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentialsSupplier.get();
  }

  private AwsCredentials resolveCredentialsInternal() {
    return AwsBasicCredentials.create(lookupPassword(accessKey), lookupPassword(secretKey));
  }

  private String lookupPassword(String alias) {
    final String pattern = config.get(alias);
    try {
      char[] credential = DremioCredentialProviderFactory.resolveCredentialPattern(alias, pattern);
      if (credential != null) {
        return String.valueOf(credential);
      }
      return pattern;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
