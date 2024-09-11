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
package com.dremio.exec.store.hive;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.dremio.hadoop.security.alias.DremioCredentialProviderFactory;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

/**
 * Glue specific implementation of {@link AWSCredentialsProvider} to avoid class loader issues. Uses
 * DremioCredentialProviderFactory to resolve secrets.
 */
public class GlueAWSCredentialsProvider implements AWSCredentialsProvider {
  private final Configuration config;
  private final String accessKey;
  private final String secretKey;
  private final Supplier<AWSCredentials> credentialsSupplier;

  public GlueAWSCredentialsProvider(final URI uri, final Configuration config) {
    this.config = config;
    this.accessKey = Constants.ACCESS_KEY;
    this.secretKey = Constants.SECRET_KEY;
    this.credentialsSupplier =
        Suppliers.memoizeWithExpiration(this::getCredentialsInternal, 5, TimeUnit.MINUTES);
  }

  @Override
  public AWSCredentials getCredentials() {
    return credentialsSupplier.get();
  }

  private AWSCredentials getCredentialsInternal() {
    try {
      return new BasicAWSCredentials(lookupPassword(accessKey), lookupPassword(secretKey));
    } catch (IllegalArgumentException e) {
      throw new AmazonClientException("Access key or secret key is null", e);
    }
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

  @Override
  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
