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
package com.dremio.plugins.elastic;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.dremio.exec.catalog.conf.SecretRef;
import com.google.common.base.Suppliers;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;

/**
 * Copied from hadoop-aws to avoid dependency of it, which includes aws-java-sdk 1.7.4 that is not
 * compatible with elasticsearch plugin for Amazon Elasticsearch Service. Added SecretRef support.
 */
public class BasicAWSCredentialsProvider implements AWSCredentialsProvider {
  private final String accessKey;
  private final SecretRef secretKey;
  private final Supplier<AWSCredentials> credentialsSupplier;

  public BasicAWSCredentialsProvider(String accessKey, SecretRef secretKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.credentialsSupplier =
        Suppliers.memoizeWithExpiration(this::getCredentialsInternal, 5, TimeUnit.MINUTES);
  }

  @Override
  public AWSCredentials getCredentials() {
    return credentialsSupplier.get();
  }

  private AWSCredentials getCredentialsInternal() {
    if (!StringUtils.isEmpty(accessKey) && !SecretRef.isEmpty(secretKey)) {
      return new BasicAWSCredentials(accessKey, secretKey.get());
    }
    throw new AmazonClientException("Access key or secret key is null");
  }

  @Override
  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
