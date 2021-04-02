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
package com.dremio.ssl;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.netty.handler.ssl.util.SimpleTrustManagerFactory;

/**
 * A factory for creating a TrustManager that checks if certificates
 * validate against multiple sources.
 */
public class CompositeTrustManagerFactory extends SimpleTrustManagerFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompositeTrustManagerFactory.class);

  private final CompositeTrustManager trustManager;

  private CompositeTrustManagerFactory(Builder builder) {
    this.trustManager = new CompositeTrustManager(builder.trustManagersBuilder.build());
  }

  @Override
  protected void engineInit(KeyStore keyStore) throws Exception {
  }

  @Override
  protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws Exception {
  }

  @Override
  protected TrustManager[] engineGetTrustManagers() {
    return new TrustManager[] {trustManager};
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<X509ExtendedTrustManager> trustManagersBuilder = ImmutableList.builder();

    private Builder() {
    }

    /**
     * Add the default JVM trust store to the CompositeTrustManagerFactory.
     */
    public Builder addDefaultTrustStore() {
      return addTrustStore((KeyStore) null);
    }

    /**
     * Add a trust store to the CompositeTrustManagerFactory.
     * The load() function must have already been called if it is required.
     */
    public Builder addTrustStore(KeyStore keyStore) {
      try {
        toX509TrustManager(keyStore).ifPresent(trustManagersBuilder::add);
      } catch (KeyStoreException ex) {
        logger.warn("Unable to load KeyStore of type {}", keyStore, ex);
      }
      return this;
    }

    @VisibleForTesting
    Builder addTrustStore(Iterable<X509ExtendedTrustManager> managers) {
      trustManagersBuilder.addAll(managers);
      return this;
    }

    public CompositeTrustManagerFactory build() {
      return new CompositeTrustManagerFactory(this);
    }

    private static Optional<X509ExtendedTrustManager> toX509TrustManager(KeyStore keyStore)
      throws KeyStoreException {
      try {
        final TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(keyStore);
        return Arrays.stream(factory.getTrustManagers())
          .filter(tm -> tm instanceof X509ExtendedTrustManager)
          .map(tm -> (X509ExtendedTrustManager) tm)
          .findFirst();
      } catch (NoSuchAlgorithmException ex) {
        // This can't happen as we are using the TrustManagerFactory's default algorithm.
        return Optional.empty();
      }
    }
  }
}
