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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Optional;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Default implementation.
 */
public class SSLEngineFactoryImpl implements SSLEngineFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLEngineFactoryImpl.class);

  private static final String SSL_PROVIDER_PROPERTY = "dremio.ssl.provider";
  private static final String DEFAULT_SSL_PROVIDER;
  private static final SslProvider SSL_PROVIDER;

  private static final String SSL_PROTOCOLS_PROPERTY = "dremio.ssl.protocols";
  private static final String DEFAULT_SSL_PROTOCOLS = "TLSv1.2";
  private static final String[] SSL_PROTOCOLS;

  private static final String SSL_CIPHERS_PROPERTY = "dremio.ssl.ciphers";
  private static final Iterable<String> SSL_CIPHERS;

  static {
    DEFAULT_SSL_PROVIDER = OpenSsl.isAvailable() ? SslProvider.OPENSSL.name() : SslProvider.JDK.name();

    SSL_PROVIDER = SslProvider.valueOf(System.getProperty(SSL_PROVIDER_PROPERTY, DEFAULT_SSL_PROVIDER));

    SSL_PROTOCOLS = System.getProperty(SSL_PROTOCOLS_PROPERTY, DEFAULT_SSL_PROTOCOLS).split(",");

    final String cipherString = System.getProperty(SSL_CIPHERS_PROPERTY);
    Iterable<String> ciphers = null; // defaults to null; see SSlContextBuilder#ciphers
    if (cipherString != null) {
      ciphers = ImmutableList.copyOf(cipherString.split(","));
    }
    SSL_CIPHERS = ciphers;
  }

  private final SSLConfig sslConfig;

  private final KeyManagerFactory keyManagerFactory;
  private final TrustManagerFactory trustManagerFactory;

  private SSLEngineFactoryImpl(SSLConfig sslConfig) throws SSLException {
    this.sslConfig = sslConfig;

    try {
      keyManagerFactory = newKeyManagerFactory();
      trustManagerFactory = newTrustManagerFactory();
    } catch (GeneralSecurityException | IOException e) {
      throw new SSLException(e);
    }
  }

  private KeyManagerFactory newKeyManagerFactory() throws GeneralSecurityException, IOException {
    if (sslConfig.getKeyStorePath() == SSLConfig.UNSPECIFIED) {
      return null;
    }

    final KeyStore keyStore = KeyStore.getInstance(sslConfig.getKeyStoreType());
    try (InputStream stream = new FileInputStream(sslConfig.getKeyStorePath())) {
      keyStore.load(stream, sslConfig.getKeyStorePassword().toCharArray());
    }

    if (keyStore.size() == 0) {
      throw new IllegalArgumentException("Key store has no entries");
    }

    final KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    factory.init(keyStore, sslConfig.getKeyPassword().toCharArray());
    return factory;
  }

  private TrustManagerFactory newTrustManagerFactory() throws GeneralSecurityException, IOException {
    final KeyStore trustStore;
    if (sslConfig.getTrustStorePath() == SSLConfig.UNSPECIFIED) {
      // uses JDK default
      // see https://docs.oracle.com/javase/1.5.0/docs/guide/security/jsse/JSSERefGuide.html#X509TrustManager
      trustStore = null;
    } else {
      trustStore = KeyStore.getInstance(sslConfig.getTrustStoreType());
      try (InputStream stream = new FileInputStream(sslConfig.getTrustStorePath())) {
        trustStore.load(stream, sslConfig.getTrustStorePassword().toCharArray());
      }
    }

    final TrustManagerFactory factory;
    if (sslConfig.disablePeerVerification()) {
      factory = InsecureTrustManagerFactory.INSTANCE;
    } else {
      factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    }

    factory.init(trustStore);
    return factory;
  }

  @Override
  public SslContextBuilder newServerContextBuilder() throws SSLException {
    return SslContextBuilder.forServer(keyManagerFactory)
      .trustManager(trustManagerFactory)
      .clientAuth(sslConfig.disablePeerVerification() ? ClientAuth.OPTIONAL : ClientAuth.REQUIRE)
      .sslProvider(SSL_PROVIDER)
      .protocols(SSL_PROTOCOLS)
      .ciphers(SSL_CIPHERS);
  }

  @Override
  public SSLEngine newServerEngine(ByteBufAllocator allocator, String peerHost, int peerPort)
    throws SSLException {
    final SslContext sslContext = newServerContextBuilder().build();

    final SSLEngine engine = sslContext.newEngine(allocator, peerHost, peerPort);
    try {
      engine.setEnableSessionCreation(true);
    } catch (UnsupportedOperationException ignored) {
      // see ReferenceCountedOpenSslEngine#setEnableSessionCreation
      logger.trace("Session creation not enabled", ignored);
    }

    return engine;
  }

  @Override
  public SslContextBuilder newClientContextBuilder() throws SSLException {
    return SslContextBuilder.forClient()
      .keyManager(keyManagerFactory)
      .trustManager(trustManagerFactory)
      .sslProvider(SSL_PROVIDER)
      .protocols(SSL_PROTOCOLS)
      .ciphers(SSL_CIPHERS);
  }

  @Override
  public SSLEngine newClientEngine(ByteBufAllocator allocator, String peerHost, int peerPort)
    throws SSLException {
    final SslContext sslContext = newClientContextBuilder().build();

    final SSLEngine engine = sslContext.newEngine(allocator, peerHost, peerPort);
    final SSLParameters sslParameters = engine.getSSLParameters();
    sslParameters.setServerNames(Collections.singletonList(new SNIHostName(peerHost)));

    if (!sslConfig.disableHostVerification()) {
      // only available since Java 7
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
    }

    engine.setSSLParameters(sslParameters);

    try {
      engine.setEnableSessionCreation(true);
    } catch (UnsupportedOperationException ignored) {
      // see ReferenceCountedOpenSslEngine#setEnableSessionCreation
      logger.trace("Session creation not enabled", ignored);
    }

    return engine;
  }

  /**
   * Creates an instance of this factory.
   *
   * @param sslConfig SSL config
   * @return ssl engine factory
   * @throws SSLException if creation fails
   */
  public static Optional<SSLEngineFactory> create(Optional<SSLConfig> sslConfig) throws SSLException {
    return sslConfig.isPresent() ? Optional.of(new SSLEngineFactoryImpl(sslConfig.get())) : Optional.empty();
  }
}
