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
package com.dremio.dac.server;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.dremio.config.DremioConfig;
import com.dremio.exec.rpc.ssl.SSLConfig;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

/**
 * Helper class that generates an {@link ServerConnector} with SSL.
 */
public class HttpsConnectorGenerator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpsConnectorGenerator.class);
  private static final String DREMIO_SSL_CIPHERSUITE_OVERRIDE = "dremio_ciphersuite";

  /**
   * Create an HTTPS connector for given jetty server instance. If the config has specified keystore/truststore settings
   * they will be used else a self-signed certificate is generated and used.
   *
   * @param hostName      hostname
   * @param config        {@link DremioConfig} containing SSL related settings if any.
   * @param embeddedJetty Jetty server instance needed for creating a ServerConnector.
   * @return Initialized {@link ServerConnector} for HTTPS connections and the trust store. Trust store is non-null only
   * when in case of auto generated self-signed certificate.
   * @throws Exception
   */
  public Pair<ServerConnector, KeyStore> createHttpsConnector(
      final Server embeddedJetty,
      final DremioConfig config,
      final String hostName,
      final String... alternativeNames
  ) throws Exception {
    logger.info("Setting up HTTPS connector for web server");

    final SSLConfigurator configurator = new SSLConfigurator(config, DremioConfig.WEB_SSL_PREFIX, "web");
    final Optional<SSLConfig> sslConfigOption = configurator.getSSLConfig(true, hostName, alternativeNames);
    Preconditions.checkState(sslConfigOption.isPresent()); // caller's responsibility
    final SSLConfig sslConfig = sslConfigOption.get();

    final KeyStore keyStore = KeyStore.getInstance(sslConfig.getKeyStoreType());
    try (InputStream stream = Files.newInputStream(Paths.get(sslConfig.getKeyStorePath()))) {
      keyStore.load(stream, sslConfig.getKeyStorePassword().toCharArray());
    }

    KeyStore trustStore = null;
    //noinspection StringEquality
    if (sslConfig.getTrustStorePath() != SSLConfig.UNSPECIFIED) {
      trustStore = KeyStore.getInstance(sslConfig.getTrustStoreType());
      try (InputStream stream = Files.newInputStream(Paths.get(sslConfig.getTrustStorePath()))) {
        trustStore.load(stream, sslConfig.getTrustStorePassword().toCharArray());
      }
    }

    final SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStore(keyStore);
    sslContextFactory.setKeyManagerPassword(sslConfig.getKeyPassword());
    // TODO(DX-12920): sslContextFactory.setKeyStorePassword(sslConfig.getKeyStorePassword());
    sslContextFactory.setTrustStore(trustStore);

    final String[] enabledCiphers;
    final String customCipherSuite = System.getProperty(DREMIO_SSL_CIPHERSUITE_OVERRIDE);
    if (customCipherSuite != null) {
      logger.info("Using custom cipher list for web server");
      enabledCiphers = Splitter.on(",")
          .trimResults()
          .omitEmptyStrings()
          .splitToList(customCipherSuite)
          .toArray(new String[0]);
      logger.info("Selected cipher list: {}", Arrays.toString(enabledCiphers));
    } else {
      /* By default, only enable the OWASP broad compatibility list of cipher suites, the order listed
       * is the preferred priority of the cipher suites.
       * TLS 1.3 is not supported in JDK 8, but the first three ciphers are still included for future compatibility.
       *
       * See: https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets/TLS_Cipher_String_Cheat_Sheet.md
       */
      enabledCiphers = new String[] {
        "TLS_AES_256_GCM_SHA384", // TLS 1.3
        "TLS_CHACHA20_POLY1305_SHA256", // TLS 1.3
        "TLS_AES_128_GCM_SHA256", // TLS 1.3
        "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
      };
    }
    sslContextFactory.setIncludeCipherSuites(enabledCiphers);
    sslContextFactory.setRenegotiationAllowed(false);

    // TODO(DX-12920): sslContextFactory.setValidateCerts(true); to ensure that the server starts up with a valid
    // certificate
    // TODO(DX-12920): sslContextFactory.setValidatePeerCerts(!sslConfig.disableCertificateVerification());

    // this ensures that jersey is aware that we are using https - without this it thinks that every connection is unsecured
    final HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSecureScheme("https");
    httpConfig.addCustomizer(new SecureRequestCustomizer());

    final ServerConnector sslConnector =
      new ServerConnector(
        embeddedJetty,
        new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
        new HttpConnectionFactory(httpConfig)
      );

    return Pair.of(sslConnector, trustStore);
  }
}
