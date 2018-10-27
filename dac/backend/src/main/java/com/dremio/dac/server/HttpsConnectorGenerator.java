/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.dremio.config.DremioConfig;
import com.dremio.exec.rpc.ssl.SSLConfig;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.google.common.base.Preconditions;

/**
 * Helper class that generates an {@link ServerConnector} with SSL.
 */
public class HttpsConnectorGenerator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpsConnectorGenerator.class);

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
    if (sslConfig.getTrustStorePath() != null) {
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
    // Disable ciphers, protocols and other that are considered weak/vulnerable
    sslContextFactory.setExcludeCipherSuites(
        "TLS_DHE.*",
        "TLS_EDH.*"
        // TODO(DX-12920): there are few other ciphers that Chrome complains about being obsolete. Research more about
        // them and include here.
    );
    sslContextFactory.setExcludeProtocols("SSLv3");
    sslContextFactory.setRenegotiationAllowed(false);
    // TODO(DX-12920): sslContextFactory.setValidateCerts(true); to ensure that the server starts up with a valid
    // certificate
    // TODO(DX-12920): sslContextFactory.setValidatePeerCerts(!sslConfig.disableCertificateVerification());

    final ServerConnector sslConnector =
        new ServerConnector(
            embeddedJetty,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(new HttpConfiguration())
        );

    return Pair.of(sslConnector, trustStore);
  }
}
