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

import static com.dremio.config.DremioConfig.LOCAL_WRITE_PATH_STRING;
import static com.dremio.config.DremioConfig.WEB_SSL_AUTOCERTIFICATE_ENABLED_BOOL;
import static com.dremio.config.DremioConfig.WEB_SSL_KEYSTORE;
import static com.dremio.config.DremioConfig.WEB_SSL_KEYSTORE_PASSWORD;
import static com.dremio.config.DremioConfig.WEB_SSL_TRUSTSTORE;
import static com.dremio.config.DremioConfig.WEB_SSL_TRUSTSTORE_PASSWORD;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.TrustedCertificateEntry;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.joda.time.DateTime;

import com.dremio.config.DremioConfig;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;

/**
 * Helper class that generates an {@link ServerConnector} with SSL.
 */
public class HttpsConnectorGenerator {
  private static final String UNSECURE_PASSWORD = "averylongandunsecurepasswordfordremiokeystore";
  private static final char[] UNSECURE_PASSWORD_CHARARRAY = UNSECURE_PASSWORD.toCharArray();

  static final String KEY_STORE_DIRECTORY = "security";
  static final Set<PosixFilePermission> KEY_STORE_DIRECTORY_PERMISSIONS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE);

  static final String KEY_STORE_FILE = "keystore";
  static final Set<PosixFilePermission> KEY_STORE_FILE_PERMISSIONS =
    EnumSet.of(OWNER_READ, OWNER_WRITE);

  static final String TRUST_STORE_FILE = "certs";
  static final Set<PosixFilePermission> TRUST_STORE_FILE_PERMISSIONS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, GROUP_READ, OTHERS_READ);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpsConnectorGenerator.class);

  private Pair<KeyStore, String> getKeyStore(final DremioConfig config, final String hostName,
                                             final String... alternativeHostNames)
    throws GeneralSecurityException, IOException {
    final String keyStorePath = getConfig(config, WEB_SSL_KEYSTORE);
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    if (keyStorePath != null) {
      logger.info("Loading configured keystore at {}", keyStorePath);
      String password = getConfig(config, WEB_SSL_KEYSTORE_PASSWORD);
      try(InputStream stream = Files.newInputStream(Paths.get(keyStorePath))) {
        keyStore.load(stream, password != null ? password.toCharArray() : null);
      }
      return Pair.of(keyStore, password);
    }


    if (!config.getBoolean(WEB_SSL_AUTOCERTIFICATE_ENABLED_BOOL)) {
      throw new RuntimeException("No keystore configured and certificate auto-generation is disabled");
    }

    logger.warn("*** Using generated self-signed SSL settings for web server ***\n"
        + "Using auto-generated certificates is not secure. Please consider switching to your own certificates.");

    final String localWritePathString = config.getString(LOCAL_WRITE_PATH_STRING);
    // Get default path for internal keystore
    final Path path = Paths.get(localWritePathString, KEY_STORE_DIRECTORY, KEY_STORE_FILE);
    if (Files.exists(path)) {
      try(InputStream stream = Files.newInputStream(path)) {
        keyStore.load(stream, UNSECURE_PASSWORD_CHARARRAY);
      }

      /*
       * Previously key store and trust store were in the KEY_STORE_DIRECTORY. This function moves
       * the trust store from KEY_STORE_DIRECTORY to local data directory, if necessary, and
       * sets appropriate permissions on the file.
       */
      moveTrustStoreIfNecessary(localWritePathString);

      return Pair.of(keyStore, UNSECURE_PASSWORD);
    }

    Files.createDirectories(path.getParent(), PosixFilePermissions.asFileAttribute(KEY_STORE_DIRECTORY_PERMISSIONS));
    // Initialialize a new keystore, along with a truststore for clients
    keyStore.load(null, null);
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);

    logger.info("No previous keystore detected, creating certificate. The operation might take time...");
    final SecureRandom random = new SecureRandom();

    // Generate a private-public 2048-bit key pair
    final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048, random);
    final KeyPair keyPair = keyPairGenerator.generateKeyPair();

    final DateTime now = DateTime.now();

    // Create builder for certificate attributes
    final X500NameBuilder nameBuilder = new X500NameBuilder(BCStyle.INSTANCE)
        .addRDN(BCStyle.CN, hostName)
        .addRDN(BCStyle.OU, "Dremio Corp. (auto-generated)")
        .addRDN(BCStyle.O, "Dremio Corp. (auto-generated)")
        .addRDN(BCStyle.L, "Mountain View")
        .addRDN(BCStyle.ST, "California")
        .addRDN(BCStyle.C, "US");

    final Date notBefore = now.minusDays(1).toDate();
    final Date notAfter = now.plusYears(1).toDate();
    final BigInteger serialNumber = new BigInteger(128, random);

    // Create a certificate valid for 1 years from now.
    // Add the main hostname + the alternative hostnames to the SAN extension.
    GeneralName[] alternativeSubjectNames = new GeneralName[alternativeHostNames.length + 1];
    alternativeSubjectNames[0] = new GeneralName(GeneralName.dNSName, hostName);
    for(int i = 0; i<alternativeHostNames.length; i++) {
      alternativeSubjectNames[i + 1] = new GeneralName(GeneralName.dNSName, alternativeHostNames[i]);
    }
    final X509v3CertificateBuilder certificateBuilder =
        new JcaX509v3CertificateBuilder(
            nameBuilder.build(),
            serialNumber,
            notBefore,
            notAfter,
            nameBuilder.build(),
            keyPair.getPublic()
        )
        .addExtension(
            Extension.subjectAlternativeName,
            false,
            new DERSequence(alternativeSubjectNames));

    // Sign the certificate using the private key
    final ContentSigner contentSigner;
    try {
      contentSigner = new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());
    } catch (OperatorCreationException e) {
      throw new GeneralSecurityException(e);
    }
    final X509Certificate certificate =
        new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner));

    // Check the validity
    certificate.checkValidity(now.toDate());

    // Make sure the certificate is self-signed.
    certificate.verify(certificate.getPublicKey());

    String fingerprint = BaseEncoding
        .base16()
        .withSeparator(":", 2)
        .encode(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
    logger.info("Certificate created (SHA-256 fingerprint: {})", fingerprint);

    // Generate a random password for keystore protection
    keyStore.setKeyEntry(
        "DremioAutoGeneratedPrivateKey",
        keyPair.getPrivate(),
        UNSECURE_PASSWORD_CHARARRAY,
        new java.security.cert.Certificate[] { certificate }
    );

    // Add random number to cert name to avoid collisions between certs in multinode cluster tests.
    trustStore.setEntry("DremioAutoGeneratedCert" + RandomStringUtils.randomNumeric(5),
        new TrustedCertificateEntry(certificate), null);

    // Save stores
    try(OutputStream stream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)) {
      keyStore.store(stream, UNSECURE_PASSWORD_CHARARRAY);
    }
    Files.setPosixFilePermissions(path, KEY_STORE_FILE_PERMISSIONS);

    final Path trustStorePath = Paths.get(localWritePathString, TRUST_STORE_FILE);
    try(OutputStream stream = Files.newOutputStream(trustStorePath, StandardOpenOption.CREATE_NEW)) {
      trustStore.store(stream, UNSECURE_PASSWORD_CHARARRAY);
    }
    Files.setPosixFilePermissions(trustStorePath, TRUST_STORE_FILE_PERMISSIONS);

    return Pair.of(keyStore, UNSECURE_PASSWORD);
  }

  private static void moveTrustStoreIfNecessary(final String localWritePath) {
    final Path toPath = Paths.get(localWritePath, TRUST_STORE_FILE);
    if (Files.exists(toPath)) {
      return; // already moved
    }

    final Path fromPath = Paths.get(localWritePath, KEY_STORE_DIRECTORY, TRUST_STORE_FILE);
    Preconditions.checkState(Files.exists(fromPath));
    logger.info("Moving trust store from '{}' to '{}'", fromPath, toPath);
    try {
      Files.move(fromPath, toPath);
    } catch (IOException e) {
      logger.error(String.format("Failed to move trust store from '%s' to '%s'. " +
          "Please do so manually. Also, set permissions to 644 on trust store.",
          fromPath, toPath), e);
      return;
    }

    try {
      Files.setPosixFilePermissions(toPath, TRUST_STORE_FILE_PERMISSIONS);
    } catch (IOException e) {
      logger.error(String.format("Failed to set 644 permissions on trust store at '%s'." +
          " Please do so manually.", toPath), e);
    }
  }

  public KeyStore getTrustStore(DremioConfig config) throws GeneralSecurityException, IOException {
    String trustStorePath = getConfig(config, WEB_SSL_TRUSTSTORE);
    char[] trustStorePassword;
    if (trustStorePath == null) {
      // Check if auto-generated certificates are used
      Path path = Paths.get(config.getString(LOCAL_WRITE_PATH_STRING), TRUST_STORE_FILE);
      if (Files.notExists(path)) {
        return null;
      }

      trustStorePath = path.toString();
      trustStorePassword = UNSECURE_PASSWORD_CHARARRAY;
    } else {
      logger.info("Loading configured truststore at {}", trustStorePath);
      String password = getConfig(config, WEB_SSL_TRUSTSTORE_PASSWORD);
      trustStorePassword = password != null ? password.toCharArray() : null;
    }

    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    try(InputStream stream = Files.newInputStream(Paths.get(trustStorePath))) {
      trustStore.load(stream, trustStorePassword);
    }
    return trustStore;
  }

  /**
   * Create an HTTPS connector for given jetty server instance. If the config has specified keystore/truststore settings
   * they will be used else a self-signed certificate is generated and used.
   *
   * @param hostName
   * @param config {@link DremioConfig} containing SSL related settings if any.
   * @param embeddedJetty Jetty server instance needed for creating a ServerConnector.
   *
   * @return Initialized {@link ServerConnector} for HTTPS connections and the trust store. Trust store is non-null only
   * when in case of auto generated self-signed certificate.
   * @throws Exception
   */
  public Pair<ServerConnector, KeyStore> createHttpsConnector(final Server embeddedJetty,
      final DremioConfig config, final String hostName, final String... alternativeNames) throws Exception {
    logger.info("Setting up HTTPS connector for web server");

    final SslContextFactory sslContextFactory = new SslContextFactory();

    Pair<KeyStore, String> keyStore = getKeyStore(config, hostName, alternativeNames);
    KeyStore trustStore = getTrustStore(config);

    sslContextFactory.setKeyStore(keyStore.getLeft());
    // Assuming that the keystore and the keymanager passwords are the same
    // based on JSSE examples...
    sslContextFactory.setKeyManagerPassword(keyStore.getRight());
    sslContextFactory.setTrustStore(trustStore);

    // Disable ciphers, protocols and other that are considered weak/vulnerable
    sslContextFactory.setExcludeCipherSuites(
        "TLS_DHE.*",
        "TLS_EDH.*"
        // TODO: there are few other ciphers that Chrome complains about being obsolete. Research more about them and
        // include here.
    );

    sslContextFactory.setExcludeProtocols("SSLv3");
    sslContextFactory.setRenegotiationAllowed(false);

    // SSL Connector
    final ServerConnector sslConnector = new ServerConnector(embeddedJetty,
        new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
        new HttpConnectionFactory(new HttpConfiguration()));

    return Pair.of(sslConnector, trustStore);
  }

  private static String getConfig(DremioConfig config, String propertyPath) {
    if (config.hasPath(propertyPath) && !isNullOrEmpty(config.getString(propertyPath))) {
      return config.getString(propertyPath);
    }

    return null;
  }
}
