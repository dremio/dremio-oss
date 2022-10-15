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
package com.dremio.exec.rpc.ssl;

import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
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
import java.util.Optional;
import java.util.Set;

import javax.inject.Provider;

import org.apache.commons.lang3.RandomStringUtils;
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
import org.joda.time.DateTime;

import com.dremio.config.DremioConfig;
import com.dremio.security.SecurityFolder;
import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.ssl.SSLConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;
import com.google.common.net.InetAddresses;

/**
 * Helper class to create {@link SSLConfig} instances for the specified communication path, based on config.
 */
public class SSLConfigurator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLConfigurator.class);

  @VisibleForTesting
  public static final String UNSECURE_PASSWORD = "averylongandunsecurepasswordfordremiokeystore";
  private static final char[] UNSECURE_PASSWORD_CHAR_ARRAY = UNSECURE_PASSWORD.toCharArray();

  public static final String KEY_STORE_FILE = "keystore";

  public static final String TRUST_STORE_FILE = "certs";
  public static final Set<PosixFilePermission> TRUST_STORE_FILE_PERMISSIONS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, GROUP_READ, OTHERS_READ);

  private final DremioConfig config;
  private final Provider<CredentialsService> credentialsServiceProvider;
  private final String prefix;
  private final String communicationPath;

  /**
   * Constructor.
   *
   * @param config dremio config
   * @param prefix prefix used in config block
   * @param communicationPath communication path name, used for logging
   */
  public SSLConfigurator(
    DremioConfig config,
    Provider<CredentialsService> credentialsServiceProvider,
    String prefix,
    String communicationPath
  ) {
    this.config = config;
    this.credentialsServiceProvider = credentialsServiceProvider;
    this.prefix = prefix;
    this.communicationPath = communicationPath;
  }

  /**
   * Get the {@link SSLConfig}.
   *
   * {@param hostName} and {@param alternativeHostNames} are required for auto-generating certificates.
   * {@param disablePeerVerification} is ignored if auto-generating certificates, since the peer would have to as well.
   *
   *
   * @param disablePeerVerification disable peer verification
   * @param hostName host name of this service
   * @param alternativeHostNames alternative host names of this service
   * @return ssl config
   */
  public Optional<SSLConfig> getSSLConfig(
      boolean disablePeerVerification,
      String hostName,
      String... alternativeHostNames
  ) throws GeneralSecurityException, IOException {
    final boolean enabled = getBooleanConfig(DremioConfig.SSL_ENABLED);
    if (!enabled) {
      return Optional.empty();
    }

    final SSLConfig.Builder builder = SSLConfig.newBuilderForServer();
    builder.setDisablePeerVerification(disablePeerVerification);

    final boolean autoGenerated = getBooleanConfig(DremioConfig.SSL_AUTO_GENERATED_CERTIFICATE);
    if (autoGenerated) {
      logger.warn("*** Using generated self-signed SSL settings for server ('{}' component) ***\n"
              + "Using auto-generated certificates is not secure. Please consider switching to your own certificates.",
          communicationPath);

      final SecurityFolder securityFolder = SecurityFolder.of(config);
      final String localWritePathString = config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING);
      final boolean configured = configureUsingPreviouslyGeneratedStores(builder, securityFolder, localWritePathString);
      if (!configured) {
        logger.info("No previous keystore detected, creating certificate. This operation might take time...");
        generateCertificatesAndConfigure(builder,securityFolder, localWritePathString, hostName, alternativeHostNames);
      }

      builder.setDisablePeerVerification(true); // explicitly set

      return Optional.of(builder.build());
    }

    // else, configure using the provided keystore/ truststore
    configureUsingConfFile(builder);
    return Optional.of(builder.build());
  }

  private Optional<String> getStringConfig(String base) {
    assert config.hasPath(prefix + base);

    final String value = config.getString(prefix + base);
    return Strings.isNullOrEmpty(value) ? Optional.empty() : Optional.of(value);
  }

  private boolean getBooleanConfig(String base) {
    assert config.hasPath(prefix + base);

    return config.getBoolean(prefix + base);
  }

  private boolean configureUsingPreviouslyGeneratedStores(SSLConfig.Builder builder, SecurityFolder securityFolder, String localWritePathString)
      throws GeneralSecurityException, IOException {
    if (securityFolder.exists(KEY_STORE_FILE)) {
      // if file exists, stores were generated previously
      logger.debug("Using previously generated keystore/truststore");

      // Previously key store and trust store were in the KEY_STORE_DIRECTORY. This function moves
      // the trust store from KEY_STORE_DIRECTORY to local data directory, if necessary, and
      // sets appropriate permissions on the file.
      moveTrustStoreIfNecessary(securityFolder, localWritePathString);

      // must be this path at this point
      final Path trustStorePath = Paths.get(localWritePathString, TRUST_STORE_FILE);
      Preconditions.checkState(Files.exists(trustStorePath), "auto-generated trust store is missing");

      builder.setKeyStorePath(securityFolder.resolve(KEY_STORE_FILE).toString())
          .setKeyStorePassword(UNSECURE_PASSWORD)
          .setTrustStorePath(trustStorePath.toString())
          .setTrustStorePassword(UNSECURE_PASSWORD);

      return true;
    }

    return false;
  }

  private void moveTrustStoreIfNecessary(SecurityFolder securityFolder, String localWritePathString) {
    final Path toPath = Paths.get(localWritePathString, TRUST_STORE_FILE);
    if (Files.exists(toPath)) {
      return; // already moved
    }

    final Path fromPath = securityFolder.resolve(TRUST_STORE_FILE);
    Preconditions.checkState(Files.exists(fromPath));
    logger.info("Moving trust store from '{}' to '{}'", fromPath, toPath);
    try {
      Files.move(fromPath, toPath);
    } catch (IOException e) {
      final String message =
          String.format("Failed to move trust store from '%s' to '%s'. " +
              "Please do so manually. Also, set permissions to 644 on trust store.", fromPath, toPath);

      logger.error(message, e);
      throw new RuntimeException(message, e);
    }

    try {
      Files.setPosixFilePermissions(toPath, TRUST_STORE_FILE_PERMISSIONS);
    } catch (IOException e) {
      final String message =
          String.format("Failed to set 644 permissions on trust store at '%s'. Please do so manually.", toPath);

      logger.error(message, e);
      throw new RuntimeException(message, e);
    }
  }

  private void generateCertificatesAndConfigure(
      SSLConfig.Builder builder,
      SecurityFolder securityFolder,
      String localWritePathString,
      String hostName,
      String... alternativeHostNames
  ) throws GeneralSecurityException, IOException {
    // initialize a new keystore, along with a trust store for clients
    final String storeType = KeyStore.getDefaultType();
    final KeyStore keyStore = KeyStore.getInstance(storeType);
    keyStore.load(null, null);

    final KeyStore trustStore = KeyStore.getInstance(storeType);
    trustStore.load(null, null);

    final SecureRandom random = new SecureRandom();

    // generate a private-public 2048-bit key pair
    final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048, random);
    final KeyPair keyPair = keyPairGenerator.generateKeyPair();

    final DateTime now = DateTime.now();

    // create builder for certificate attributes
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

    // create a certificate valid for 1 years from now
    // add the main hostname + the alternative hostnames to the SAN extension
    final GeneralName[] alternativeSubjectNames = new GeneralName[alternativeHostNames.length + 1];
    alternativeSubjectNames[0] = newGeneralName(hostName);
    for (int i = 0; i < alternativeHostNames.length; i++) {
      alternativeSubjectNames[i + 1] = newGeneralName(alternativeHostNames[i]);
    }

    final X509v3CertificateBuilder certificateBuilder =
        new JcaX509v3CertificateBuilder(
            nameBuilder.build(),
            serialNumber,
            notBefore,
            notAfter,
            nameBuilder.build(),
            keyPair.getPublic()
        ).addExtension(
            Extension.subjectAlternativeName,
            false,
            new DERSequence(alternativeSubjectNames));

    // sign the certificate using the private key
    final ContentSigner contentSigner;
    try {
      contentSigner = new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());
    } catch (OperatorCreationException e) {
      throw new GeneralSecurityException(e);
    }
    final X509Certificate certificate =
        new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner));

    // check the validity
    certificate.checkValidity(now.toDate());

    // make sure the certificate is self-signed
    certificate.verify(certificate.getPublicKey());

    final String fingerprint = BaseEncoding.base16()
        .withSeparator(":", 2)
        .encode(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
    logger.info("Certificate created (SHA-256 fingerprint: {})", fingerprint);

    // Generate a random password for keystore protection
    keyStore.setKeyEntry(
        "DremioAutoGeneratedPrivateKey",
        keyPair.getPrivate(),
        UNSECURE_PASSWORD_CHAR_ARRAY,
        new java.security.cert.Certificate[]{certificate}
    );

    // add random number to cert name to avoid collisions between certs in multinode cluster tests
    trustStore.setEntry("DremioAutoGeneratedCert" + RandomStringUtils.randomNumeric(5),
        new TrustedCertificateEntry(certificate), null);

    // save stores and add details to builder
    try (OutputStream stream = securityFolder.newSecureOutputStream(KEY_STORE_FILE, SecurityFolder.OpenOption.CREATE_ONLY)) {
      keyStore.store(stream, UNSECURE_PASSWORD_CHAR_ARRAY);
    }

    final Path trustStorePath = Paths.get(localWritePathString, TRUST_STORE_FILE);
    try (OutputStream stream = Files.newOutputStream(trustStorePath, StandardOpenOption.CREATE_NEW)) {
      trustStore.store(stream, UNSECURE_PASSWORD_CHAR_ARRAY);
    }
    Files.setPosixFilePermissions(trustStorePath, TRUST_STORE_FILE_PERMISSIONS);

    builder.setKeyStoreType(storeType)
        .setKeyStorePath(securityFolder.resolve(KEY_STORE_FILE).toString())
        .setKeyStorePassword(UNSECURE_PASSWORD)
        .setTrustStoreType(storeType)
        .setTrustStorePath(trustStorePath.toString())
        .setTrustStorePassword(UNSECURE_PASSWORD);
  }

  private static GeneralName newGeneralName(String name) {
    int nameType = InetAddresses.isInetAddress(name) ? GeneralName.iPAddress : GeneralName.dNSName;

    return new GeneralName(nameType, name);
  }

  /**
   * Look up a password by {@param passwordPattern} using {@param credentialsServiceProvider}.
   * @return a password looked up by {@link CredentialsService} or {@param passwordPattern}
   * itself if {@param passwordPattern} is not a URI.
   * @throws RuntimeException if {@param passwordPattern} has a valid format but
   * {@link CredentialsService} cannot resolve the password.
   */
  private String lookupPassword(
    String passwordPattern,
    Provider<CredentialsService> credentialsServiceProvider
  ) {
    try {
      return credentialsServiceProvider.get().lookup(passwordPattern);
    } catch (IllegalArgumentException e) {
      logger.warn("The string used to locate secret is not a valid URI.");
      return passwordPattern;
    } catch (CredentialsException e) {
      throw new RuntimeException(e);
    }
  }

  private void configureUsingConfFile(SSLConfig.Builder builder) {
    final Optional<String> keyStorePath = getStringConfig(DremioConfig.SSL_KEY_STORE_PATH);
    if (!keyStorePath.isPresent()) {
      throw new IllegalArgumentException(String.format(
          "No keystore configured, and certificate auto-generation is disabled. But SSL is enabled for '%s' path",
          communicationPath));
    }

    logger.info("Using configured keystore for '{}' component at '{}'", communicationPath, keyStorePath);
    keyStorePath.ifPresent(builder::setKeyStorePath);
    getStringConfig(DremioConfig.SSL_KEY_STORE_PASSWORD)
        .ifPresent(jksPwdUri -> builder.setKeyStorePassword(lookupPassword(jksPwdUri, credentialsServiceProvider)));
    getStringConfig(DremioConfig.SSL_KEY_PASSWORD)
        .ifPresent(keyPwdUri -> builder.setKeyPassword(lookupPassword(keyPwdUri, credentialsServiceProvider)));
    getStringConfig(DremioConfig.SSL_KEY_STORE_TYPE)
        .ifPresent(builder::setKeyStoreType);

    getStringConfig(DremioConfig.SSL_TRUST_STORE_TYPE)
        .ifPresent(builder::setTrustStoreType);
    getStringConfig(DremioConfig.SSL_TRUST_STORE_PATH)
        .ifPresent(builder::setTrustStorePath);
    getStringConfig(DremioConfig.SSL_TRUST_STORE_PASSWORD)
        .ifPresent(tsPwdUri -> builder.setTrustStorePassword(lookupPassword(tsPwdUri, credentialsServiceProvider)));
  }

  /**
   * Loads and returns the trust store as defined in the configuration. If not explicitly configured or
   * auto-generated, this returns {@code null}.
   *
   * @return trust store
   */
  public Optional<KeyStore> getTrustStore() throws GeneralSecurityException, IOException {
    final Optional<String> configuredPath = getStringConfig(DremioConfig.SSL_TRUST_STORE_PATH);

    final String trustStorePath;
    final char[] trustStorePassword;
    if (configuredPath.isPresent()) {
      logger.info("Loading configured trust store at {}", configuredPath.get());
      trustStorePath = configuredPath.get();

      trustStorePassword = getStringConfig(DremioConfig.SSL_TRUST_STORE_PASSWORD)
          .map(pwdUri -> lookupPassword(pwdUri, credentialsServiceProvider).toCharArray())
          .orElse(new char[]{});
    } else {
      // Check if auto-generated certificates are used
      final Path path = Paths.get(config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING), TRUST_STORE_FILE);
      if (Files.notExists(path)) {
        return Optional.empty();
      }

      trustStorePath = path.toString();
      trustStorePassword = UNSECURE_PASSWORD_CHAR_ARRAY;
    }

    // TODO(DX-12920): the type could be different
    final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    try (InputStream stream = Files.newInputStream(Paths.get(trustStorePath))) {
      trustStore.load(stream, trustStorePassword);
    }
    return Optional.of(trustStore);
  }
}
