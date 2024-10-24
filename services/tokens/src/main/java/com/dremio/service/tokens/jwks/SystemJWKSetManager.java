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
package com.dremio.service.tokens.jwks;

import com.dremio.common.server.WebServerInfoProvider;
import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.security.SecurityFolder;
import com.dremio.security.SecurityFolder.OpenOption;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.tokens.TokenManagerImplV2;
import com.dremio.service.tokens.jwt.JWTSigner;
import com.dremio.service.tokens.jwt.JWTValidator;
import com.dremio.service.tokens.jwt.JWTValidatorImpl;
import com.dremio.service.tokens.jwt.NoOpJWTSigner;
import com.dremio.service.tokens.jwt.NoOpJWTValidator;
import com.dremio.service.tokens.jwt.SystemJWTSigner;
import com.dremio.service.users.UserResolver;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceUtils;
import com.dremio.services.credentials.SecretsCreator;
import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.AsymmetricJWK;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.proc.SingleKeyJWSKeySelector;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import io.protostuff.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStore.Entry;
import java.security.KeyStore.Entry.Attribute;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PKCS12Attribute;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Provider;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.immutables.value.Value.Immutable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the lifecycle of Dremio's generated and self-signed JWKS, which are stored in
 * a <code>KeyStore</code> on the local filesystem. In order to centralize management of the keys
 * (namely the private signing key), only the master coordinator is intended to use this class to
 * manage Dremio's JWKS.
 */
@Options
public class SystemJWKSetManager implements JWKSetManager {
  public static final int KEY_ROTATION_PERIOD_DAYS = 15;

  // Use 99999 as a fake PEN (Private Enterprise Number) for now that's far enough out of range of
  // existing ASN.1 OIDs under the standard 1.3.6.1.4.1 prefix that we shouldn't run into conflicts.
  // See https://www.iana.org/assignments/enterprise-numbers for more info.
  private static final String DREMIO_ASN_1_PREFIX = "1.3.6.1.4.1.99999.";
  private static final String KEY_ENTRY_SEQUENCE_ATTRIBUTE = DREMIO_ASN_1_PREFIX + "1";
  private static final String KEY_ENTRY_CREATION_DATE_ATTRIBUTE = DREMIO_ASN_1_PREFIX + "2";

  private static final Logger logger = LoggerFactory.getLogger(SystemJWKSetManager.class);
  private static final String KEYSTORE_FILENAME = "token-manager-keystore.p12";
  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final int JWKS_CONSISTENCY_JOB_PERIOD_HOURS = 1;
  private static final String KEY_ENTRY_CONFIG_STORE_BASE_KEY = "system.jwks.manager.key.";

  private final SecureRandom secureRandom = new SecureRandom();

  private final Clock clock;
  private final Provider<WebServerInfoProvider> webServerInfoProvider;
  private final Provider<UserResolver> userResolverProvider;
  private final Provider<OptionManager> optionManagerProvider;
  private final Provider<SchedulerService> schedulerServiceProvider;
  private final Provider<ConfigurationStore> configStoreProvider;
  private final Provider<SecretsCreator> secretsCreatorProvider;
  private final Provider<CredentialsService> credentialsServiceProvider;
  private final DremioConfig dremioConfig;

  private Cancellable jwksConsistencyTask;
  private AtomicLong currentKeySequence;
  private ECKey currentJwk;
  private JWKSet currentJwks;
  private JWTSigner jwtSigner;
  private JWTValidator jwtValidator;

  public SystemJWKSetManager(
      Clock clock,
      Provider<WebServerInfoProvider> webServerInfoProvider,
      Provider<UserResolver> userResolverProvider,
      Provider<OptionManager> optionManagerProvider,
      Provider<SchedulerService> schedulerServiceProvider,
      Provider<ConfigurationStore> configStoreProvider,
      Provider<SecretsCreator> secretsCreatorProvider,
      Provider<CredentialsService> credentialsServiceProvider,
      DremioConfig dremioConfig) {
    this.clock = clock;
    this.webServerInfoProvider = webServerInfoProvider;
    this.userResolverProvider = userResolverProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.schedulerServiceProvider = schedulerServiceProvider;
    this.configStoreProvider = configStoreProvider;
    this.secretsCreatorProvider = secretsCreatorProvider;
    this.credentialsServiceProvider = credentialsServiceProvider;
    this.dremioConfig = dremioConfig;
  }

  @Override
  public void start() throws Exception {
    if (!optionManagerProvider.get().getOption(TokenManagerImplV2.ENABLE_JWT_ACCESS_TOKENS)) {
      // Don't initialize JWKS keystore or schedule rotation task if JWTs aren't enabled. Note that
      // this means a system restart will be required once JWTs are enabled to initialize the
      // keystore. This will only be the case for the private preview and will be improved for the
      // public release.
      this.jwtValidator = new NoOpJWTValidator();
      this.jwtSigner = new NoOpJWTSigner();

      return;
    }

    enforceConsistentJwksState();

    this.jwksConsistencyTask =
        schedulerServiceProvider
            .get()
            .schedule(
                Schedule.Builder.everyHours(JWKS_CONSISTENCY_JOB_PERIOD_HOURS)
                    .startingAt(
                        clock.instant().plus(Duration.ofHours(JWKS_CONSISTENCY_JOB_PERIOD_HOURS)))
                    .build(),
                () -> {
                  long start = clock.millis();
                  try {
                    enforceConsistentJwksState();
                    logger.debug(
                        "JWK set manager finished enforcing consistency of the current JWK set after {}ms",
                        clock.millis() - start);
                  } catch (Exception e) {
                    logger.error(
                        "JWK set manager failed to enforce the consistency of the current JWK set after {}ms",
                        clock.millis() - start,
                        e);
                  }
                });
  }

  @Override
  public void close() throws Exception {
    if (jwksConsistencyTask != null) {
      final long start = clock.millis();
      // Don't interrupt if the task is running to avoid leaving the keystore in an inconsistent
      // state, and because the task should finish quickly.
      jwksConsistencyTask.cancel(false);
      logger.info(
          "JWK set manager successfully cancelled scheduled JWKS consistency task after {}ms",
          clock.millis() - start);
    }
  }

  private RotatingPersistedKey getRotatingKey() {
    boolean keystoreExists = SecurityFolder.exists(dremioConfig, KEYSTORE_FILENAME);

    try {
      SecurityFolder securityFolder = SecurityFolder.of(dremioConfig);
      final char[] keystorePassword = getKeystorePassword();
      if (keystoreExists) {
        return loadKeysFromKeystore(securityFolder, keystorePassword);
      }

      // Bootstrap a keystore
      final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      keyStore.load(null, getKeystorePassword());

      this.currentKeySequence = new AtomicLong(1);
      final ECKey jwk = generateNewKey();
      final X509Certificate certificate = generateCertificate(jwk);
      final long keySequence = this.currentKeySequence.getAndIncrement();
      final Date keyCreationDate = new Date(clock.millis());
      final PrivateKeyEntry privateKeyEntry =
          createPrivateKeyEntry(jwk.toPrivateKey(), certificate, keySequence, keyCreationDate);
      keyStore.setEntry(
          jwk.getKeyID(),
          privateKeyEntry,
          new PasswordProtection(createPrivateKeyPassword(jwk.getKeyID())));

      logger.info(
          "Creating initial JWK with key alias '{}' and adding to JWKS keystore", jwk.getKeyID());

      try (OutputStream outputStream =
          securityFolder.newSecureOutputStream(
              KEYSTORE_FILENAME, SecurityFolder.OpenOption.CREATE_ONLY)) {
        keyStore.store(outputStream, getKeystorePassword());
        logger.info("Wrote initial JWKS keystore to {}", securityFolder.resolve(KEYSTORE_FILENAME));
      }

      return new ImmutableRotatingPersistedKey.Builder()
          .setCurrentKey(
              new ImmutablePersistedKey.Builder()
                  .setKeySequence(keySequence)
                  .setAlias(jwk.getKeyID())
                  .setJwk(jwk)
                  .setDateCreated(keyCreationDate)
                  .build())
          .build();
    } catch (JOSEException e) {
      throw new RuntimeException("Failed to convert JWK to private key", e);
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException("Failed to load private key", e);
    }
  }

  private char[] getKeystorePassword() {
    try {
      return CredentialsServiceUtils.getKeystorePassword(
          dremioConfig, credentialsServiceProvider.get());
    } catch (CredentialsException e) {
      throw new RuntimeException("Failed to retrieve JWKS keystore password", e);
    }
  }

  private RotatingPersistedKey loadKeysFromKeystore(
      final SecurityFolder securityFolder, final char[] keystorePassword)
      throws GeneralSecurityException, IOException {
    final KeyStore keyStore = loadKeyStore(securityFolder, keystorePassword);

    Iterable<String> keystoreAliases =
        () -> {
          try {
            return keyStore.aliases().asIterator();
          } catch (KeyStoreException e) {
            throw new RuntimeException("Failed to load keystore aliases", e);
          }
        };
    // Get stored keys ordered from newest to oldest
    List<PersistedKey> storedKeys =
        StreamSupport.stream(keystoreAliases.spliterator(), false)
            .map(
                alias -> {
                  final KeyEntryAttributes keyAttributes = getKeyEntryAttributes(alias, keyStore);

                  if (currentKeySequence == null
                      || currentKeySequence.get() <= keyAttributes.getKeySequence()) {
                    this.currentKeySequence = new AtomicLong(keyAttributes.getKeySequence() + 1);
                  }

                  final JWK key;
                  try {
                    // The keystore alias is the key ID
                    key = ECKey.load(keyStore, alias, getPrivateKeyPassword(alias));
                  } catch (KeyStoreException | JOSEException e) {
                    throw new RuntimeException(
                        String.format("Failed to load key with alias '%s' from keystore", alias),
                        e);
                  }

                  return new ImmutablePersistedKey.Builder()
                      .setKeySequence(keyAttributes.getKeySequence())
                      .setAlias(alias)
                      .setJwk(key)
                      .setDateCreated(keyAttributes.getDateCreated())
                      .build();
                })
            .sorted(
                (key, otherKey) -> {
                  final long keySequence = key.getKeySequence();
                  final long otherKeySequence = otherKey.getKeySequence();

                  if (keySequence > otherKeySequence) {
                    return -1;
                  } else if (keySequence < otherKeySequence) {
                    return 1;
                  } else {
                    throw new RuntimeException(
                        String.format(
                            "Found two different keys with the same ID attribute value. Key aliases: '%s' and '%s'",
                            key.getAlias(), otherKey.getAlias()));
                  }
                })
            .collect(Collectors.toList());

    if (storedKeys.isEmpty()) {
      // TODO: add keys and write keystore in this case
      throw new RuntimeException("Found no keys in JWKS keystore");
    } else if (storedKeys.size() > 2) {
      throw new RuntimeException(
          "Unexpected number of keys in JWKS keystore: " + storedKeys.size());
    }

    final ImmutableRotatingPersistedKey.Builder keyBuilder =
        new ImmutableRotatingPersistedKey.Builder().setCurrentKey(storedKeys.get(0));

    if (storedKeys.size() == 2) {
      keyBuilder.setOldKey(Optional.of(storedKeys.get(1)));
    } else {
      keyBuilder.setOldKey(Optional.empty());
    }

    return keyBuilder.build();
  }

  private KeyStore loadKeyStore(SecurityFolder securityFolder, final char[] keystorePassword)
      throws GeneralSecurityException, IOException {
    final KeyStore keystore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (final InputStream inputStream = securityFolder.newSecureInputStream(KEYSTORE_FILENAME)) {
      keystore.load(inputStream, keystorePassword);
    }
    return keystore;
  }

  private KeyEntryAttributes getKeyEntryAttributes(String keyAlias, KeyStore keyStore) {
    final Entry entry;
    try {
      entry = keyStore.getEntry(keyAlias, new PasswordProtection(getPrivateKeyPassword(keyAlias)));
    } catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException e) {
      throw new RuntimeException(String.format("Failed to load key with alias '%s'", keyAlias), e);
    }

    final Map<String, String> customAttributes =
        entry.getAttributes().stream()
            .filter(
                attribute ->
                    KEY_ENTRY_SEQUENCE_ATTRIBUTE.equals(attribute.getName())
                        || KEY_ENTRY_CREATION_DATE_ATTRIBUTE.equals(attribute.getName()))
            .collect(Collectors.toMap(Attribute::getName, Attribute::getValue));
    if (!customAttributes.containsKey(KEY_ENTRY_SEQUENCE_ATTRIBUTE)
        || !customAttributes.containsKey(KEY_ENTRY_CREATION_DATE_ATTRIBUTE)) {
      throw new RuntimeException("Failed to find key entry sequence and creation date attributes");
    }

    final ImmutableKeyEntryAttributes.Builder attributesBuilder =
        new ImmutableKeyEntryAttributes.Builder();

    try {
      attributesBuilder.setKeySequence(
          Long.parseLong(customAttributes.get(KEY_ENTRY_SEQUENCE_ATTRIBUTE)));
    } catch (NumberFormatException e) {
      throw new RuntimeException(
          String.format(
              "Key with alias '%s' has an ID attribute that is not a valid integer. Found: '%s'",
              keyAlias, customAttributes.get(KEY_ENTRY_SEQUENCE_ATTRIBUTE)));
    }

    try {
      attributesBuilder.setDateCreated(
          new Date(Long.parseLong(customAttributes.get(KEY_ENTRY_CREATION_DATE_ATTRIBUTE))));
    } catch (NumberFormatException e) {
      throw new RuntimeException(
          String.format(
              "Key with alias '%s' has a creation date attribute that is not a valid timestamp. Found: '%s'",
              keyAlias, customAttributes.get(KEY_ENTRY_CREATION_DATE_ATTRIBUTE)));
    }

    return attributesBuilder.build();
  }

  private ECKey generateNewKey() throws GeneralSecurityException, IOException, JOSEException {
    try {
      return new ECKeyGenerator(Curve.P_256)
          .keyUse(KeyUse.SIGNATURE)
          .keyID(UUID.randomUUID().toString())
          .algorithm(JWSAlgorithm.ES256)
          .generate();
    } catch (JOSEException e) {
      throw new RuntimeException("Failed to generate private key", e);
    }
  }

  private X509Certificate generateCertificate(final AsymmetricJWK key)
      throws JOSEException, GeneralSecurityException {
    // TODO: move cert generate logic between here and
    //  SSLConfigurator.generateCertificatesAndConfigure to a common location
    final DateTime now = DateTime.now();

    final X500NameBuilder nameBuilder =
        new X500NameBuilder(BCStyle.INSTANCE)
            .addRDN(BCStyle.OU, "Dremio Corp. (auto-generated)")
            .addRDN(BCStyle.O, "Dremio Corp. (auto-generated)")
            .addRDN(BCStyle.L, "Mountain View")
            .addRDN(BCStyle.ST, "California")
            .addRDN(BCStyle.C, "US");
    final Date notBefore = now.minusDays(1).toDate();

    // TODO: determine what the expiration should be based on configured key rotation period.
    final Date notAfter = now.plusYears(1).toDate();
    final BigInteger serialNumber = new BigInteger(128, secureRandom);

    final X509v3CertificateBuilder certificateBuilder =
        new JcaX509v3CertificateBuilder(
            nameBuilder.build(),
            serialNumber,
            notBefore,
            notAfter,
            nameBuilder.build(),
            key.toPublicKey());

    // sign the certificate using the private key
    final ContentSigner contentSigner;
    try {
      contentSigner = new JcaContentSignerBuilder("SHA256WITHECDSA").build(key.toPrivateKey());
    } catch (OperatorCreationException e) {
      throw new GeneralSecurityException(e);
    }
    final X509Certificate certificate =
        new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner));

    // check the validity
    certificate.checkValidity(now.toDate());

    // make sure the certificate is self-signed
    certificate.verify(certificate.getPublicKey());

    return certificate;
  }

  private char[] createPrivateKeyPassword(String keyAlias) {
    final String password = new BigInteger(130, secureRandom).toString(32);
    return createPrivateKeyPassword(keyAlias, password);
  }

  @VisibleForTesting
  char[] createPrivateKeyPassword(String keyAlias, String keyPassword) {
    final String configStoreKey = getConfigStoreEntryKey(keyAlias);
    final ConfigurationEntry existingEntry = configStoreProvider.get().get(configStoreKey);
    if (existingEntry != null) {
      throw new RuntimeException(
          String.format(
              "Attempt to generate private key password failed: password reference already exists for key with alias '%s'",
              keyAlias));
    }

    final Optional<URI> optionalSecretUri;
    try {
      optionalSecretUri = secretsCreatorProvider.get().encrypt(keyPassword);
    } catch (CredentialsException e) {
      throw new RuntimeException("Failed to generate JWK keystore entry password", e);
    }

    if (optionalSecretUri.isEmpty()) {
      throw new RuntimeException(
          "Failed to generate JWK keystore entry password: encryption was refused");
    }

    final ConfigurationEntry keyPasswordEntry = new ConfigurationEntry();
    keyPasswordEntry.setValue(ByteString.copyFromUtf8(optionalSecretUri.get().toString()));
    configStoreProvider.get().put(configStoreKey, keyPasswordEntry);

    return keyPassword.toCharArray();
  }

  private String getConfigStoreEntryKey(String keyAlias) {
    return KEY_ENTRY_CONFIG_STORE_BASE_KEY + keyAlias;
  }

  @VisibleForTesting
  char[] getPrivateKeyPassword(String keyAlias) {
    final String configStoreKey = getConfigStoreEntryKey(keyAlias);

    final ConfigurationEntry keyPasswordUriConfigEntry =
        configStoreProvider.get().get(configStoreKey);
    if (keyPasswordUriConfigEntry == null || keyPasswordUriConfigEntry.getValue() == null) {
      throw new RuntimeException(
          "Failed to retrieve JWK keystore entry password reference from configuration store");
    } else {
      final String keyPasswordUri = keyPasswordUriConfigEntry.getValue().toStringUtf8();
      try {
        return credentialsServiceProvider.get().lookup(keyPasswordUri).toCharArray();
      } catch (CredentialsException e) {
        throw new RuntimeException("Failed to retrieve JWK keystore entry password", e);
      }
    }
  }

  private PrivateKeyEntry createPrivateKeyEntry(
      PrivateKey key, Certificate certificate, long keySequence, Date keyCreationDate) {
    final Set<Entry.Attribute> attributes =
        Set.of(
            new PKCS12Attribute(KEY_ENTRY_SEQUENCE_ATTRIBUTE, "" + keySequence),
            new PKCS12Attribute(KEY_ENTRY_CREATION_DATE_ATTRIBUTE, "" + keyCreationDate.getTime()));
    return new PrivateKeyEntry(key, new java.security.cert.Certificate[] {certificate}, attributes);
  }

  /**
   * Enforce the consistency of our JWKS keys in the persisted keystore as well as the cached keys
   * used for signing and validating JWTs. Calling this method:
   *
   * <p>1. Initializes the keystore with a new key pair if the keystore does not yet exist.
   *
   * <p>2. Handles key rotation and revocation of old keys based on the key rotation period and
   * configured JWT expiration such that a new key is generated every rotation period and the old
   * public key is maintained just long enough to allow us to verify unexpired JWTs that were signed
   * with the old private key.
   */
  // TODO: phase 2 item: add an admin command to re-create the keystore and generate a new key pair
  //  to give admins a break glass option if the keystore winds up in a bad state
  @VisibleForTesting
  void enforceConsistentJwksState() {
    final RotatingPersistedKey rotatingKey = getRotatingKey();

    if (rotatingKey.getOldKey().isEmpty()) {
      final PersistedKey currentKey = rotatingKey.getCurrentKey();
      this.currentJwk = (ECKey) currentKey.getJwk();
      this.currentJwks = new JWKSet(currentKey.getJwk());

      final Instant keyRotationDate =
          currentKey.getDateCreated().toInstant().plus(Duration.ofDays(KEY_ROTATION_PERIOD_DAYS));
      final Instant now = clock.instant();

      if (now.isAfter(keyRotationDate)) {
        rotatePrivateKey();
      }
    } else {
      final PersistedKey oldKey = rotatingKey.getOldKey().get();
      this.currentJwk = (ECKey) rotatingKey.getCurrentKey().getJwk();
      this.currentJwks = new JWKSet(List.of(currentJwk, oldKey.getJwk()));

      final long jwtExpirationMinutes =
          optionManagerProvider.get().getOption(TokenManagerImplV2.TOKEN_EXPIRATION_TIME_MINUTES);
      final Instant oldKeyRevocationDate =
          oldKey
              .getDateCreated()
              .toInstant()
              .plus(Duration.ofDays(KEY_ROTATION_PERIOD_DAYS).plusMinutes(jwtExpirationMinutes));
      final Instant now = clock.instant();

      if (now.isAfter(oldKeyRevocationDate)) {
        revokeOldKey();
      }

      final PersistedKey currentKey = rotatingKey.getCurrentKey();
      final Instant currentKeyRevocationDate =
          currentKey
              .getDateCreated()
              .toInstant()
              .plus(Duration.ofDays(KEY_ROTATION_PERIOD_DAYS).plusMinutes(jwtExpirationMinutes));
      // Ensure we rotate the current key if we've missed a rotation period either due to previous
      // rotations failing or because Dremio was offline for an extended period.
      if (now.isAfter(currentKeyRevocationDate)) {
        rotatePrivateKey();
      }
    }

    final ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
    jwtProcessor.setJWSTypeVerifier(new DefaultJOSEObjectTypeVerifier<>(JOSEObjectType.JWT));

    final ECKey currentKey = currentJwk;

    if (currentJwks.getKeys().size() == 1) {
      try {
        jwtProcessor.setJWSKeySelector(
            new SingleKeyJWSKeySelector<>(JWSAlgorithm.ES256, currentKey.toPublicKey()));
      } catch (JOSEException e) {
        throw new RuntimeException("Failed to get JWK public key", e);
      }
    } else {
      jwtProcessor.setJWSKeySelector(
          new JWSVerificationKeySelector<>(JWSAlgorithm.ES256, new ImmutableJWKSet<>(currentJwks)));
    }

    final WebServerInfoProvider serverInfo = webServerInfoProvider.get();
    final JWTClaimsSet exactMatchClaims =
        new JWTClaimsSet.Builder()
            .issuer(serverInfo.getBaseURL().toString())
            .audience(serverInfo.getClusterId())
            .build();
    final Set<String> requiredClaims = new HashSet<>(JWTClaimsSet.getRegisteredNames());
    jwtProcessor.setJWTClaimsSetVerifier(
        new DefaultJWTClaimsVerifier<>(exactMatchClaims, requiredClaims));

    this.jwtValidator = new JWTValidatorImpl(userResolverProvider, jwtProcessor);

    try {
      this.jwtSigner =
          new SystemJWTSigner(new ECDSASigner(currentKey.toECPrivateKey()), currentKey.getKeyID());
    } catch (JOSEException e) {
      throw new RuntimeException("Failed to create JWT signer", e);
    }
  }

  private void rotatePrivateKey() {
    if (!SecurityFolder.exists(dremioConfig, KEYSTORE_FILENAME)) {
      throw new RuntimeException("Can't rotate key because the keystore doesn't exist yet");
    }

    final SecurityFolder securityFolder;
    try {
      securityFolder = SecurityFolder.of(dremioConfig);
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException("Failed to load Dremio security folder", e);
    }

    final KeyStore keyStore;
    try {
      keyStore = loadKeyStore(securityFolder, getKeystorePassword());
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException("Failed to load keystore", e);
    }

    try {
      final ECKey oldKey = currentJwk;

      // Generate new key pair and persist it in the keystore before we begin signing new JWTs with
      // the new private key to prevent error scenarios from preventing us from persisting the new
      // key pair after we've already signed JWTs with the new private key.
      final ECKey newKey = generateNewKey();
      final X509Certificate certificate = generateCertificate(newKey);
      final PrivateKeyEntry privateKeyEntry =
          createPrivateKeyEntry(
              newKey.toPrivateKey(),
              certificate,
              currentKeySequence.getAndIncrement(),
              new Date(clock.millis()));
      keyStore.setEntry(
          newKey.getKeyID(),
          privateKeyEntry,
          new PasswordProtection(createPrivateKeyPassword(newKey.getKeyID())));

      logger.info("Rotating in new JWK with alias {}", newKey.getKeyID());

      try (OutputStream outputStream =
          securityFolder.newSecureOutputStream(KEYSTORE_FILENAME, OpenOption.NO_CREATE)) {
        keyStore.store(outputStream, getKeystorePassword());
        logger.info("Wrote updated JWKS keystore to {}", securityFolder.resolve(KEYSTORE_FILENAME));
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException("Failed to write new private key to keystore", e);
      }

      this.currentJwks = new JWKSet(List.of(newKey, oldKey));
      this.currentJwk = newKey;
    } catch (GeneralSecurityException | IOException | JOSEException e) {
      throw new RuntimeException("Failed to generate new key", e);
    }
  }

  private void revokeOldKey() {
    if (currentJwks.getKeys().size() == 1) {
      // Nothing to do since only the old key exists. We should never end up here, but log a warning
      // if we do.
      logger.warn(
          "JWK set manager attempted to revoke old key, but only found one key in the key set");
      return;
    }

    if (!SecurityFolder.exists(dremioConfig, KEYSTORE_FILENAME)) {
      throw new RuntimeException("Keystore doesn't exist");
    }

    final List<JWK> keys =
        currentJwks.getKeys().stream()
            .filter(jwk -> !jwk.getKeyID().equals(currentJwk.getKeyID()))
            .collect(Collectors.toList());

    if (keys.size() != 1) {
      throw new RuntimeException("Expected to find one old key, but found " + keys.size());
    }

    final SecurityFolder securityFolder;
    try {
      securityFolder = SecurityFolder.of(dremioConfig);
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException("Failed to load Dremio security folder", e);
    }

    final KeyStore keyStore;
    try {
      keyStore = loadKeyStore(securityFolder, getKeystorePassword());
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException("Failed to load keystore", e);
    }

    final JWK oldKey = keys.get(0);
    try {
      keyStore.deleteEntry(oldKey.getKeyID());
      logger.info("Revoked old JWK with alias {}", oldKey.getKeyID());
    } catch (KeyStoreException e) {
      throw new RuntimeException("Failed to delete old key", e);
    }

    try (OutputStream outputStream =
        securityFolder.newSecureOutputStream(KEYSTORE_FILENAME, OpenOption.NO_CREATE)) {
      keyStore.store(outputStream, getKeystorePassword());
      logger.info("Wrote updated JWKS keystore to {}", securityFolder.resolve(KEYSTORE_FILENAME));
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException("Failed to write new private key to keystore", e);
    }

    // Delete the configuration store entry for the old key's password URI now that it's no longer
    // needed.
    final String configStoreKey = getConfigStoreEntryKey(oldKey.getKeyID());
    configStoreProvider.get().delete(configStoreKey);

    currentJwks = new JWKSet(currentJwk);
  }

  @Override
  public JWTSigner getSigner() {
    return jwtSigner;
  }

  @Override
  public JWTValidator getValidator() {
    return jwtValidator;
  }

  @Override
  public Map<String, Object> getPublicJWKS() {
    if (currentJwks == null) {
      throw new IllegalStateException("Cannot retrieve public JWKS: current JWKS is null");
    }

    return currentJwks.toPublicJWKSet().toJSONObject();
  }

  @Immutable
  interface PersistedKey {
    String getAlias();

    long getKeySequence();

    JWK getJwk();

    Date getDateCreated();
  }

  @Immutable
  interface RotatingPersistedKey {
    PersistedKey getCurrentKey();

    Optional<PersistedKey> getOldKey();
  }

  @Immutable
  interface KeyEntryAttributes {
    long getKeySequence();

    Date getDateCreated();
  }
}
