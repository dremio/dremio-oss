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

import static com.dremio.test.DremioTest.DEFAULT_DREMIO_CONFIG;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.server.WebServerInfoProvider;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.Kind;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.tokens.jwt.ImmutableJWTClaims;
import com.dremio.service.tokens.jwt.JWTClaims;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserResolver;
import com.dremio.service.users.proto.UID;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.credentials.Cipher;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceImpl;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.services.credentials.SecretsCreatorImpl;
import com.dremio.services.credentials.SystemCipher;
import com.dremio.services.credentials.SystemSecretCredentialsProvider;
import com.dremio.test.DremioTest;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.threeten.extra.MutableClock;

class TestSystemJWKSetManager {
  private static final String USERNAME = "testuser";
  private static final User TEST_USER =
      SimpleUser.newBuilder()
          .setUserName(USERNAME)
          .setUID(new UID(UUID.randomUUID().toString()))
          .build();
  private static final String CLUSTER_ID = "test-cluster-id";
  private static final String ISSUER_URL = "https://localhost:9047";
  private static final int JWT_EXPIRATION_MINUTES = 60;

  @TempDir private Path securityFolder;

  private LegacyKVStoreProvider kvStoreProvider;
  private OptionManager optionManager;
  private WebServerInfoProvider webServerInfoProvider;
  private UserResolver userResolver;
  private DremioConfig dremioConfig;
  private MutableClock clock;
  private SystemJWKSetManager jwkSetManager;
  private CredentialsService credentialsService;
  private SecretsCreator secretsCreator;

  @BeforeEach
  public void beforeEach() throws Exception {
    this.optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(mock(OptionValidatorListingImpl.class))
            .withOptionManager(mock(DefaultOptionManager.class))
            .withOptionManager(mock(SystemOptionManager.class))
            .build();
    when(optionManager.getOption("token.jwt-access-token.enabled"))
        .thenReturn(
            OptionValue.createOption(
                Kind.BOOLEAN,
                OptionValue.OptionType.SYSTEM,
                "token.jwt-access-token.enabled",
                "true"));
    when(optionManager.getOption("token.jwt-access-token.expiration.minutes"))
        .thenReturn(
            OptionValue.createOption(
                OptionValue.Kind.LONG,
                OptionValue.OptionType.SYSTEM,
                "token.jwt-access-token.expiration.minutes",
                "" + JWT_EXPIRATION_MINUTES));
    this.webServerInfoProvider =
        new WebServerInfoProvider() {
          @Override
          public String getClusterId() {
            return CLUSTER_ID;
          }

          @Override
          public URL getBaseURL() {
            try {
              return new URL(ISSUER_URL);
            } catch (MalformedURLException e) {
              throw new RuntimeException(e);
            }
          }
        };
    this.userResolver = mock(UserResolver.class);
    when(userResolver.getUser(TEST_USER.getUID())).thenReturn(TEST_USER);
    when(userResolver.getUser(USERNAME)).thenReturn(TEST_USER);

    this.dremioConfig =
        DEFAULT_DREMIO_CONFIG.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING, securityFolder.toString());

    this.clock = MutableClock.of(Instant.now(), ZoneOffset.UTC);

    this.credentialsService =
        CredentialsServiceImpl.newInstance(
            dremioConfig, Set.of(SystemSecretCredentialsProvider.class));
    final Cipher cipher = new SystemCipher(dremioConfig, credentialsService);
    this.secretsCreator = new SecretsCreatorImpl(() -> cipher, () -> credentialsService);
    this.kvStoreProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    kvStoreProvider.start();
    final ConfigurationStore configurationStore = new ConfigurationStore(kvStoreProvider);

    this.jwkSetManager =
        new SystemJWKSetManager(
            clock,
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> optionManager,
            () -> new LocalSchedulerService(1),
            () -> configurationStore,
            () -> secretsCreator,
            () -> credentialsService,
            dremioConfig);
    jwkSetManager.start();
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (jwkSetManager != null) {
      jwkSetManager.close();
    }
    if (kvStoreProvider != null) {
      kvStoreProvider.close();
    }
  }

  @Test
  public void enforceConsistentJwksState() throws ParseException {
    assertNotNull(jwkSetManager.getPublicJWKS());
    List<Map<String, Object>> keys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    final String originalKeyId = (String) keys.get(0).get("kid");

    // Need to use the actual current time for the issue time and not-before-time because the
    // library we use to verify JWTs uses the system time to verify these properties.
    final Date originalIssueTime = new Date();
    final Date originalExpirationTime =
        new Date(clock.instant().plus(Duration.ofMinutes(JWT_EXPIRATION_MINUTES)).toEpochMilli());
    final JWTClaims originalJwtClaims =
        new ImmutableJWTClaims.Builder()
            .setJWTId(UUID.randomUUID().toString())
            .setSubject(TEST_USER.getUID().getId())
            .setAudience(CLUSTER_ID)
            .setIssuer(ISSUER_URL)
            .setIssueTime(originalIssueTime)
            .setNotBeforeTime(originalIssueTime)
            .setExpirationTime(originalExpirationTime)
            .build();
    final String originalToken = jwkSetManager.getSigner().sign(originalJwtClaims);

    // We haven't passed any rotation period, so enforcing a consistent state should be a no-op.
    jwkSetManager.enforceConsistentJwksState();
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    assertEquals(originalKeyId, keys.get(0).get("kid"));
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(originalToken).username);

    // Advance time to immediately after the key rotation period and enforce the consistency of the
    // JWKS state to trigger a key rotation. Afterward, we'll sign JWTs with a new private key and
    // verify JWTs with both the old and new public key.
    clock.add(Duration.ofDays(SystemJWKSetManager.KEY_ROTATION_PERIOD_DAYS).plusSeconds(1));
    jwkSetManager.enforceConsistentJwksState();

    assertNotNull(jwkSetManager.getPublicJWKS());
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, keys.size());
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(originalToken).username);

    // Need to use the actual current time for the issue time and not-before-time because the
    // library we use to verify JWTs uses the system time to verify these properties.
    final Date newIssueTime = new Date();
    final Date newExpirationTime =
        new Date(clock.instant().plus(Duration.ofMinutes(JWT_EXPIRATION_MINUTES)).toEpochMilli());
    final JWTClaims newJwtClaims =
        new ImmutableJWTClaims.Builder()
            .setJWTId(UUID.randomUUID().toString())
            .setSubject(TEST_USER.getUID().getId())
            .setAudience(CLUSTER_ID)
            .setIssuer(ISSUER_URL)
            .setIssueTime(newIssueTime)
            .setNotBeforeTime(newIssueTime)
            .setExpirationTime(newExpirationTime)
            .build();
    final String newToken = jwkSetManager.getSigner().sign(newJwtClaims);
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(newToken).username);

    // We haven't passed a rotation period since last enforcing a consistent state, so enforce
    // consistent state should be a no-op.
    jwkSetManager.enforceConsistentJwksState();
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, keys.size());
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(originalToken).username);
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(newToken).username);

    // Advance time to immediately after the rotation period where we maintained the old and new
    // public key to validate JWTs that may have been signed with the old private key. After
    // enforcing a consistent state of the JWKS, we should revoke the old key.
    clock.add(Duration.ofMinutes(JWT_EXPIRATION_MINUTES));
    jwkSetManager.enforceConsistentJwksState();

    assertNotNull(jwkSetManager.getPublicJWKS());
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size(), "There should be one key in our JWKS after revoking the old key");
    assertNotEquals(originalKeyId, keys.get(0).get("kid"));
    assertThrows(
        IllegalArgumentException.class,
        () -> jwkSetManager.getValidator().validate(originalToken),
        "The original token should fail to validate because we've revoked its key pair and the JWT has expired");
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(newToken).username);

    // We haven't passed a rotation period since last enforcing a consistent state, so enforce
    // consistent state should be a no-op.
    jwkSetManager.enforceConsistentJwksState();
    assertNotNull(jwkSetManager.getPublicJWKS());
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size(), "There should be one key in our JWKS after revoking the old key");
    assertNotEquals(originalKeyId, keys.get(0).get("kid"));
    assertThrows(
        IllegalArgumentException.class,
        () -> jwkSetManager.getValidator().validate(originalToken),
        "The original token should fail to validate because we've revoked its key pair and the JWT has expired");
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(newToken).username);
  }

  @Test
  public void enforceConsistentJwksStateManagesKeyPasswordLifecycle() {
    assertNotNull(jwkSetManager.getPublicJWKS());
    List<Map<String, Object>> keys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    final String originalKeyId = (String) keys.get(0).get("kid");

    validateKeyPasswordConfigStoreEntryExists(originalKeyId);

    // Advance time to immediately after the key rotation period and enforce the consistency of the
    // JWKS state to trigger a key rotation.
    clock.add(Duration.ofDays(SystemJWKSetManager.KEY_ROTATION_PERIOD_DAYS).plusSeconds(1));
    jwkSetManager.enforceConsistentJwksState();

    assertNotNull(jwkSetManager.getPublicJWKS());
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, keys.size());
    final Optional<Map<String, Object>> optionalNewKey =
        keys.stream().filter(key -> !originalKeyId.equals(key.get("kid"))).findFirst();
    assertTrue(optionalNewKey.isPresent());
    final Map<String, Object> newKey = optionalNewKey.get();
    final String newKeyId = (String) newKey.get("kid");

    validateKeyPasswordConfigStoreEntryExists(
        originalKeyId,
        "The password for the original key should still exist since the key hasn't been revoked");
    validateKeyPasswordConfigStoreEntryExists(newKeyId);

    // Advance time to immediately after the revocation date of the original key and enforce
    // consistency to revoke the key.
    clock.add(Duration.ofMinutes(JWT_EXPIRATION_MINUTES));
    jwkSetManager.enforceConsistentJwksState();

    assertNotNull(jwkSetManager.getPublicJWKS());
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());

    validateKeyPasswordConfigStoreEntryIsDeleted(
        originalKeyId, "The old key's password should have been deleted when the key was revoked");
    validateKeyPasswordConfigStoreEntryExists(
        newKeyId,
        "The password for the new key should still exist since the key hasn't been revoked");
  }

  private void validateKeyPasswordConfigStoreEntryExists(String keyAlias) {
    validateKeyPasswordConfigStoreEntryExists(keyAlias, null);
  }

  private void validateKeyPasswordConfigStoreEntryExists(String keyAlias, String failureMessage) {
    final char[] retrievedPassword = jwkSetManager.getPrivateKeyPassword(keyAlias);
    assertNotNull(retrievedPassword, failureMessage);
    assertTrue(retrievedPassword.length > 0, "The key password should not be empty");
  }

  private void validateKeyPasswordConfigStoreEntryIsDeleted(
      String keyAlias, String failureMessage) {
    final RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> jwkSetManager.getPrivateKeyPassword(keyAlias),
            failureMessage);
    assertEquals(
        "Failed to retrieve JWK keystore entry password reference from configuration store",
        e.getMessage());
  }

  @Test
  public void testCreateAndGetPrivateKeyPassword() {
    final String keyAlias = UUID.randomUUID().toString();
    final String expectedPassword = "test.password01234!@#$/\\=";
    final char[] givenPasswordChars =
        jwkSetManager.createPrivateKeyPassword(keyAlias, expectedPassword);
    assertArrayEquals(expectedPassword.toCharArray(), givenPasswordChars);
    assertArrayEquals(
        expectedPassword.toCharArray(), jwkSetManager.getPrivateKeyPassword(keyAlias));
  }

  @Test
  public void enforceConsistentJwksStateHandlesOnlyKeyIsExpired() {
    assertNotNull(jwkSetManager.getPublicJWKS());
    List<Map<String, Object>> keys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    final String originalKeyId = (String) keys.get(0).get("kid");

    // Need to use the actual current time for the issue time and not-before-time because the
    // library we use to verify JWTs uses the system time to verify these properties.
    final Date originalIssueTime = new Date();
    final Date originalExpirationTime =
        new Date(clock.instant().plus(Duration.ofMinutes(JWT_EXPIRATION_MINUTES)).toEpochMilli());
    final JWTClaims originalJwtClaims =
        new ImmutableJWTClaims.Builder()
            .setJWTId(UUID.randomUUID().toString())
            .setSubject(TEST_USER.getUID().getId())
            .setAudience(CLUSTER_ID)
            .setIssuer(ISSUER_URL)
            .setIssueTime(originalIssueTime)
            .setNotBeforeTime(originalIssueTime)
            .setExpirationTime(originalExpirationTime)
            .build();
    final String originalToken = jwkSetManager.getSigner().sign(originalJwtClaims);

    // Set the time to a year and a day after the keys were first generated to simulate either
    // Dremio being offline for an extended period and starting up again, or key rotation failing
    // for an extended period.
    clock.add(Duration.ofDays(366));
    jwkSetManager.enforceConsistentJwksState();

    // The old key pair has been rotated out, but we'll continue to publish the old public key until
    // we enforce a consistent JWKS state again.
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, keys.size());
    final Set<String> keyIds =
        keys.stream().map(key -> (String) key.get("kid")).collect(Collectors.toSet());
    assertTrue(keyIds.contains(originalKeyId));

    // Enforce a consistent JWKS state so that we revoke the old key pair. At this point, we'll only
    // publish the new public key that was rotated in with the last consistency check.
    jwkSetManager.enforceConsistentJwksState();
    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    assertNotEquals(originalKeyId, keys.get(0).get("kid"));

    assertThrows(
        IllegalArgumentException.class,
        () -> jwkSetManager.getValidator().validate(originalToken),
        "The original token is no longer valid now that its key pair has been revoked");
  }

  @Test
  public void enforceConsistentJwksStateHandlesBothKeysExpired() throws ParseException {
    assertNotNull(jwkSetManager.getPublicJWKS());
    List<Map<String, Object>> originalKeys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, originalKeys.size());

    // Need to use the actual current time for the issue time and not-before-time because the
    // library we use to verify JWTs uses the system time to verify these properties.
    final Date originalIssueTime = new Date();
    final Date originalExpirationTime =
        new Date(clock.instant().plus(Duration.ofMinutes(JWT_EXPIRATION_MINUTES)).toEpochMilli());
    final JWTClaims originalJwtClaims =
        new ImmutableJWTClaims.Builder()
            .setJWTId(UUID.randomUUID().toString())
            .setSubject(TEST_USER.getUID().getId())
            .setAudience(CLUSTER_ID)
            .setIssuer(ISSUER_URL)
            .setIssueTime(originalIssueTime)
            .setNotBeforeTime(originalIssueTime)
            .setExpirationTime(originalExpirationTime)
            .build();
    final String originalToken = jwkSetManager.getSigner().sign(originalJwtClaims);

    clock.add(Duration.ofDays(SystemJWKSetManager.KEY_ROTATION_PERIOD_DAYS).plusSeconds(1));
    jwkSetManager.enforceConsistentJwksState();

    assertNotNull(jwkSetManager.getPublicJWKS());
    originalKeys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, originalKeys.size());

    assertEquals(USERNAME, jwkSetManager.getValidator().validate(originalToken).username);

    // Set the time to a year and a day after the keys were first generated to simulate either
    // Dremio being offline for an extended period and starting up again, or key rotation failing
    // for an extended period such that both keys have expired.
    clock.add(Duration.ofDays(366));
    jwkSetManager.enforceConsistentJwksState();
    List<Map<String, Object>> newKeys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, newKeys.size());
    final Set<String> originalKeyIds =
        originalKeys.stream().map(key -> (String) key.get("kid")).collect(Collectors.toSet());

    final Set<String> newKeyIds =
        newKeys.stream()
            .map(key -> (String) key.get("kid"))
            .filter(key -> !originalKeyIds.contains(key))
            .collect(Collectors.toSet());
    assertEquals(1, newKeyIds.size(), "There should be 1 new key in the JWK set");

    assertThrows(
        IllegalArgumentException.class,
        () -> jwkSetManager.getValidator().validate(originalToken),
        "The original token will fail to validate now that the key used to sign it is revoked");

    final Date newIssueTime = new Date();
    final Date newExpirationTime =
        new Date(clock.instant().plus(Duration.ofMinutes(JWT_EXPIRATION_MINUTES)).toEpochMilli());
    final JWTClaims newJwtClaims =
        new ImmutableJWTClaims.Builder()
            .setJWTId(UUID.randomUUID().toString())
            .setSubject(TEST_USER.getUID().getId())
            .setAudience(CLUSTER_ID)
            .setIssuer(ISSUER_URL)
            .setIssueTime(newIssueTime)
            .setNotBeforeTime(newIssueTime)
            .setExpirationTime(newExpirationTime)
            .build();
    final String newToken = jwkSetManager.getSigner().sign(newJwtClaims);
    assertEquals(USERNAME, jwkSetManager.getValidator().validate(newToken).username);
  }

  @Test
  public void rotateSingleKeyOnStart() throws Exception {
    assertNotNull(jwkSetManager.getPublicJWKS());
    List<Map<String, Object>> keys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    final String originalKeyId = (String) keys.get(0).get("kid");

    // Advance time passed the rotation period for the original generated key
    clock.add(Duration.ofDays(SystemJWKSetManager.KEY_ROTATION_PERIOD_DAYS).plusSeconds(1));

    // Create a new SystemJWKSetManager and start it to simulate a server restart
    try (final SystemJWKSetManager otherJwksManager =
        new SystemJWKSetManager(
            clock,
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> optionManager,
            () -> new LocalSchedulerService(1),
            () -> new ConfigurationStore(kvStoreProvider),
            () -> secretsCreator,
            () -> credentialsService,
            dremioConfig)) {
      otherJwksManager.start();

      assertNotNull(otherJwksManager.getPublicJWKS());
      List<Map<String, Object>> otherKeys =
          (List<Map<String, Object>>) otherJwksManager.getPublicJWKS().get("keys");
      assertEquals(
          2,
          otherKeys.size(),
          "A new key should have been rotated in on SystemJWKSetManager startup");
      final Optional<Map<String, Object>> optionalOriginalKeyId =
          otherKeys.stream().filter(key -> originalKeyId.equals(key.get("kid"))).findFirst();
      assertTrue(
          optionalOriginalKeyId.isPresent(),
          "The original key should have been loaded by the new SystemJWKSetManager");
    }
  }

  @Test
  public void revokeOldKeyOnStart() throws Exception {
    assertNotNull(jwkSetManager.getPublicJWKS());
    List<Map<String, Object>> keys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    final String originalKeyId = (String) keys.get(0).get("kid");

    // Advance time passed the rotation period for the original generated key
    clock.add(Duration.ofDays(SystemJWKSetManager.KEY_ROTATION_PERIOD_DAYS).plusSeconds(1));
    // Enforce consistency to trigger key rotation
    jwkSetManager.enforceConsistentJwksState();

    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, keys.size());
    final Optional<Map<String, Object>> optionalOriginalKey =
        keys.stream().filter(key -> originalKeyId.equals(key.get("kid"))).findFirst();
    assertTrue(
        optionalOriginalKey.isPresent(), "The original key should still be in the current JWK set");
    final String newKeyId =
        keys.stream()
            .map(key -> (String) key.get("kid"))
            .filter(keyId -> !originalKeyId.equals(keyId))
            .findFirst()
            .get();

    // Advance time passed the old key's revocation date
    clock.add(Duration.ofMinutes(JWT_EXPIRATION_MINUTES));

    // Create a new SystemJWKSetManager and start it to simulate a server restart
    try (final SystemJWKSetManager otherJwksManager =
        new SystemJWKSetManager(
            clock,
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> optionManager,
            () -> new LocalSchedulerService(1),
            () -> new ConfigurationStore(kvStoreProvider),
            () -> secretsCreator,
            () -> credentialsService,
            dremioConfig)) {
      otherJwksManager.start();

      assertNotNull(otherJwksManager.getPublicJWKS());
      List<Map<String, Object>> otherKeys =
          (List<Map<String, Object>>) otherJwksManager.getPublicJWKS().get("keys");
      assertEquals(1, otherKeys.size(), "The old key should have been revoked on startup");
      assertEquals(
          newKeyId,
          otherKeys.get(0).get("kid"),
          "The new key should be the only key in the current JWK set");
    }
  }

  @Test
  public void revokeAndRotateOldKeysOnStart() throws Exception {
    assertNotNull(jwkSetManager.getPublicJWKS());
    List<Map<String, Object>> keys =
        (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(1, keys.size());
    final String originalKeyId = (String) keys.get(0).get("kid");

    // Advance time passed the rotation period for the original generated key
    clock.add(Duration.ofDays(SystemJWKSetManager.KEY_ROTATION_PERIOD_DAYS).plusSeconds(1));
    // Enforce consistency to trigger key rotation
    jwkSetManager.enforceConsistentJwksState();

    keys = (List<Map<String, Object>>) jwkSetManager.getPublicJWKS().get("keys");
    assertEquals(2, keys.size());
    final Optional<Map<String, Object>> optionalOriginalKey =
        keys.stream().filter(key -> originalKeyId.equals(key.get("kid"))).findFirst();
    assertTrue(
        optionalOriginalKey.isPresent(), "The original key should still be in the current JWK set");
    final String secondKeyId =
        keys.stream()
            .map(key -> (String) key.get("kid"))
            .filter(keyId -> !originalKeyId.equals(keyId))
            .findFirst()
            .get();

    // Advance time passed both the new and old key revocation dates
    clock.add(
        Duration.ofMinutes(JWT_EXPIRATION_MINUTES)
            .plusDays(SystemJWKSetManager.KEY_ROTATION_PERIOD_DAYS));

    // Create a new SystemJWKSetManager and start it to simulate a server restart
    try (final SystemJWKSetManager otherJwksManager =
        new SystemJWKSetManager(
            clock,
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> optionManager,
            () -> new LocalSchedulerService(1),
            () -> new ConfigurationStore(kvStoreProvider),
            () -> secretsCreator,
            () -> credentialsService,
            dremioConfig)) {
      otherJwksManager.start();

      assertNotNull(otherJwksManager.getPublicJWKS());
      List<Map<String, Object>> otherKeys =
          (List<Map<String, Object>>) otherJwksManager.getPublicJWKS().get("keys");
      assertEquals(
          2,
          otherKeys.size(),
          "The old key should have been revoked and a new key should have been rotated in on startup");
      final Set<String> otherKeyIds =
          otherKeys.stream().map(key -> (String) key.get("kid")).collect(Collectors.toSet());
      assertFalse(
          otherKeyIds.contains(originalKeyId),
          "The original key should have been revoked on startup");
      assertTrue(
          otherKeyIds.contains(secondKeyId), "The second key should still be in the JWK set");
    }
  }
}
