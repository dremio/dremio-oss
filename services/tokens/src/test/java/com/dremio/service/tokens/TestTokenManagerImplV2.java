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
package com.dremio.service.tokens;

import static com.dremio.service.tokens.TokenManagerImplV2.TOKEN_EXPIRATION_TIME_MINUTES;
import static com.dremio.test.DremioTest.DEFAULT_DREMIO_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.tokens.jwks.JWKSetManager;
import com.dremio.service.tokens.jwks.SystemJWKSetManager;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.proc.BadJWSException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.threeten.extra.MutableClock;

public class TestTokenManagerImplV2 {
  private static final int JWT_EXPIRATION_MINUTES = 30;

  private TokenManager legacyTokenManager;
  private DremioConfig dremioConfig;
  private WebServerInfoProvider webServerInfoProvider;
  private UserResolver userResolver;
  private OptionManager optionManager;
  private SystemJWKSetManager jwksManager;
  private TokenManagerImplV2 tokenManager;
  private MutableClock clock;
  private LegacyKVStoreProvider provider;
  private ConfigurationStore configurationStore;
  private CredentialsService credentialsService;
  private Cipher cipher;
  private SecretsCreator secretsCreator;

  @TempDir private Path securityFolder;

  private static final User testUser =
      SimpleUser.newBuilder()
          .setUserName("testuser")
          .setUID(new UID(UUID.randomUUID().toString()))
          .build();
  private static final String username = "testuser";
  private static final String clientAddress = "localhost";

  @BeforeEach
  public void startServices() throws Exception {
    optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(mock(OptionValidatorListingImpl.class))
            .withOptionManager(mock(DefaultOptionManager.class))
            .withOptionManager(mock(SystemOptionManager.class))
            .build();
    withJwtAccessTokensEnabled(true);
    when(optionManager.getOption("token.jwt-access-token.expiration.minutes"))
        .thenReturn(
            OptionValue.createOption(
                OptionValue.Kind.LONG,
                OptionValue.OptionType.SYSTEM,
                "token.jwt-access-token.expiration.minutes",
                "" + JWT_EXPIRATION_MINUTES));
    when(optionManager.getOption("token.expiration.min"))
        .thenReturn(
            OptionValue.createOption(
                OptionValue.Kind.LONG,
                OptionValue.OptionType.SYSTEM,
                "token.expiration.min",
                "30"));
    when(optionManager.getOption("token.release.leadership.ms"))
        .thenReturn(
            OptionValue.createOption(
                OptionValue.Kind.LONG,
                OptionValue.OptionType.SYSTEM,
                "token.release.leadership.ms",
                "144000000"));
    when(optionManager.getOption("token.temporary.expiration.sec"))
        .thenReturn(
            OptionValue.createOption(
                OptionValue.Kind.LONG,
                OptionValue.OptionType.SYSTEM,
                "token.temporary.expiration.sec",
                "5"));

    provider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    provider.start();

    final URL baseUrl = new URL("https://localhost:9047");
    webServerInfoProvider =
        new WebServerInfoProvider() {
          @Override
          public String getClusterId() {
            return "cluster-id";
          }

          @Override
          public URL getBaseURL() {
            return baseUrl;
          }
        };

    userResolver = mock(UserResolver.class);
    when(userResolver.getUser(username)).thenReturn(testUser);
    when(userResolver.getUser(testUser.getUID())).thenReturn(testUser);

    dremioConfig =
        DEFAULT_DREMIO_CONFIG.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING,
            securityFolder.resolve("default-security-dir").toString());
    legacyTokenManager =
        new TokenManagerImpl(
            () -> provider,
            () -> mock(SchedulerService.class),
            () -> optionManager,
            true,
            dremioConfig);
    legacyTokenManager.start();

    clock = MutableClock.of(Instant.now(), ZoneOffset.UTC);

    this.credentialsService =
        CredentialsServiceImpl.newInstance(
            dremioConfig, Set.of(SystemSecretCredentialsProvider.class));
    credentialsService.start();

    this.cipher = new SystemCipher(dremioConfig, credentialsService);
    this.secretsCreator = new SecretsCreatorImpl(() -> cipher, () -> credentialsService);
    this.configurationStore = new ConfigurationStore(provider);

    this.jwksManager =
        spy(
            new SystemJWKSetManager(
                Clock.systemUTC(),
                () -> webServerInfoProvider,
                () -> userResolver,
                () -> optionManager,
                () -> new LocalSchedulerService(1),
                () -> configurationStore,
                () -> secretsCreator,
                () -> credentialsService,
                dremioConfig));
    jwksManager.start();

    tokenManager =
        new TokenManagerImplV2(
            clock,
            () -> legacyTokenManager,
            () -> optionManager,
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> jwksManager);
    tokenManager.start();
  }

  @AfterEach
  public void stopServices() throws Exception {
    if (provider != null) {
      provider.close();
    }
    if (tokenManager != null) {
      tokenManager.close();
    }
    if (legacyTokenManager != null) {
      legacyTokenManager.close();
    }
    if (jwksManager != null) {
      jwksManager.close();
    }
    if (credentialsService != null) {
      credentialsService.close();
    }
  }

  @Test
  public void validJwt() {
    final TokenDetails details = tokenManager.createJwt(username, clientAddress);
    assertEquals(tokenManager.validateToken(details.token).username, username);
  }

  @Test
  public void validJwtWithValidExpiresAt() {
    final long defaultDurationInMinutes = optionManager.getOption(TOKEN_EXPIRATION_TIME_MINUTES);
    // half of the default:
    final long expiresAt =
        clock.instant().plus(Duration.ofMinutes(defaultDurationInMinutes / 2)).toEpochMilli();
    final TokenDetails details = tokenManager.createJwt(username, clientAddress, expiresAt);
    assertEquals(details.expiresAt, expiresAt);
  }

  @Test
  public void validJwtWithLongExpiresAt() {
    final long defaultDurationInMinutes = optionManager.getOption(TOKEN_EXPIRATION_TIME_MINUTES);
    // longer than the default:
    final long expectedExpiresAtEpochMs =
        clock.instant().plus(Duration.ofMinutes(defaultDurationInMinutes)).toEpochMilli();
    final long expiresAtEpochMs =
        clock.instant().plus(Duration.ofMinutes(defaultDurationInMinutes + 1)).toEpochMilli();
    final TokenDetails details = tokenManager.createJwt(username, clientAddress, expiresAtEpochMs);
    assertEquals(expectedExpiresAtEpochMs, details.expiresAt);
  }

  @Test
  public void nullToken() {
    assertThrows(IllegalArgumentException.class, () -> tokenManager.validateToken(null));
  }

  @Test
  public void invalidToken() {
    assertThrows(IllegalArgumentException.class, () -> tokenManager.validateToken("invalidtoken"));
  }

  @Test
  public void jwtSignedWithWrongKeyFailsValidation() throws Exception {
    final JWKSetManager otherJwksManager =
        new SystemJWKSetManager(
            Clock.systemUTC(),
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> optionManager,
            () -> new LocalSchedulerService(1),
            () -> configurationStore,
            () -> secretsCreator,
            () -> credentialsService,
            DEFAULT_DREMIO_CONFIG.withValue(
                DremioConfig.LOCAL_WRITE_PATH_STRING,
                securityFolder.resolve("other-security-dir").toString()));
    otherJwksManager.start();

    final TokenManager otherTokenManager =
        new TokenManagerImplV2(
            clock,
            () -> legacyTokenManager,
            () -> optionManager,
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> otherJwksManager);
    otherTokenManager.start();

    final TokenDetails tokenDetails = tokenManager.createJwt(username, clientAddress);
    assertThrows(
        IllegalArgumentException.class, () -> otherTokenManager.validateToken(tokenDetails.token));
  }

  @Test
  public void tamperedJwtFailsValidation() throws IOException {
    final TokenDetails details = tokenManager.createJwt(username, clientAddress);
    final String[] tokenParts = details.token.split("\\.");
    final ObjectMapper mapper = new ObjectMapper();

    final Map<String, String> claims =
        mapper.readValue(Base64.getDecoder().decode(tokenParts[1]), Map.class);
    claims.put("malicious", "value");

    final String tamperedToken =
        tokenParts[0]
            + "."
            + Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(claims))
            + "."
            + tokenParts[2];

    final Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> tokenManager.validateToken(tamperedToken));
    assertInstanceOf(BadJWSException.class, e.getCause());
  }

  @Test
  public void useJwtAfterExpiry() {
    // Set "now" to 31 minutes second ago so the generated token will have expired 1 minute ago.
    // Note that our JWT library allows 60 seconds of clock skew by default, which is why the token
    // must be at least 1 minute expired for it to be considered expired by our system.
    clock.setInstant(Instant.now().minus(Duration.ofMinutes(JWT_EXPIRATION_MINUTES + 1)));

    final TokenDetails details = tokenManager.createJwt(username, clientAddress);
    assertThrows(IllegalArgumentException.class, () -> tokenManager.validateToken(details.token));
  }

  @Test
  public void createJwtUserDoesNotExist() throws UserNotFoundException {
    final String nonExtantUsername = "nonextant";
    when(userResolver.getUser(nonExtantUsername))
        .thenThrow(new UserNotFoundException(nonExtantUsername));

    assertThrows(
        IllegalArgumentException.class,
        () -> tokenManager.createJwt(nonExtantUsername, clientAddress));
  }

  @Test
  public void createJwtThrowsWhenJwtsDisabled() {
    withJwtAccessTokensEnabled(false);
    assertThrows(
        UnsupportedOperationException.class, () -> tokenManager.createJwt(username, clientAddress));
  }

  @Test
  public void validateTokenUserDoesNotExist() throws UserNotFoundException {
    final String usernameToBeDeleted = "nonextant";
    final String userId = UUID.randomUUID().toString();
    final User userToBeDeleted =
        SimpleUser.newBuilder().setUserName(userId).setUID(new UID(userId)).build();
    when(userResolver.getUser(usernameToBeDeleted)).thenReturn(userToBeDeleted);

    final TokenDetails tokenDetails = tokenManager.createJwt(usernameToBeDeleted, clientAddress);
    assertEquals(usernameToBeDeleted, tokenDetails.username);

    // Simulate user deletion between the token being created and the token being validated
    when(userResolver.getUser(new UID(userId)))
        .thenThrow(new UserNotFoundException(usernameToBeDeleted));
    assertThrows(
        IllegalArgumentException.class, () -> tokenManager.validateToken(tokenDetails.token));
  }

  @Test
  public void validateTokenFallsBackToLegacyTokenManager() {
    final TokenDetails tokenDetails = legacyTokenManager.createToken(username, clientAddress);
    assertEquals(tokenManager.validateToken(tokenDetails.token).username, username);
  }

  @Test
  public void createTokenReturnsLegacyToken() {
    final TokenDetails tokenDetails = tokenManager.createToken(username, clientAddress);
    assertEquals(legacyTokenManager.validateToken(tokenDetails.token).username, username);
  }

  @Test
  public void createTokenWithScopes() {
    final List<String> requestedScopes = List.of("dremio.all", "offline_access");
    final long expiresAt = clock.instant().plus(Duration.ofDays(1)).toEpochMilli();
    final TokenDetails tokenDetails =
        tokenManager.createToken(username, clientAddress, expiresAt, requestedScopes);

    final TokenDetails validatedToken = tokenManager.validateToken(tokenDetails.token);
    assertEquals(validatedToken.username, username);
    assertEquals(requestedScopes, validatedToken.getScopes());
  }

  @Test
  public void invalidateJwtThrows() {
    final TokenDetails tokenDetails = tokenManager.createJwt(username, clientAddress);
    assertThrows(
        IllegalArgumentException.class, () -> tokenManager.invalidateToken(tokenDetails.token));
  }

  @Test
  public void invalidateTokenJwtsDisabled() {
    withJwtAccessTokensEnabled(false);

    final TokenDetails tokenDetails = tokenManager.createToken(username, clientAddress);
    assertEquals(username, tokenManager.validateToken(tokenDetails.token).username);

    tokenManager.invalidateToken(tokenDetails.token);
    verify(jwksManager, never()).getValidator();
    assertThrows(
        IllegalArgumentException.class, () -> tokenManager.validateToken(tokenDetails.token));
  }

  @Test
  public void onlyValidateLegacyTokensWhenJwtsDisabled() {
    final TokenDetails jwtDetails = tokenManager.createJwt(username, clientAddress);
    assertEquals(tokenManager.validateToken(jwtDetails.token).username, username);

    withJwtAccessTokensEnabled(false);

    assertThrows(
        IllegalArgumentException.class, () -> tokenManager.validateToken(jwtDetails.token));

    final TokenDetails opaqueToken = tokenManager.createToken(username, clientAddress);
    assertEquals(tokenManager.validateToken(opaqueToken.token).username, username);
  }

  @Test
  public void validateAndInvalidateLegacyTokensWithUninitializedKeystore() throws Exception {
    // Disable JWTs and start up a new SystemJWKSetManager with a different security folder location
    // configured so there is no initialized keystore. In this case, we still want to fall back to
    // validating/invalidating tokens as if they are all legacy opaque tokens.
    final DremioConfig differentSecurityFolderConfig =
        dremioConfig.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING, securityFolder.resolve("new-subdir").toString());
    withJwtAccessTokensEnabled(false);

    try (final SystemJWKSetManager otherJwksManager =
        new SystemJWKSetManager(
            Clock.systemUTC(),
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> optionManager,
            () -> new LocalSchedulerService(1),
            () -> configurationStore,
            () -> secretsCreator,
            () -> credentialsService,
            differentSecurityFolderConfig)) {
      otherJwksManager.start();

      try (final TokenManagerImplV2 otherTokenManager =
          new TokenManagerImplV2(
              clock,
              () -> legacyTokenManager,
              () -> optionManager,
              () -> webServerInfoProvider,
              () -> userResolver,
              () -> otherJwksManager)) {
        otherTokenManager.start();

        // Enable JWT access tokens at runtime even though the JWKS keystore will not be initialized
        // until a server restart. Legacy token validation should continue to work until the server
        // restart.
        withJwtAccessTokensEnabled(true);

        final TokenDetails tokenDetails = otherTokenManager.createToken(username, clientAddress);
        assertEquals(username, otherTokenManager.validateToken(tokenDetails.token).username);

        otherTokenManager.invalidateToken(tokenDetails.token);
        assertThrows(
            IllegalArgumentException.class,
            () -> otherTokenManager.validateToken(tokenDetails.token));
      }
    }
  }

  @Test
  public void useLegacyTokenAfterLogOut() {
    final TokenDetails tokenDetails = tokenManager.createToken(username, clientAddress);
    assertEquals(tokenManager.validateToken(tokenDetails.token).username, username);

    // Logout
    tokenManager.invalidateToken(tokenDetails.token);

    assertThrows(
        IllegalArgumentException.class, () -> tokenManager.validateToken(tokenDetails.token));
  }

  @Test
  public void reloadSamePrivateKey() throws Exception {
    try (final JWKSetManager otherJwksManager =
        new SystemJWKSetManager(
            Clock.systemUTC(),
            () -> webServerInfoProvider,
            () -> userResolver,
            () -> optionManager,
            () -> new LocalSchedulerService(1),
            () -> configurationStore,
            () -> secretsCreator,
            () -> credentialsService,
            dremioConfig)) {
      otherJwksManager.start();

      try (final TokenManagerImplV2 otherTokenManager =
          new TokenManagerImplV2(
              clock,
              () -> legacyTokenManager,
              () -> optionManager,
              () -> webServerInfoProvider,
              () -> userResolver,
              () -> otherJwksManager)) {
        otherTokenManager.start();

        // Both TokenManagers should load the same keys, so they should be able to create and
        // validate each others' tokens. This shows a server restart won't invalidate otherwise
        // valid JWTs.
        final TokenDetails tokenDetails = tokenManager.createJwt(username, clientAddress);
        assertEquals(tokenManager.validateToken(tokenDetails.token).username, username);
        assertEquals(otherTokenManager.validateToken(tokenDetails.token).username, username);

        final TokenDetails otherTokenDetails = otherTokenManager.createJwt(username, clientAddress);
        assertEquals(otherTokenManager.validateToken(otherTokenDetails.token).username, username);
        assertEquals(tokenManager.validateToken(tokenDetails.token).username, username);
      }
    }
  }

  @Test
  public void createAndValidateThirdPartyToken() {
    final String clientId = UUID.randomUUID().toString();
    final List<String> expectedScopes = List.of("openid", "profile", "email");
    final TokenDetails tokenDetails =
        tokenManager.createThirdPartyToken(
            username, clientAddress, clientId, expectedScopes, Duration.ofHours(1).toMillis());
    final TokenDetails validatedToken = tokenManager.validateToken(tokenDetails.token);
    assertEquals(username, validatedToken.username);
    assertEquals(clientId, validatedToken.clientId);
    assertEquals(new HashSet<>(expectedScopes), new HashSet<>(validatedToken.getScopes()));
  }

  @Test
  public void createAndValidateTemporaryToken() {
    final Map<String, List<String>> expectedQueryParams =
        Map.of("param", List.of("some", "value"), "otherParam", List.of("test"));
    final String expectedPath = "/test/path";
    final TokenDetails tokenDetails =
        tokenManager.createTemporaryToken(
            username, expectedPath, expectedQueryParams, Duration.ofHours(1).toMillis());
    final TokenDetails validatedToken =
        tokenManager.validateTemporaryToken(tokenDetails.token, expectedPath, expectedQueryParams);
    assertEquals(username, validatedToken.username);
  }

  private void withJwtAccessTokensEnabled(boolean settingValue) {
    when(optionManager.getOption("token.jwt-access-token.enabled"))
        .thenReturn(
            OptionValue.createOption(
                Kind.BOOLEAN,
                OptionValue.OptionType.SYSTEM,
                "token.jwt-access-token.enabled",
                settingValue ? "true" : "false"));
  }
}
