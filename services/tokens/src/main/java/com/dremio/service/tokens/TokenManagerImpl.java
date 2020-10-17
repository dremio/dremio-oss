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

import static com.google.common.base.Preconditions.checkArgument;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.config.DremioConfig;
import com.dremio.dac.proto.model.tokens.SessionState;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Sets;

/**
 * Token manager implementation.
 */
@Options
public class TokenManagerImpl implements TokenManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TokenManagerImpl.class);

  private static final String LOCAL_TASK_LEADER_NAME = "tokenmanager";

  public static final PositiveLongValidator TOKEN_EXPIRATION_TIME_MINUTES =
    new PositiveLongValidator(
      "token.expiration.min",
      Integer.MAX_VALUE,
      TimeUnit.MINUTES.convert(30, TimeUnit.HOURS));

  public static final PositiveLongValidator TOKEN_RELEASE_LEADERSHIP_MS =
    new PositiveLongValidator(
      "token.release.leadership.ms",
      Long.MAX_VALUE,
      TimeUnit.HOURS.toMillis(40));

  private final SecureRandom generator = new SecureRandom();

  private final Provider<LegacyKVStoreProvider> kvProvider;
  private final Provider<SchedulerService> schedulerService;
  private final Provider<OptionManager> optionManagerProvider;
  private final boolean isMaster;
  private final int cacheSize;
  private final int cacheExpiration;

  private LegacyKVStore<String, SessionState> tokenStore;
  private LoadingCache<String, SessionState> tokenCache;

  public TokenManagerImpl(final Provider<LegacyKVStoreProvider> kvProvider,
                          final Provider<SchedulerService> schedulerService,
                          final Provider<OptionManager> optionManagerProvider,
                          final boolean isMaster,
                          final DremioConfig config) {
    this(kvProvider,
        schedulerService,
        optionManagerProvider,
        isMaster,
        config.getInt(DremioConfig.WEB_TOKEN_CACHE_SIZE),
        config.getInt(DremioConfig.WEB_TOKEN_CACHE_EXPIRATION));
  }

  @VisibleForTesting
  TokenManagerImpl(final Provider<LegacyKVStoreProvider> kvProvider,
                   final Provider<SchedulerService> schedulerService,
                   final Provider<OptionManager> optionManagerProvider,
                   final boolean isMaster,
                   final int cacheSize,
                   final int cacheExpiration) {
    this.kvProvider = kvProvider;
    this.schedulerService = schedulerService;
    this.optionManagerProvider = optionManagerProvider;
    this.isMaster = isMaster;
    this.cacheSize = cacheSize;
    this.cacheExpiration = cacheExpiration;
  }

  @Override
  public void start() {
    this.tokenStore = kvProvider.get().getStore(TokenStoreCreator.class);
    this.tokenCache = CacheBuilder.newBuilder()
      .maximumSize(cacheSize)
      // so a token is fetched from the store ever so often
      .expireAfterWrite(cacheExpiration, TimeUnit.MINUTES)
      .removalListener(new RemovalListener<String, SessionState>() {
        @Override
        public void onRemoval(RemovalNotification<String, SessionState> notification) {
          if (!notification.wasEvicted()) {
            // TODO: broadcast this message to other coordinators; for now, cache on each coordinator could allow
            // an invalid token to be used for up to "expiration"
            tokenStore.delete(notification.getKey());
          }
        }
      })
      .build(new CacheLoader<String, SessionState>() {
        @Override
        public SessionState load(String key) {
          return tokenStore.get(key);
        }
      });

    if (isMaster) {
      final long tokenReleaseLeadership = optionManagerProvider.get().getOption(TOKEN_RELEASE_LEADERSHIP_MS);
      final Schedule everyDay = Schedule.Builder.everyDays(1)
        .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
        .releaseOwnershipAfter(tokenReleaseLeadership, TimeUnit.MILLISECONDS).build();
      schedulerService.get().schedule(everyDay, new RemoveExpiredTokens());
    }
  }

  @Override
  public void close() {
  }

  // From https://stackoverflow.com/questions/41107/how-to-generate-a-random-alpha-numeric-string
  // ... This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding
  // them in base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32
  // number can encode 5 bits, so 128 is rounded up to the next multiple of 5 ... Why 32? Because 32 = 2^5;
  // each character will represent exactly 5 bits, and 130 bits can be evenly divided into characters.
  private String newToken() {
    return new BigInteger(130, generator).toString(32);
  }

  @VisibleForTesting
  TokenDetails createToken(final String username, final String clientAddress, final long issuedAt,
                     final long expiresAt) {
    final String token = newToken();
    final SessionState state = new SessionState()
      .setUsername(username)
      .setClientAddress(clientAddress)
      .setIssuedAt(issuedAt)
      .setExpiresAt(expiresAt);

    tokenStore.put(token, state);
    tokenCache.put(token, state);
    logger.trace("Created token: {}", token);
    return TokenDetails.of(token, username, expiresAt);
  }

  @Override
  public TokenDetails createToken(final String username, final String clientAddress) {
    final long now = System.currentTimeMillis();
    final long expires = now + TimeUnit.MILLISECONDS.convert(optionManagerProvider
      .get()
      .getOption(TOKEN_EXPIRATION_TIME_MINUTES), TimeUnit.MINUTES);

    return createToken(username, clientAddress, now, expires);
  }

  private SessionState getSessionState(final String token) {
    checkArgument(token != null, "invalid token");
    final SessionState value;
    try {
      value = tokenCache.getUnchecked(token);
    } catch (CacheLoader.InvalidCacheLoadException ignored) {
      throw new IllegalArgumentException("invalid token");
    }

    return value;
  }

  @Override
  public TokenDetails validateToken(final String token) throws IllegalArgumentException {
    final SessionState value = getSessionState(token);
    if (System.currentTimeMillis() >= value.getExpiresAt()) {
      tokenCache.invalidate(token); // removes from the store as well
      throw new IllegalArgumentException("token expired");
    }

    logger.trace("Validated token: {}", token);
    return TokenDetails.of(token, value.getUsername(), value.getExpiresAt());
  }

  @Override
  public void invalidateToken(final String token) {
    logger.trace("Invalidate token: {}", token);
    tokenCache.invalidate(token); // removes from the store as well
  }

  @VisibleForTesting
  LegacyKVStore<String, SessionState> getTokenStore() {
    return tokenStore;
  }

  /**
   * Periodically removes expired tokens. When a user abandons a session, that token maybe left behind.
   * Since the token may never be accessed in the future, this task cleans up the store. This task must run
   * only on master coordinator.
   */
  class RemoveExpiredTokens implements Runnable {

    @Override
    public void run() {
      final long now = System.currentTimeMillis();
      final Set<String> expiredTokens = Sets.newHashSet();
      for (final Map.Entry<String, SessionState> entry : tokenStore.find()) {
        if (now >= entry.getValue().getExpiresAt()) {
          expiredTokens.add(entry.getKey());
        }
      }

      for (final String token : expiredTokens) {
        tokenStore.delete(token);
      }
    }
  }
}
