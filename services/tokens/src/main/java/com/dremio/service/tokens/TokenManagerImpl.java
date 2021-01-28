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
import static java.lang.Math.min;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.config.DremioConfig;
import com.dremio.dac.proto.model.tokens.QueryParam;
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

  public static final PositiveLongValidator TEMPORARY_TOKEN_EXPIRATION_TIME_SECONDS =
    new PositiveLongValidator(
      "token.temporary.expiration.sec",
      Integer.MAX_VALUE,
      TimeUnit.SECONDS.convert(5, TimeUnit.MINUTES));

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
                     final long expiresAt, final String path, final List<QueryParam> queryParams) {
    final String token = newToken();
    final SessionState state = new SessionState()
      .setUsername(username)
      .setClientAddress(clientAddress)
      .setIssuedAt(issuedAt)
      .setExpiresAt(expiresAt)
      .setPath(path)
      .setQueryParamsList(queryParams);

    tokenStore.put(token, state);
    tokenCache.put(token, state);
    logger.trace("Created token for user: {}", username);
    return TokenDetails.of(token, username, expiresAt);
  }

  @Override
  public TokenDetails createToken(final String username, final String clientAddress) {
    final long now = System.currentTimeMillis();
    final long expires = now + TimeUnit.MILLISECONDS.convert(optionManagerProvider
      .get()
      .getOption(TOKEN_EXPIRATION_TIME_MINUTES), TimeUnit.MINUTES);

    return createToken(username, clientAddress, now, expires, null, null);
  }

  @Override
  public TokenDetails createTemporaryToken(String username,
                                           String path,
                                           Map<String, List<String>> queryParamsMap,
                                           long durationMillis) {
    checkArgument(path != null, "missing designated URL path");
    checkArgument(queryParamsMap != null, "missing designated URL query params");

    final long now = System.currentTimeMillis();
    final long duration = min(durationMillis, TimeUnit.SECONDS.toMillis(optionManagerProvider
      .get()
      .getOption(TEMPORARY_TOKEN_EXPIRATION_TIME_SECONDS)));
    final long expires = now + duration;

    List<QueryParam> queryParamsList = queryParamsMap.entrySet().stream()
      .map(e -> new QueryParam().setKey(e.getKey()).setValuesList(e.getValue()))
      .collect(Collectors.toList());

    return createToken(username, "", now, expires, path, queryParamsList);
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

    if (value.getPath() != null) {
      // non temporary access API is not allowed to use temporary token
      throw new IllegalArgumentException("invalid token");
    }

    logger.trace("Validated token for user: {}", value.getUsername());
    return TokenDetails.of(token, value.getUsername(), value.getExpiresAt());
  }

  public TokenDetails validateTemporaryToken(String token,
                                             String path,
                                             Map<String, List<String>> queryParams) throws IllegalArgumentException {
    checkArgument(path != null,"undefined request path");
    checkArgument(queryParams != null,"undefined request query params");
    final SessionState value = getSessionState(token);
    if (System.currentTimeMillis() >= value.getExpiresAt()) {
      tokenCache.invalidate(token); // removes from the store as well
      throw new IllegalArgumentException("token expired");
    }

    if (value.getPath() == null) {  // token is a regular session token
      throw new IllegalArgumentException("invalid token");
    }
    validateRequestUrl(path, value.getPath(), queryParams, value.getQueryParamsList());

    logger.trace("Validated token for user: {}", value.getUsername());
    return TokenDetails.of(token, value.getUsername(), value.getExpiresAt());
  }

  /**
   * Validate if the incoming REST API request url matches the designated request url of the token
   * @param requestUriPath incoming REST API url path
   * @param designatedUrlPath designated REST API url path.
   * @param requestUriQueryParams incoming REST API url query parameters
   * @param designatedUrlQueryParams designated REST API url query parameters
   * @throws IllegalArgumentException if validation fails
   */
  static void validateRequestUrl(final String requestUriPath, final String designatedUrlPath,
                                 final Map<String, List<String>> requestUriQueryParams,
                                 final List<QueryParam> designatedUrlQueryParams) throws IllegalArgumentException {
    if (!requestsUrlPathsMatch(requestUriPath, designatedUrlPath) ||
      !requestsQueryParamsMatch(requestUriQueryParams, designatedUrlQueryParams)) {
      logger.debug("Incoming request and designated request did not match.");
      throw new IllegalArgumentException("invalid token");
    }
  }

  /**
   * @return true if both paths are the same. Note: ignore starting or ending slash.
   */
  private static boolean requestsUrlPathsMatch(String path1, String path2) {
    return trimPath(path1).equals(trimPath(path2));
  }

  private static String trimPath(String path) {
    path = path.startsWith("/") ? path.substring(1) : path;
    return path.endsWith("/") ? path.substring(0, path.length()-1) : path;
  }

  /**
   * Check if both query params have the same set of keys and each corresponding
   * value(s) should be the same (order and duplication don't matter).
   * @param queryParams1 a map of query param key to values. Values could contain duplicates.
   * @param queryParams2 a list of QueryParam. No duplicate keys from QueryParam instances of the list.
   * @return true if both match.
   */
  private static boolean requestsQueryParamsMatch(final Map<String, List<String>> queryParams1,
                                                  final List<QueryParam> queryParams2) {
    if (queryParams1.size() != queryParams2.size()) {
      return false;
    }
    for (QueryParam q : queryParams2) {
      List<String> v = queryParams1.get(q.getKey());
      if (v == null || !(v.containsAll(q.getValuesList()) && q.getValuesList().containsAll(v))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void invalidateToken(final String token) {
    logger.trace("Invalidate token");
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
