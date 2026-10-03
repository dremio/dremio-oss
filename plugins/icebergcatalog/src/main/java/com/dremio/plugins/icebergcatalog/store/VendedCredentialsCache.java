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
package com.dremio.plugins.icebergcatalog.store;

import com.dremio.common.exceptions.UserException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * The storage credentials an Iceberg REST catalog vends per table, cached until shortly before they
 * expire.
 *
 * <p>Every node resolves them on its own, from the catalog (see {@link
 * CatalogAccessor#loadTableStorageProperties}): credentials never travel in query plans or
 * fragments. A lookup that yields no credentials (the catalog did not vend any, or could not be
 * reached) is cached briefly too, so that file system creation does not call the catalog every
 * time; callers then use the source's storage properties.
 */
final class VendedCredentialsCache {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VendedCredentialsCache.class);

  /** Credentials are refreshed this long before they expire (at most half their lifetime). */
  @VisibleForTesting static final long REFRESH_MARGIN_MILLIS = TimeUnit.MINUTES.toMillis(5);

  /** How long credentials without an expiry time are used before asking the catalog again. */
  @VisibleForTesting static final long DEFAULT_LIFETIME_MILLIS = TimeUnit.MINUTES.toMillis(10);

  /** How long the answer "the catalog vended no credentials for this table" is kept. */
  @VisibleForTesting
  static final long NO_CREDENTIALS_LIFETIME_MILLIS = TimeUnit.MINUTES.toMillis(5);

  /** How long a failed lookup is kept before the catalog is asked again. */
  @VisibleForTesting static final long FAILURE_LIFETIME_MILLIS = TimeUnit.SECONDS.toMillis(30);

  /** Lower bound of every lifetime, so that (nearly) expired credentials cause no request storm. */
  @VisibleForTesting static final long MIN_LIFETIME_MILLIS = TimeUnit.SECONDS.toMillis(10);

  private static final long MAX_ENTRIES = 100_000;

  /** The credentials of a table (if any) and until when they, or their absence, may be used. */
  static final class Result {
    private final Optional<VendedStorageCredentials> credentials;
    private final long validUntilMillis;

    Result(Optional<VendedStorageCredentials> credentials, long validUntilMillis) {
      this.credentials = credentials;
      this.validUntilMillis = validUntilMillis;
    }

    Optional<VendedStorageCredentials> getCredentials() {
      return credentials;
    }

    /** Wall clock time (epoch millis) after which the result must be looked up again. */
    long getValidUntilMillis() {
      return validUntilMillis;
    }

    @Override
    public String toString() {
      return credentials.map(Object::toString).orElse("no vended credentials")
          + ", valid until "
          + Instant.ofEpochMilli(validUntilMillis);
    }
  }

  private final Function<TableIdentifier, Map<String, String>> tableStorageProperties;
  private final String sourceName;
  private final UnaryOperator<String> redactor;
  private final LongSupplier clockMillis;
  private final Cache<TableIdentifier, Result> cache;

  /**
   * @param tableStorageProperties loads the FileIO properties of a table from the catalog
   * @param redactor masks the source's secret values in catalog error messages
   */
  VendedCredentialsCache(
      Function<TableIdentifier, Map<String, String>> tableStorageProperties,
      String sourceName,
      UnaryOperator<String> redactor) {
    this(
        tableStorageProperties,
        sourceName,
        redactor,
        Ticker.systemTicker(),
        System::currentTimeMillis);
  }

  @VisibleForTesting
  VendedCredentialsCache(
      Function<TableIdentifier, Map<String, String>> tableStorageProperties,
      String sourceName,
      UnaryOperator<String> redactor,
      Ticker ticker,
      LongSupplier clockMillis) {
    this.tableStorageProperties = Preconditions.checkNotNull(tableStorageProperties);
    this.sourceName = sourceName;
    this.redactor = Preconditions.checkNotNull(redactor);
    this.clockMillis = clockMillis;
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(MAX_ENTRIES)
            .ticker(ticker)
            .expireAfter(
                new Expiry<TableIdentifier, Result>() {
                  @Override
                  public long expireAfterCreate(TableIdentifier key, Result value, long now) {
                    return remainingNanos(value);
                  }

                  @Override
                  public long expireAfterUpdate(
                      TableIdentifier key, Result value, long now, long currentDuration) {
                    return remainingNanos(value);
                  }

                  @Override
                  public long expireAfterRead(
                      TableIdentifier key, Result value, long now, long currentDuration) {
                    return currentDuration;
                  }
                })
            .build();
  }

  private long remainingNanos(Result result) {
    return TimeUnit.MILLISECONDS.toNanos(
        Math.max(0L, result.getValidUntilMillis() - clockMillis.getAsLong()));
  }

  /**
   * Returns the cached result for the table, asking the catalog if there is none (any more).
   *
   * <p>Entries expire on the monotonic ticker, while their validity is wall clock time. When the
   * wall clock moves forward (NTP step, resumed VM), a cached result can be past its validity
   * before its entry expires: it is then replaced, so that callers never get expired credentials or
   * a validity in the past.
   */
  Result get(TableIdentifier table) {
    Result result = cache.get(table, this::load);
    if (result.getValidUntilMillis() <= clockMillis.getAsLong()) {
      cache.asMap().remove(table, result);
      result = cache.get(table, this::load);
    }
    return result;
  }

  /**
   * Drops the cached result of one table, e.g. after the table was created or dropped: a table
   * created again under the same name may live elsewhere, and a new table gets credentials only
   * once it exists.
   */
  void invalidate(TableIdentifier table) {
    cache.invalidate(table);
  }

  /** Drops every cached result (and with it the credentials held in memory). */
  void invalidateAll() {
    cache.invalidateAll();
    cache.cleanUp();
  }

  private Result load(TableIdentifier table) {
    final Map<String, String> properties;
    try {
      properties = tableStorageProperties.apply(table);
    } catch (RuntimeException e) {
      long now = clockMillis.getAsLong();
      logger.warn(
          "Could not get the storage credentials of table [{}] from Iceberg REST catalog source {};"
              + " its files are accessed with the storage properties of the source for the next {}"
              + " seconds. Cause: {}",
          table,
          sourceName,
          TimeUnit.MILLISECONDS.toSeconds(FAILURE_LIFETIME_MILLIS),
          describe(e));
      return new Result(Optional.empty(), now + FAILURE_LIFETIME_MILLIS);
    }
    long now = clockMillis.getAsLong();
    Optional<VendedStorageCredentials> credentials =
        VendedStorageCredentials.fromTableProperties(properties);
    if (!credentials.isPresent()) {
      logger.debug(
          "Iceberg REST catalog source {} vended no S3 credentials for table [{}]; its files are"
              + " accessed with the storage properties of the source.",
          sourceName,
          table);
      return new Result(Optional.empty(), now + NO_CREDENTIALS_LIFETIME_MILLIS);
    }
    Result result =
        new Result(credentials, validUntilMillis(credentials.get().getExpiresAtMillis(), now));
    logger.debug(
        "Iceberg REST catalog source {} vended {} for table [{}]; refreshed after {}.",
        sourceName,
        credentials.get(),
        table,
        Instant.ofEpochMilli(result.getValidUntilMillis()));
    return result;
  }

  /**
   * Until when vended credentials are used: up to {@link #REFRESH_MARGIN_MILLIS} (at most half of
   * their remaining lifetime) before they expire, but at least {@link #MIN_LIFETIME_MILLIS}.
   */
  @VisibleForTesting
  static long validUntilMillis(@Nullable Long expiresAtMillis, long nowMillis) {
    if (expiresAtMillis == null) {
      return nowMillis + DEFAULT_LIFETIME_MILLIS;
    }
    long remaining = expiresAtMillis - nowMillis;
    long margin = Math.min(REFRESH_MARGIN_MILLIS, remaining / 2);
    return Math.max(nowMillis + MIN_LIFETIME_MILLIS, expiresAtMillis - margin);
  }

  /** The failure, without secrets: messages from the catalog are redacted and abbreviated. */
  private String describe(RuntimeException e) {
    if (e instanceof UserException) {
      // Built by RestCatalogExceptionMapper from a redacted server message.
      return ((UserException) e).getOriginalMessage();
    }
    return e.getClass().getSimpleName()
        + ": "
        + RestCatalogExceptionMapper.redactedServerMessage(e, redactor);
  }
}
