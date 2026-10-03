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

import static com.dremio.common.UserConstants.SYSTEM_ID;
import static com.dremio.exec.catalog.CatalogOptions.RESTCATALOG_VIEWS_SUPPORTED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_FILE_SYSTEM_EXPIRE_AFTER_WRITE_MINUTES;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_FILE_SYSTEM_OPTIMISTIC_LOCKING;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_MUTABLE_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS;
import static com.dremio.plugins.icebergcatalog.store.VendedStorageCredentials.FS_S3A_ACCESS_KEY;
import static com.dremio.plugins.icebergcatalog.store.VendedStorageCredentials.FS_S3A_CREDENTIALS_PROVIDER;
import static com.dremio.plugins.icebergcatalog.store.VendedStorageCredentials.FS_S3A_SECRET_KEY;
import static com.dremio.plugins.icebergcatalog.store.VendedStorageCredentials.FS_S3A_SESSION_TOKEN;
import static com.dremio.plugins.icebergcatalog.store.VendedStorageCredentials.SIMPLE_CREDENTIALS_PROVIDER;
import static com.dremio.plugins.icebergcatalog.store.VendedStorageCredentials.TEMPORARY_CREDENTIALS_PROVIDER;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.plugins.icebergcatalog.dfs.DatasetFileSystemCache;
import com.dremio.service.namespace.NamespaceKey;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Vended storage credentials ({@code isUsingVendedCredentials=true}): the S3 credentials an Iceberg
 * REST catalog returns in the config of its load table (or staged create) response reach the file
 * systems Dremio creates for that table, and nothing else.
 *
 * <p>The catalog is a real Iceberg {@link RESTCatalog} over an in-memory {@link RESTClient} that
 * answers with mocked {@link LoadTableResponse}s shaped like the ones Apache Polaris 1.1 returns
 * for {@code X-Iceberg-Access-Delegation: vended-credentials}. Every test also checks that no
 * credential value reaches a log event of the {@code com.dremio.plugins.icebergcatalog} and {@code
 * org.apache.iceberg} loggers (captured at DEBUG); dedicated tests check {@code toString()} and
 * error messages.
 */
public class TestRestCatalogVendedCredentials {

  // Dummy, test-only values. They are used to assert that secrets never leak.
  private static final String VENDED_ACCESS_KEY = "test-vended-access-key-0001";
  private static final String VENDED_SECRET_KEY = "test-vended-secret-key-0002";
  private static final String VENDED_SESSION_TOKEN = "test-vended-session-token-0003";
  private static final String STATIC_ACCESS_KEY = "test-static-access-key-0004";
  private static final String STATIC_SECRET_KEY = "test-static-secret-key-0005";
  private static final String CLIENT_SECRET = "test-client-secret-0006";
  private static final List<String> SECRETS =
      Arrays.asList(
          VENDED_ACCESS_KEY,
          VENDED_SECRET_KEY,
          VENDED_SESSION_TOKEN,
          STATIC_ACCESS_KEY,
          STATIC_SECRET_KEY,
          CLIENT_SECRET);

  private static final String SOURCE = "vsrc";
  private static final String DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
  private static final TableIdentifier TABLE = TableIdentifier.of("ns1", "t1");
  private static final String TABLE_PATH = "v1/namespaces/ns1/tables/t1";
  private static final String TABLES_PATH = "v1/namespaces/ns1/tables";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

  private final List<ListAppender<ILoggingEvent>> appenders = new ArrayList<>();
  private final Map<Logger, Level> previousLevels = new HashMap<>();
  private final List<RestIcebergCatalogPlugin> plugins = new ArrayList<>();

  @Before
  public void captureLogs() {
    for (String name : Arrays.asList("com.dremio.plugins.icebergcatalog", "org.apache.iceberg")) {
      Logger logger = (Logger) LoggerFactory.getLogger(name);
      previousLevels.put(logger, logger.getLevel());
      logger.setLevel(Level.DEBUG);
      ListAppender<ILoggingEvent> appender = new ListAppender<>();
      appender.start();
      logger.addAppender(appender);
      appenders.add(appender);
    }
  }

  @After
  public void checkLogs() throws Exception {
    try {
      for (RestIcebergCatalogPlugin plugin : plugins) {
        plugin.close();
      }
    } finally {
      for (Map.Entry<Logger, Level> e : previousLevels.entrySet()) {
        for (ListAppender<ILoggingEvent> appender : appenders) {
          e.getKey().detachAppender(appender);
        }
        e.getKey().setLevel(e.getValue());
      }
    }
    for (ListAppender<ILoggingEvent> appender : appenders) {
      for (ILoggingEvent event : events(appender)) {
        assertNoSecretIn("log event", eventText(event));
      }
    }
  }

  /**
   * A snapshot of the captured events. File systems are closed on a background thread (cache
   * removal listener), which may log while a test reads the events; appending holds the appender's
   * lock.
   */
  private static List<ILoggingEvent> events(ListAppender<ILoggingEvent> appender) {
    synchronized (appender) {
      return new ArrayList<>(appender.list);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Mapping of the vended config onto the S3A properties of Dremio's S3 file system
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testSessionCredentialsUseTheTemporaryCredentialsProvider() {
    long expiresAt = 1_791_020_956_000L;
    VendedStorageCredentials credentials =
        VendedStorageCredentials.fromTableProperties(polarisConfig(expiresAt)).get();
    Configuration conf = staticSourceConf();

    credentials.applyTo(conf);

    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(VENDED_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_SECRET_KEY)).isEqualTo(VENDED_SECRET_KEY);
    assertThat(conf.get(FS_S3A_SESSION_TOKEN)).isEqualTo(VENDED_SESSION_TOKEN);
    assertThat(conf.get(FS_S3A_CREDENTIALS_PROVIDER)).isEqualTo(TEMPORARY_CREDENTIALS_PROVIDER);
    assertThat(credentials.isSessionCredential()).isTrue();
    assertThat(credentials.getExpiresAtMillis()).isEqualTo(expiresAt);
  }

  @Test
  public void testKeysWithoutSessionTokenUseTheSimpleCredentialsProvider() {
    Map<String, String> config = new HashMap<>(polarisConfig(1_000L));
    config.remove(VendedStorageCredentials.S3_SESSION_TOKEN);
    config.remove(VendedStorageCredentials.S3_SESSION_TOKEN_EXPIRES_AT_MS);
    config.remove(VendedStorageCredentials.EXPIRATION_TIME);
    Configuration conf = staticSourceConf();
    conf.set(FS_S3A_SESSION_TOKEN, "a-stale-session-token");

    VendedStorageCredentials credentials =
        VendedStorageCredentials.fromTableProperties(config).get();
    credentials.applyTo(conf);

    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(VENDED_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_SECRET_KEY)).isEqualTo(VENDED_SECRET_KEY);
    assertThat(conf.get(FS_S3A_SESSION_TOKEN)).isNull();
    assertThat(conf.get(FS_S3A_CREDENTIALS_PROVIDER)).isEqualTo(SIMPLE_CREDENTIALS_PROVIDER);
    assertThat(credentials.getExpiresAtMillis()).isNull();
  }

  /**
   * Per-bucket credential settings would be copied over the vended keys (FileSystemConfUtil, Hadoop
   * S3A) and mix the source's keys with the vended session token: they are removed. Other
   * per-bucket settings stay.
   */
  @Test
  public void testVendedCredentialsReplacePerBucketCredentials() {
    Configuration conf = staticSourceConf();
    conf.set("fs.s3a.bucket.bucket.access.key", STATIC_ACCESS_KEY);
    conf.set("fs.s3a.bucket.bucket.secret.key", STATIC_SECRET_KEY);
    conf.set("fs.s3a.bucket.bucket.session.token", "a-stale-session-token");
    conf.set(
        "fs.s3a.bucket.my.dotted.bucket.aws.credentials.provider", SIMPLE_CREDENTIALS_PROVIDER);
    conf.set("fs.s3a.bucket.bucket.endpoint", "127.0.0.1:9001");

    VendedStorageCredentials.fromTableProperties(polarisConfig(1_000L)).get().applyTo(conf);

    assertThat(conf.get("fs.s3a.bucket.bucket.access.key")).isNull();
    assertThat(conf.get("fs.s3a.bucket.bucket.secret.key")).isNull();
    assertThat(conf.get("fs.s3a.bucket.bucket.session.token")).isNull();
    assertThat(conf.get("fs.s3a.bucket.my.dotted.bucket.aws.credentials.provider")).isNull();
    assertThat(conf.get("fs.s3a.bucket.bucket.endpoint")).isEqualTo("127.0.0.1:9001");
    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(VENDED_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_CREDENTIALS_PROVIDER)).isEqualTo(TEMPORARY_CREDENTIALS_PROVIDER);
  }

  /**
   * Vended credentials are scoped to the table's location: they normally may not list all buckets
   * (s3:ListAllMyBuckets) or the bucket root, which bucket discovery does when the S3 file system
   * starts. It is turned off, also when the source left it on.
   */
  @Test
  public void testVendedCredentialsTurnOffBucketDiscovery() {
    Configuration conf = staticSourceConf();
    conf.unset(VendedStorageCredentials.DREMIO_BUCKET_DISCOVERY);

    VendedStorageCredentials.fromTableProperties(polarisConfig(1_000L)).get().applyTo(conf);

    assertThat(conf.get(VendedStorageCredentials.DREMIO_BUCKET_DISCOVERY)).isEqualTo("false");
  }

  @Test
  public void testIncompleteCredentialsAreIgnored() {
    assertThat(VendedStorageCredentials.fromTableProperties(null)).isEmpty();
    assertThat(VendedStorageCredentials.fromTableProperties(Collections.emptyMap())).isEmpty();
    Map<String, String> noSecret = new HashMap<>(polarisConfig(1_000L));
    noSecret.remove(VendedStorageCredentials.S3_SECRET_ACCESS_KEY);
    assertThat(VendedStorageCredentials.fromTableProperties(noSecret)).isEmpty();
    Map<String, String> blankKey = new HashMap<>(polarisConfig(1_000L));
    blankKey.put(VendedStorageCredentials.S3_ACCESS_KEY_ID, " ");
    assertThat(VendedStorageCredentials.fromTableProperties(blankKey)).isEmpty();
  }

  @Test
  public void testExpiryIsTheEarliestValidExpiryTime() {
    Map<String, String> config = new HashMap<>(polarisConfig(5_000L));
    config.put(VendedStorageCredentials.EXPIRATION_TIME, "4000");
    assertThat(VendedStorageCredentials.fromTableProperties(config).get().getExpiresAtMillis())
        .isEqualTo(4_000L);

    config.put(VendedStorageCredentials.EXPIRATION_TIME, "not-a-number");
    assertThat(VendedStorageCredentials.fromTableProperties(config).get().getExpiresAtMillis())
        .isEqualTo(5_000L);

    config.remove(VendedStorageCredentials.S3_SESSION_TOKEN_EXPIRES_AT_MS);
    config.put(VendedStorageCredentials.EXPIRATION_TIME, "6000");
    assertThat(VendedStorageCredentials.fromTableProperties(config).get().getExpiresAtMillis())
        .isEqualTo(6_000L);
  }

  @Test
  public void testOnlyCredentialsAreMappedConnectionSettingsStayWithTheSource() {
    Configuration conf = staticSourceConf();
    Configuration before = new Configuration(conf);

    VendedStorageCredentials.fromTableProperties(polarisConfig(1_000L)).get().applyTo(conf);

    // Polaris also returns s3.endpoint (with scheme), s3.path-style-access and client.region:
    // how Dremio reaches the storage stays as configured on the source.
    for (String key :
        Arrays.asList(
            "fs.s3a.endpoint",
            "fs.s3a.path.style.access",
            "fs.s3a.connection.ssl.enabled",
            "dremio.s3.compat",
            "dremio.s3.region")) {
      assertThat(conf.get(key)).as(key).isEqualTo(before.get(key));
    }
    for (Map.Entry<String, String> entry : conf) {
      assertThat(entry.getKey()).doesNotStartWith("s3.").doesNotStartWith("client.");
    }
  }

  @Test
  public void testToStringNeverShowsCredentials() {
    VendedStorageCredentials credentials =
        VendedStorageCredentials.fromTableProperties(polarisConfig(1_791_020_956_000L)).get();
    VendedCredentialsCache.Result result =
        new VendedCredentialsCache.Result(java.util.Optional.of(credentials), 42L);

    assertNoSecretIn("credentials", credentials.toString());
    assertNoSecretIn("result", result.toString());
    assertThat(credentials.toString())
        .isEqualTo("temporary S3 credentials expiring at 2026-10-03T09:49:16Z");
  }

  // ---------------------------------------------------------------------------------------------
  // Caching and refresh
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testValidityEndsShortlyBeforeTheCredentialsExpire() {
    long now = 1_000_000L;
    long hour = TimeUnit.HOURS.toMillis(1);
    // Refreshed 5 minutes before they expire...
    assertThat(VendedCredentialsCache.validUntilMillis(now + hour, now))
        .isEqualTo(now + hour - VendedCredentialsCache.REFRESH_MARGIN_MILLIS);
    // ...or after half of their remaining lifetime when that is shorter...
    long fourMinutes = TimeUnit.MINUTES.toMillis(4);
    assertThat(VendedCredentialsCache.validUntilMillis(now + fourMinutes, now))
        .isEqualTo(now + fourMinutes / 2);
    // ...but never immediately (expired credentials are not requested in a loop).
    assertThat(VendedCredentialsCache.validUntilMillis(now - 1, now))
        .isEqualTo(now + VendedCredentialsCache.MIN_LIFETIME_MILLIS);
    // Without an expiry time: the default lifetime.
    assertThat(VendedCredentialsCache.validUntilMillis(null, now))
        .isEqualTo(now + VendedCredentialsCache.DEFAULT_LIFETIME_MILLIS);
  }

  @Test
  public void testCredentialsAreCachedUntilTheyAreRefreshed() {
    FakeClock clock = new FakeClock();
    long expiresAt = clock.millis() + TimeUnit.HOURS.toMillis(1);
    AtomicInteger loads = new AtomicInteger();
    VendedCredentialsCache cache =
        clock.newCache(
            table -> {
              loads.incrementAndGet();
              return polarisConfig(expiresAt);
            });

    VendedCredentialsCache.Result first = cache.get(TABLE);
    assertThat(first.getCredentials()).isPresent();
    assertThat(first.getValidUntilMillis())
        .isEqualTo(expiresAt - VendedCredentialsCache.REFRESH_MARGIN_MILLIS);
    assertThat(cache.get(TABLE)).isSameAs(first);
    assertThat(loads).hasValue(1);

    clock.advance(TimeUnit.MINUTES.toMillis(54));
    assertThat(cache.get(TABLE)).isSameAs(first);
    assertThat(loads).hasValue(1);

    clock.advance(TimeUnit.MINUTES.toMillis(2));
    assertThat(cache.get(TABLE)).isNotSameAs(first);
    assertThat(loads).hasValue(2);

    cache.invalidateAll();
    cache.get(TABLE);
    assertThat(loads).hasValue(3);
  }

  @Test
  public void testTablesWithoutVendedCredentialsAreCachedAsSuch() {
    FakeClock clock = new FakeClock();
    AtomicInteger loads = new AtomicInteger();
    VendedCredentialsCache cache =
        clock.newCache(
            table -> {
              loads.incrementAndGet();
              return ImmutableMap.of("s3.endpoint", "http://127.0.0.1:9000");
            });

    VendedCredentialsCache.Result result = cache.get(TABLE);

    assertThat(result.getCredentials()).isEmpty();
    assertThat(result.getValidUntilMillis())
        .isEqualTo(clock.millis() + VendedCredentialsCache.NO_CREDENTIALS_LIFETIME_MILLIS);
    cache.get(TABLE);
    assertThat(loads).hasValue(1);
  }

  @Test
  public void testFailuresFallBackToTheSourceAndAreRetriedSoon() {
    FakeClock clock = new FakeClock();
    AtomicInteger loads = new AtomicInteger();
    VendedCredentialsCache cache =
        clock.newCache(
            table -> {
              loads.incrementAndGet();
              // A catalog that echoes a secret in its error message.
              throw new ForbiddenException("Forbidden: denied for %s", CLIENT_SECRET);
            });

    VendedCredentialsCache.Result result = cache.get(TABLE);

    assertThat(result.getCredentials()).isEmpty();
    assertThat(result.getValidUntilMillis())
        .isEqualTo(clock.millis() + VendedCredentialsCache.FAILURE_LIFETIME_MILLIS);
    assertThat(warnings())
        .anySatisfy(
            w ->
                assertThat(w)
                    .contains("Could not get the storage credentials of table [ns1.t1]")
                    .contains("source " + SOURCE)
                    .contains("ForbiddenException: Forbidden: denied for ****"));
    cache.get(TABLE);
    assertThat(loads).hasValue(1);
    clock.advance(VendedCredentialsCache.FAILURE_LIFETIME_MILLIS + 1);
    cache.get(TABLE);
    assertThat(loads).hasValue(2);
  }

  /**
   * Entries expire on the monotonic ticker, validity is wall clock time. After the wall clock
   * stepped forward (NTP, resumed VM), a cached result past its validity is looked up again instead
   * of being returned with a validity in the past.
   */
  @Test
  public void testResultsPastTheirValidityAreReplacedAfterAWallClockStep() {
    AtomicLong ticker = new AtomicLong();
    AtomicLong wallClock = new AtomicLong(1_700_000_000_000L);
    AtomicInteger loads = new AtomicInteger();
    VendedCredentialsCache cache =
        new VendedCredentialsCache(
            table -> {
              loads.incrementAndGet();
              return polarisConfig(wallClock.get() + TimeUnit.HOURS.toMillis(1));
            },
            SOURCE,
            text -> text,
            ticker::get,
            wallClock::get);

    VendedCredentialsCache.Result first = cache.get(TABLE);
    assertThat(cache.get(TABLE)).isSameAs(first);

    // Two hours on the wall clock, a second on the monotonic ticker.
    wallClock.addAndGet(TimeUnit.HOURS.toMillis(2));
    ticker.addAndGet(TimeUnit.SECONDS.toNanos(1));
    VendedCredentialsCache.Result second = cache.get(TABLE);

    assertThat(second).isNotSameAs(first);
    assertThat(second.getValidUntilMillis()).isGreaterThan(wallClock.get());
    assertThat(loads).hasValue(2);
    assertThat(cache.get(TABLE)).isSameAs(second);
  }

  @Test
  public void testInvalidatedTablesAreLookedUpAgain() {
    FakeClock clock = new FakeClock();
    AtomicInteger loads = new AtomicInteger();
    VendedCredentialsCache cache =
        clock.newCache(
            table -> {
              loads.incrementAndGet();
              return polarisConfig(clock.millis() + TimeUnit.HOURS.toMillis(1));
            });
    TableIdentifier other = TableIdentifier.of("ns1", "t2");

    cache.get(TABLE);
    cache.get(other);
    cache.invalidate(TABLE);
    cache.get(TABLE);
    cache.get(other);

    assertThat(loads).hasValue(3);
  }

  // ---------------------------------------------------------------------------------------------
  // Accessor: what the Iceberg REST client makes of the catalog's response
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testLoadTableResponseConfigReachesTheTableStorageProperties() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableConfig = polarisConfig(1_791_020_956_000L);
    try (RESTCatalog catalog = client.newCatalog(true)) {
      AbstractRestCatalogAccessor accessor = newAccessor(catalog);

      Map<String, String> properties = accessor.loadTableStorageProperties(TABLE, true);

      assertThat(VendedStorageCredentials.fromTableProperties(properties))
          .hasValueSatisfying(c -> assertThat(c.isSessionCredential()).isTrue());
      assertThat(client.requests).containsExactly("GET v1/config", "GET " + TABLE_PATH);
      // The source asked for vended credentials.
      assertThat(client.headersOf("GET " + TABLE_PATH))
          .containsEntry(DELEGATION_HEADER, "vended-credentials");
    }
  }

  @Test
  public void testNewTableGetsTheCredentialsOfAStagedCreation() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableExists = false;
    client.tableConfig = polarisConfig(1_791_020_956_000L);
    try (RESTCatalog catalog = client.newCatalog(true)) {
      AbstractRestCatalogAccessor accessor = newAccessor(catalog);

      Map<String, String> properties = accessor.loadTableStorageProperties(TABLE, true);

      assertThat(VendedStorageCredentials.fromTableProperties(properties)).isPresent();
      assertThat(client.requests)
          .containsExactly("GET v1/config", "GET " + TABLE_PATH, "POST " + TABLES_PATH);
      // Staged only: nothing is committed, so the catalog does not create the table.
      assertThat(client.createRequests).hasSize(1);
      assertThat(client.createRequests.get(0).stageCreate()).isTrue();
      assertThat(client.createRequests.get(0).name()).isEqualTo("t1");
      assertThat(client.createRequests.get(0).location()).isNull();
    }
  }

  @Test
  public void testNewTableIsNotStagedWhenNotAsked() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableExists = false;
    client.tableConfig = polarisConfig(1_791_020_956_000L);
    try (RESTCatalog catalog = client.newCatalog(true)) {
      assertThat(newAccessor(catalog).loadTableStorageProperties(TABLE, false)).isEmpty();
      assertThat(client.requests).containsExactly("GET v1/config", "GET " + TABLE_PATH);
      assertThat(client.createRequests).isEmpty();
    }
  }

  @Test
  public void testNewTableInMissingNamespaceHasNoCredentials() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableExists = false;
    client.namespaceExists = false;
    try (RESTCatalog catalog = client.newCatalog(true)) {
      assertThat(newAccessor(catalog).loadTableStorageProperties(TABLE, true)).isEmpty();
    }
  }

  @Test
  public void testForbiddenIsAPermissionErrorWithoutSecrets() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.forbiddenMessage =
        "Principal is not authorized for op LOAD_TABLE_WITH_READ_DELEGATION, key " + CLIENT_SECRET;
    try (RESTCatalog catalog = client.newCatalog(true)) {
      AbstractRestCatalogAccessor accessor = newAccessor(catalog);

      assertThatThrownBy(() -> accessor.loadTableStorageProperties(TABLE, true))
          .isInstanceOf(UserException.class)
          .satisfies(
              t -> {
                UserException e = (UserException) t;
                assertThat(e.getErrorType()).isEqualTo(ErrorType.PERMISSION);
                assertThat(e.getOriginalMessage())
                    .contains("load the storage credentials of table [ns1.t1]")
                    .contains("LOAD_TABLE_WITH_READ_DELEGATION");
                assertNoSecretIn("error", e.getOriginalMessage());
                assertNoSecretIn("error", e.toString());
              });
    }
  }

  @Test
  public void testCatalogWithoutVendedCredentialsYieldsNone() throws Exception {
    FakeRestClient client = new FakeRestClient();
    try (RESTCatalog catalog = client.newCatalog(false)) {
      Map<String, String> properties = newAccessor(catalog).loadTableStorageProperties(TABLE, true);

      assertThat(VendedStorageCredentials.fromTableProperties(properties)).isEmpty();
      assertThat(client.headersOf("GET " + TABLE_PATH)).doesNotContainKey(DELEGATION_HEADER);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Plugin: per-table file system configuration
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testFileSystemOfATableUsesItsVendedCredentials() throws Exception {
    FakeRestClient client = new FakeRestClient();
    long expiresAt = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
    client.tableConfig = polarisConfig(expiresAt);
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(true, true), client);

    Configuration conf = plugin.getFsConfForDataset(Arrays.asList(SOURCE, "ns1", "t1"));

    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(VENDED_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_SECRET_KEY)).isEqualTo(VENDED_SECRET_KEY);
    assertThat(conf.get(FS_S3A_SESSION_TOKEN)).isEqualTo(VENDED_SESSION_TOKEN);
    assertThat(conf.get(FS_S3A_CREDENTIALS_PROVIDER)).isEqualTo(TEMPORARY_CREDENTIALS_PROVIDER);
    // The file system is replaced 5 minutes before the credentials expire.
    assertThat(conf.getLong(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS, 0))
        .isEqualTo(expiresAt - VendedCredentialsCache.REFRESH_MARGIN_MILLIS);
    // Connection settings stay with the source.
    assertThat(conf.get("fs.s3a.endpoint")).isEqualTo("127.0.0.1:9000");

    // Writers (e.g. CTAS) pass the dataset without the source name: same table, same credentials.
    Configuration writerConf = plugin.getFsConfForDataset(Arrays.asList(null, "ns1", "t1"));
    assertThat(writerConf.get(FS_S3A_ACCESS_KEY)).isEqualTo(VENDED_ACCESS_KEY);
    assertThat(client.count("GET " + TABLE_PATH)).as("table loads").isEqualTo(1);

    // The source's shared configuration never carries vended credentials.
    Configuration shared = plugin.getFsConfCopy();
    assertThat(shared.get(FS_S3A_ACCESS_KEY)).isEqualTo(STATIC_ACCESS_KEY);
    assertThat(shared.get(FS_S3A_SECRET_KEY)).isEqualTo(STATIC_SECRET_KEY);
    assertThat(shared.get(FS_S3A_SESSION_TOKEN)).isNull();
    assertThat(shared.get(FS_S3A_CREDENTIALS_PROVIDER)).isEqualTo(SIMPLE_CREDENTIALS_PROVIDER);
    assertThat(shared.get(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS)).isNull();
  }

  @Test
  public void testFileSystemWithoutTableOrVendedCredentialsUsesTheSourceSettings()
      throws Exception {
    FakeRestClient client = new FakeRestClient();
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(true, true), client);

    // No dataset (e.g. a file system for a plain path): the source's settings, no catalog call.
    Configuration noDataset = plugin.getFsConfForDataset(null);
    assertThat(noDataset.get(FS_S3A_ACCESS_KEY)).isEqualTo(STATIC_ACCESS_KEY);
    assertThat(noDataset.get(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS)).isNull();
    assertThat(client.requests).isEmpty();

    // The catalog vends nothing for the table: the source's settings, looked up again later.
    long before = System.currentTimeMillis();
    Configuration conf = plugin.getFsConfForDataset(Arrays.asList(SOURCE, "ns1", "t1"));
    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(STATIC_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_CREDENTIALS_PROVIDER)).isEqualTo(SIMPLE_CREDENTIALS_PROVIDER);
    assertThat(conf.getLong(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS, 0))
        .isBetween(
            before + VendedCredentialsCache.NO_CREDENTIALS_LIFETIME_MILLIS,
            System.currentTimeMillis() + VendedCredentialsCache.NO_CREDENTIALS_LIFETIME_MILLIS);
  }

  /**
   * A table that does not exist yet (CTAS writes its data files before the table is created): a
   * source with static keys writes it with them, as it does without vended credentials, also
   * outside the catalog's default table location. No staged creation is requested.
   */
  @Test
  public void testNewTableOfASourceWithStaticKeysUsesTheStaticKeys() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableExists = false;
    client.tableConfig = polarisConfig(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(true, true), client);

    Configuration conf = plugin.getFsConfForDataset(Arrays.asList(null, "ns1", "t1"));

    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(STATIC_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_SECRET_KEY)).isEqualTo(STATIC_SECRET_KEY);
    assertThat(conf.get(FS_S3A_SESSION_TOKEN)).isNull();
    assertThat(client.createRequests).isEmpty();
    assertThat(client.count("GET " + TABLE_PATH)).isEqualTo(1);
  }

  /** Without static keys, a new table gets the credentials of a staged creation. */
  @Test
  public void testNewTableOfASourceWithoutStaticKeysUsesAStagedCreation() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableExists = false;
    client.tableConfig = polarisConfig(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(true, false), client);

    Configuration conf = plugin.getFsConfForDataset(Arrays.asList(null, "ns1", "t1"));

    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(VENDED_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_SESSION_TOKEN)).isEqualTo(VENDED_SESSION_TOKEN);
    assertThat(client.createRequests).hasSize(1);
    assertThat(client.createRequests.get(0).stageCreate()).isTrue();
  }

  /**
   * The Iceberg REST client merges the catalog properties, i.e. every source property, into the
   * table's FileIO properties. {@code s3.*} keys configured on the source are not vended
   * credentials: a table the catalog vends nothing for uses the source's {@code fs.s3a.*} keys.
   */
  @Test
  public void testSourceS3PropertiesAreNotTakenForVendedCredentials() throws Exception {
    FakeRestClient client = new FakeRestClient();
    RestIcebergCatalogPluginConfig config = newConfig(true, true);
    config.secretPropertyList.add(
        new Property(VendedStorageCredentials.S3_ACCESS_KEY_ID, VENDED_ACCESS_KEY));
    config.secretPropertyList.add(
        new Property(VendedStorageCredentials.S3_SECRET_ACCESS_KEY, VENDED_SECRET_KEY));
    config.secretPropertyList.add(
        new Property(VendedStorageCredentials.S3_SESSION_TOKEN, VENDED_SESSION_TOKEN));
    RestIcebergCatalogPlugin plugin = startPlugin(config, client);

    Configuration conf = plugin.getFsConfForDataset(Arrays.asList(SOURCE, "ns1", "t1"));

    assertThat(client.count("GET " + TABLE_PATH)).isEqualTo(1);
    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(STATIC_ACCESS_KEY);
    assertThat(conf.get(FS_S3A_SECRET_KEY)).isEqualTo(STATIC_SECRET_KEY);
    assertThat(conf.get(FS_S3A_SESSION_TOKEN)).isNull();
    assertThat(conf.get(FS_S3A_CREDENTIALS_PROVIDER)).isEqualTo(SIMPLE_CREDENTIALS_PROVIDER);
  }

  @Test
  public void testWithoutSourcePropertiesKeepsOnlyWhatTheCatalogChanged() {
    Map<String, String> io = new HashMap<>();
    io.put("warehouse", "wh");
    io.put(VendedStorageCredentials.S3_ACCESS_KEY_ID, "from-source");
    io.put(VendedStorageCredentials.S3_SECRET_ACCESS_KEY, "from-catalog");

    Map<String, String> result =
        RestIcebergCatalogPlugin.withoutSourceProperties(
            io,
            Arrays.asList(
                new Property("warehouse", "wh"),
                new Property(VendedStorageCredentials.S3_ACCESS_KEY_ID, "from-source"),
                new Property(VendedStorageCredentials.S3_SECRET_ACCESS_KEY, "configured")));

    assertThat(result)
        .containsExactly(
            org.assertj.core.api.Assertions.entry(
                VendedStorageCredentials.S3_SECRET_ACCESS_KEY, "from-catalog"));
    assertThat(io).hasSize(3);
  }

  /**
   * After a table is dropped (or created), its cached credentials and the file systems created with
   * them are dropped on this node: a table created again under the same name may live elsewhere and
   * get other credentials.
   */
  @Test
  public void testDroppingATableForgetsItsCredentialsAndFileSystems() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableConfig = polarisConfig(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(true, false), client);
    DatasetFileSystemCache fsCache = plugin.getHadoopFileSystemCache();
    File dir = Files.createTempDirectory("vended").toFile();
    String path = dir.toURI().toString();
    try {
      FileSystem t1 = fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t1"), null, false);
      FileSystem t2 = fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t2"), null, false);
      LocalFileSystem t1Before = t1.unwrap(LocalFileSystem.class);
      LocalFileSystem t2Before = t2.unwrap(LocalFileSystem.class);
      assertThat(client.count("GET " + TABLE_PATH)).isEqualTo(1);

      plugin.dropTable(new NamespaceKey(dataset("t1")), null, null);

      assertThat(client.requests).contains("DELETE " + TABLE_PATH);
      assertThat(t1.unwrap(LocalFileSystem.class)).isNotSameAs(t1Before);
      assertThat(client.count("GET " + TABLE_PATH)).isEqualTo(2);
      assertThat(t2.unwrap(LocalFileSystem.class)).isSameAs(t2Before);
    } finally {
      Files.deleteIfExists(dir.toPath());
    }
  }

  @Test
  public void testSourceWithoutVendedCredentialsNeverAsksForThem() throws Exception {
    FakeRestClient client = new FakeRestClient();
    client.tableConfig = polarisConfig(System.currentTimeMillis() + 3_600_000L);
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(false, true), client);

    Configuration conf = plugin.getFsConfForDataset(Arrays.asList(SOURCE, "ns1", "t1"));

    assertThat(conf.get(FS_S3A_ACCESS_KEY)).isEqualTo(STATIC_ACCESS_KEY);
    assertThat(conf.get(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS)).isNull();
    assertThat(client.requests).isEmpty();
  }

  @Test
  public void testTableIdentifierOfDataset() {
    assertThat(RestIcebergCatalogPlugin.tableIdentifierOf(Arrays.asList(SOURCE, "a", "b", "t")))
        .isEqualTo(TableIdentifier.of("a", "b", "t"));
    assertThat(RestIcebergCatalogPlugin.tableIdentifierOf(Arrays.asList(null, "a", "t")))
        .isEqualTo(TableIdentifier.of("a", "t"));
    assertThat(RestIcebergCatalogPlugin.tableIdentifierOf(null)).isNull();
    assertThat(RestIcebergCatalogPlugin.tableIdentifierOf(Arrays.asList(SOURCE, "t"))).isNull();
    assertThat(RestIcebergCatalogPlugin.tableIdentifierOf(Arrays.asList(SOURCE, null, "t")))
        .isNull();
  }

  // ---------------------------------------------------------------------------------------------
  // File system cache: per dataset, replaced when the configuration says so
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testFileSystemsArePerDatasetAndReplacedWhenTheirCredentialsExpire() throws Exception {
    FakeClock clock = new FakeClock();
    AtomicLong expiresAt = new AtomicLong(Long.MAX_VALUE);
    List<Configuration> provided = new CopyOnWriteArrayList<>();
    DatasetFileSystemCache fsCache =
        clock.newFileSystemCache(
            dataset -> {
              Configuration conf = new Configuration(false);
              if (expiresAt.get() != Long.MAX_VALUE) {
                conf.setLong(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS, expiresAt.get());
              }
              provided.add(conf);
              return conf;
            });
    File dir = Files.createTempDirectory("vended").toFile();
    String path = dir.toURI().toString();
    try {
      FileSystem t1 = fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t1"), null, false);
      FileSystem t1Again =
          fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t1"), null, false);
      FileSystem t2 = fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t2"), null, false);

      LocalFileSystem first = t1.unwrap(LocalFileSystem.class);
      assertThat(t1Again.unwrap(LocalFileSystem.class)).isSameAs(first);
      assertThat(t2.unwrap(LocalFileSystem.class)).isNotSameAs(first);

      // Credentials that expire in 20 seconds: the first use after that gets a new file system.
      expiresAt.set(clock.millis() + TimeUnit.SECONDS.toMillis(20));
      FileSystem t3 = fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t3"), null, false);
      LocalFileSystem expiring = t3.unwrap(LocalFileSystem.class);
      clock.advance(TimeUnit.SECONDS.toMillis(19));
      assertThat(t3.unwrap(LocalFileSystem.class)).isSameAs(expiring);
      clock.advance(TimeUnit.SECONDS.toMillis(2));
      expiresAt.set(Long.MAX_VALUE);
      assertThat(t3.unwrap(LocalFileSystem.class)).isNotSameAs(expiring);
      // Unaffected: the default lifetime (1 minute here).
      assertThat(t1.unwrap(LocalFileSystem.class)).isSameAs(first);
      clock.advance(TimeUnit.MINUTES.toMillis(1));
      assertThat(t1.unwrap(LocalFileSystem.class)).isNotSameAs(first);

      // The marker is consumed by the cache, not passed to the file system.
      for (Configuration conf : provided) {
        assertThat(conf.get(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS)).isNull();
      }
    } finally {
      fsCache.close();
      Files.deleteIfExists(dir.toPath());
    }
  }

  /**
   * An expiry that has already passed (e.g. after a wall clock step) still gives the file system a
   * short lifetime: with none, every lookup would create a new one and no user of the cache could
   * lock the one it got ("Unable to acquire lock on freshly produced FS instance").
   */
  @Test
  public void testFileSystemsWhoseExpiryHasPassedAreStillUsable() throws Exception {
    FakeClock clock = new FakeClock();
    AtomicInteger created = new AtomicInteger();
    DatasetFileSystemCache fsCache =
        clock.newFileSystemCache(
            dataset -> {
              created.incrementAndGet();
              Configuration conf = new Configuration(false);
              conf.setLong(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS, clock.millis() - 1_000L);
              return conf;
            });
    File dir = Files.createTempDirectory("vended").toFile();
    try {
      FileSystem t1 =
          fsCache.load(
              dir.toURI().toString(), SYSTEM_USERNAME, SYSTEM_ID, dataset("t1"), null, false);
      LocalFileSystem fs = t1.unwrap(LocalFileSystem.class);
      assertThat(t1.unwrap(LocalFileSystem.class)).isSameAs(fs);
      assertThat(created).hasValue(1);
      clock.advance(
          TimeUnit.NANOSECONDS.toMillis(DatasetFileSystemCache.MIN_EXPIRING_LIFETIME_NANOS));
      assertThat(t1.unwrap(LocalFileSystem.class)).isNotSameAs(fs);
    } finally {
      fsCache.close();
      Files.deleteIfExists(dir.toPath());
    }
  }

  @Test
  public void testFileSystemsAreSharedWithoutPerDatasetCaching() throws Exception {
    DatasetFileSystemCache fsCache =
        new DatasetFileSystemCache(dataset -> new Configuration(false), fsCacheOptions());
    File dir = Files.createTempDirectory("vended").toFile();
    String path = dir.toURI().toString();
    try {
      FileSystem t1 = fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t1"), null, false);
      FileSystem t2 = fsCache.load(path, SYSTEM_USERNAME, SYSTEM_ID, dataset("t2"), null, false);

      assertThat(t2.unwrap(LocalFileSystem.class)).isSameAs(t1.unwrap(LocalFileSystem.class));
    } finally {
      fsCache.close();
      Files.deleteIfExists(dir.toPath());
    }
  }

  @Test
  public void testClosingAnUnusedFileSystemCacheIsANoOp() throws Exception {
    new DatasetFileSystemCache(dataset -> new Configuration(false), fsCacheOptions()).close();
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** The config Apache Polaris 1.1 returns with vended credentials for an S3 (MinIO) catalog. */
  private static Map<String, String> polarisConfig(long expiresAtMillis) {
    Map<String, String> config = new HashMap<>();
    config.put("s3.path-style-access", "true");
    config.put(VendedStorageCredentials.S3_ACCESS_KEY_ID, VENDED_ACCESS_KEY);
    config.put(VendedStorageCredentials.S3_SECRET_ACCESS_KEY, VENDED_SECRET_KEY);
    config.put(VendedStorageCredentials.S3_SESSION_TOKEN, VENDED_SESSION_TOKEN);
    config.put("client.refresh-credentials-endpoint", "v1/wh/namespaces/ns1/tables/t1/credentials");
    config.put(VendedStorageCredentials.EXPIRATION_TIME, Long.toString(expiresAtMillis));
    config.put("s3.endpoint", "http://127.0.0.1:9000");
    config.put(
        VendedStorageCredentials.S3_SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(expiresAtMillis));
    config.put("client.region", "us-east-1");
    return config;
  }

  /** Storage properties of a MinIO source with static keys (the Phase 4 baseline). */
  private static Configuration staticSourceConf() {
    Configuration conf = new Configuration(false);
    conf.set(FS_S3A_CREDENTIALS_PROVIDER, SIMPLE_CREDENTIALS_PROVIDER);
    conf.set(FS_S3A_ACCESS_KEY, STATIC_ACCESS_KEY);
    conf.set(FS_S3A_SECRET_KEY, STATIC_SECRET_KEY);
    conf.set("fs.s3a.endpoint", "127.0.0.1:9000");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    conf.set("dremio.s3.compat", "true");
    conf.set("dremio.s3.region", "us-east-1");
    return conf;
  }

  private static RestIcebergCatalogPluginConfig newConfig(boolean vended, boolean staticKeys) {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://catalog.invalid/api/catalog";
    conf.isUsingVendedCredentials = vended;
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "wh"),
                new Property(FS_S3A_CREDENTIALS_PROVIDER, SIMPLE_CREDENTIALS_PROVIDER),
                new Property("fs.s3a.endpoint", "127.0.0.1:9000"),
                new Property("fs.s3a.path.style.access", "true"),
                new Property("dremio.s3.compat", "true")));
    conf.secretPropertyList = new ArrayList<>();
    conf.secretPropertyList.add(new Property("token", CLIENT_SECRET));
    if (staticKeys) {
      conf.secretPropertyList.add(new Property(FS_S3A_ACCESS_KEY, STATIC_ACCESS_KEY));
      conf.secretPropertyList.add(new Property(FS_S3A_SECRET_KEY, STATIC_SECRET_KEY));
    }
    return conf;
  }

  /** Starts a source whose REST catalog talks to the given client instead of the network. */
  private RestIcebergCatalogPlugin startPlugin(
      RestIcebergCatalogPluginConfig conf, FakeRestClient client) throws Exception {
    OptionManager options = fsCacheOptions();
    when(options.getOption(RESTCATALOG_PLUGIN_ENABLED)).thenReturn(true);
    when(options.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)).thenReturn(true);
    when(options.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(false);
    when(options.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS)).thenReturn(3600L);
    when(options.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS)).thenReturn(100L);
    when(options.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS))
        .thenReturn(3L);
    PluginSabotContext context = mock(PluginSabotContext.class);
    when(context.getOptionManager()).thenReturn(options);

    RestIcebergCatalogPlugin plugin =
        new RestIcebergCatalogPlugin(conf, context, SOURCE, () -> mock(StoragePluginId.class)) {
          @Override
          protected Supplier<Catalog> createRestCatalog(Configuration config) {
            return () -> client.newCatalog(buildCatalogProperties(config));
          }
        };
    plugins.add(plugin);
    plugin.start();
    return plugin;
  }

  private static OptionManager fsCacheOptions() {
    OptionManager options = mock(OptionManager.class);
    when(options.getOption(RESTCATALOG_PLUGIN_FILE_SYSTEM_EXPIRE_AFTER_WRITE_MINUTES))
        .thenReturn(1L);
    when(options.getOption(RESTCATALOG_PLUGIN_FILE_SYSTEM_OPTIMISTIC_LOCKING)).thenReturn(false);
    return options;
  }

  private static AbstractRestCatalogAccessor newAccessor(Catalog catalog) {
    OptionManager options = mock(OptionManager.class);
    when(options.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS)).thenReturn(100L);
    when(options.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS))
        .thenReturn(3L);
    when(options.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS)).thenReturn(3600L);
    return new IcebergRestCatalogAccessor(
        () -> catalog,
        options,
        null,
        true,
        text -> text == null ? null : text.replace(CLIENT_SECRET, "****"));
  }

  private static List<String> dataset(String table) {
    return Arrays.asList(SOURCE, "ns1", table);
  }

  private List<String> warnings() {
    List<String> warnings = new ArrayList<>();
    for (ListAppender<ILoggingEvent> appender : appenders) {
      for (ILoggingEvent event : events(appender)) {
        if (event.getLevel() == Level.WARN) {
          warnings.add(event.getFormattedMessage());
        }
      }
    }
    return warnings;
  }

  private static String eventText(ILoggingEvent event) {
    StringBuilder sb = new StringBuilder(String.valueOf(event.getFormattedMessage()));
    for (IThrowableProxy t = event.getThrowableProxy(); t != null; t = t.getCause()) {
      sb.append(" | ").append(t.getClassName()).append(": ").append(t.getMessage());
    }
    return sb.toString();
  }

  private static void assertNoSecretIn(String context, String text) {
    for (String secret : SECRETS) {
      assertThat(text).as(context + " must not contain a credential").doesNotContain(secret);
    }
  }

  /** A ticker and a wall clock that move together. */
  private static final class FakeClock {
    private final AtomicLong millis = new AtomicLong(1_700_000_000_000L);

    long millis() {
      return millis.get();
    }

    void advance(long deltaMillis) {
      millis.addAndGet(deltaMillis);
    }

    Ticker ticker() {
      return () -> TimeUnit.MILLISECONDS.toNanos(millis.get());
    }

    VendedCredentialsCache newCache(Function<TableIdentifier, Map<String, String>> loader) {
      return new VendedCredentialsCache(
          loader,
          SOURCE,
          text -> text == null ? null : text.replace(CLIENT_SECRET, "****"),
          ticker(),
          millis::get);
    }

    /** A file system cache per dataset that expires its entries on this clock. */
    DatasetFileSystemCache newFileSystemCache(Function<List<String>, Configuration> confProvider) {
      return new DatasetFileSystemCache(
          confProvider, fsCacheOptions(), true, ticker(), millis::get);
    }
  }

  /**
   * An Iceberg REST catalog in memory: {@code GET v1/config}, load, (staged) create and drop of a
   * table in {@code ns1}. Records every request and its headers.
   */
  private static final class FakeRestClient implements RESTClient {
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final Map<String, Map<String, String>> headers = new HashMap<>();
    private final List<CreateTableRequest> createRequests = new CopyOnWriteArrayList<>();
    private Map<String, String> tableConfig = Collections.emptyMap();
    private boolean tableExists = true;
    private boolean namespaceExists = true;
    private String forbiddenMessage;

    RESTCatalog newCatalog(boolean vended) {
      Map<String, String> properties = new HashMap<>();
      properties.put("uri", "http://catalog.invalid/api/catalog");
      if (vended) {
        properties.put("header." + DELEGATION_HEADER, "vended-credentials");
      }
      return newCatalog(properties);
    }

    RESTCatalog newCatalog(Map<String, String> properties) {
      RESTCatalog catalog =
          new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), config -> this);
      catalog.setConf(new Configuration(false));
      catalog.initialize("test", properties);
      return catalog;
    }

    Map<String, String> headersOf(String request) {
      synchronized (headers) {
        return headers.getOrDefault(request, Collections.emptyMap());
      }
    }

    long count(String request) {
      return requests.stream().filter(request::equals).count();
    }

    private void recordRequest(String request, Map<String, String> requestHeaders) {
      requests.add(request);
      synchronized (headers) {
        headers.put(request, requestHeaders == null ? Collections.emptyMap() : requestHeaders);
      }
    }

    private static <T> T fail(
        Consumer<ErrorResponse> errorHandler, int code, String type, String message) {
      errorHandler.accept(
          ErrorResponse.builder().responseCode(code).withType(type).withMessage(message).build());
      throw new AssertionError("The error handler did not throw for HTTP " + code);
    }

    private static TableMetadata metadata(String name, boolean committed) {
      TableMetadata metadata =
          TableMetadata.newTableMetadata(
              SCHEMA,
              PartitionSpec.unpartitioned(),
              "s3://bucket/wh/ns1/" + name,
              Collections.emptyMap());
      if (!committed) {
        return metadata;
      }
      return TableMetadata.buildFrom(metadata)
          .discardChanges()
          .withMetadataLocation("s3://bucket/wh/ns1/" + name + "/metadata/00000.metadata.json")
          .build();
    }

    @Override
    public void head(
        String path, Map<String, String> requestHeaders, Consumer<ErrorResponse> errorHandler) {
      recordRequest("HEAD " + path, requestHeaders);
      fail(errorHandler, 404, "NoSuchTableException", "not found");
    }

    @Override
    public <T extends RESTResponse> T delete(
        String path,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      recordRequest("DELETE " + path, requestHeaders);
      if (path.startsWith(TABLES_PATH + "/")) {
        return null;
      }
      throw new UnsupportedOperationException(path);
    }

    @Override
    public <T extends RESTResponse> T get(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      recordRequest("GET " + path, requestHeaders);
      if ("v1/config".equals(path)) {
        return responseType.cast(ConfigResponse.builder().build());
      }
      if (path.startsWith(TABLES_PATH + "/")) {
        String name = path.substring(TABLES_PATH.length() + 1);
        if (forbiddenMessage != null) {
          return fail(errorHandler, 403, "ForbiddenException", forbiddenMessage);
        }
        if (!tableExists) {
          return fail(errorHandler, 404, "NoSuchTableException", "Table does not exist: " + name);
        }
        return responseType.cast(
            LoadTableResponse.builder()
                .withTableMetadata(metadata(name, true))
                .addAllConfig(tableConfig)
                .build());
      }
      throw new UnsupportedOperationException(path);
    }

    @Override
    public <T extends RESTResponse> T post(
        String path,
        RESTRequest body,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      recordRequest("POST " + path, requestHeaders);
      if (TABLES_PATH.equals(path)) {
        CreateTableRequest request = (CreateTableRequest) body;
        createRequests.add(request);
        if (!namespaceExists) {
          return fail(errorHandler, 404, "NoSuchNamespaceException", "Namespace does not exist");
        }
        return responseType.cast(
            LoadTableResponse.builder()
                .withTableMetadata(metadata(request.name(), !request.stageCreate()))
                .addAllConfig(tableConfig)
                .build());
      }
      throw new UnsupportedOperationException(path);
    }

    @Override
    public <T extends RESTResponse> T postForm(
        String path,
        Map<String, String> formData,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException(path);
    }

    @Override
    public void close() {}
  }
}
