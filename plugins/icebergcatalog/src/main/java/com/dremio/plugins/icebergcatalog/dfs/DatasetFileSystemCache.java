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
package com.dremio.plugins.icebergcatalog.dfs;

import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_FILE_SYSTEM_EXPIRE_AFTER_WRITE_MINUTES;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_FILE_SYSTEM_OPTIMISTIC_LOCKING;
import static com.dremio.exec.store.hive.exec.FileSystemConfUtil.AZURE_FILE_SYSTEM;
import static com.dremio.exec.store.hive.exec.FileSystemConfUtil.GCS_FILE_SYSTEM;
import static com.dremio.exec.store.hive.exec.FileSystemConfUtil.S3_FILE_SYSTEM;
import static com.dremio.io.file.UriSchemes.DREMIO_AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_S3_SCHEME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.hive.exec.FileSystemConfUtil;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorStats;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is wrapper for the cache which holds the FileSystem objects */
public class DatasetFileSystemCache implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DatasetFileSystemCache.class);
  private static final int MAX_HOURS_WAIT_FOR_FS_WRITE_LOCK = 4;

  /**
   * Optional entry of a configuration returned by the configuration provider: the wall clock time
   * (epoch millis) after which the FileSystem created from it must be replaced, for instance
   * because it carries temporary credentials that expire. The cache drops the FileSystem at that
   * time at the latest (otherwise after {@code
   * plugins.restcatalog.file_system.expire_after_write_minutes}), so the next use creates a new one
   * from a fresh configuration. The entry is removed before the FileSystem is created.
   */
  public static final String FS_EXPIRES_AT_MILLIS = "dremio.icebergcatalog.fs.expires-at-millis";

  /**
   * Shortest lifetime of an entry whose configuration set {@link #FS_EXPIRES_AT_MILLIS}, even if
   * that time has already passed (e.g. after the wall clock stepped forward). A zero lifetime would
   * make every lookup create a new FileSystem, and users of the cache could never lock the
   * FileSystem they got.
   */
  @VisibleForTesting
  public static final long MIN_EXPIRING_LIFETIME_NANOS = TimeUnit.SECONDS.toNanos(1);

  @VisibleForTesting
  protected LoadingCache<DatasetFileSystemCacheKey, LockableHadoopFileSystem> cache;

  private final Function<List<String>, Configuration> fsConfProvider;
  private final OptionManager optionManager;
  private final boolean cachingPerDataset;
  private final Ticker ticker;
  private final LongSupplier clockMillis;

  public DatasetFileSystemCache(
      Function<List<String>, Configuration> fsConfProvider, OptionManager optionManager) {
    this(fsConfProvider, optionManager, false);
  }

  /**
   * @param fsConfProvider provides the configuration of a new FileSystem, given the dataset (full
   *     table path) it is created for, or null
   * @param cachingPerDataset whether FileSystem instances are cached per dataset (needed when the
   *     configuration differs per dataset, e.g. per-table credentials)
   */
  public DatasetFileSystemCache(
      Function<List<String>, Configuration> fsConfProvider,
      OptionManager optionManager,
      boolean cachingPerDataset) {
    this(
        fsConfProvider,
        optionManager,
        cachingPerDataset,
        Ticker.systemTicker(),
        System::currentTimeMillis);
  }

  /**
   * @param ticker the time source of the cache's expiration
   * @param clockMillis the wall clock that {@link #FS_EXPIRES_AT_MILLIS} values are compared with
   */
  @VisibleForTesting
  public DatasetFileSystemCache(
      Function<List<String>, Configuration> fsConfProvider,
      OptionManager optionManager,
      boolean cachingPerDataset,
      Ticker ticker,
      LongSupplier clockMillis) {
    this.fsConfProvider = fsConfProvider;
    this.optionManager = optionManager;
    this.cachingPerDataset = cachingPerDataset;
    this.ticker = Preconditions.checkNotNull(ticker);
    this.clockMillis = Preconditions.checkNotNull(clockMillis);
  }

  @VisibleForTesting
  synchronized void initCache() {
    if (cache != null) {
      return;
    }
    this.cache =
        buildCacheExpiration(Caffeine.newBuilder().ticker(ticker), optionManager)
            .removalListener(
                (key, lockableFs, cause) -> {
                  if (lockableFs != null) {
                    try {
                      if (lockableFs.waitForClose(
                          TimeUnit.HOURS.toMillis(MAX_HOURS_WAIT_FOR_FS_WRITE_LOCK))) {
                        logger.warn("Timed out waiting for FS closure on {}", lockableFs);
                      }
                      logger.debug("Closing FS instance {} on cache removal.", lockableFs.getFs());
                      lockableFs.getFs().close();
                    } catch (IOException e) {
                      // Ignore
                      logger.error("Unable to clean FS from HadoopFileSystemCache", e);
                    } catch (InterruptedException e) {
                      // Ignore
                      logger.error(
                          "Interrupted while waiting for FS to be de-referenced for closure.", e);
                    }
                  }
                })
            .build(
                key -> {
                  final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
                  final UserGroupInformation ugi;
                  if (key.getUserName().equals(loginUser.getUserName())
                      || SYSTEM_USERNAME.equals(key.getUserName())) {
                    ugi = loginUser;
                  } else {
                    ugi = UserGroupInformation.createProxyUser(key.getUserName(), loginUser);
                  }

                  Configuration fsConf = fsConfProvider.apply(key.getDataset());
                  final long expiresAtMillis = fsConf.getLong(FS_EXPIRES_AT_MILLIS, Long.MAX_VALUE);
                  fsConf.unset(FS_EXPIRES_AT_MILLIS);
                  URI uri = injectDremioFsImpl(key.getUri(), fsConf);
                  String scheme = Optional.ofNullable(uri.getScheme()).orElse("");

                  final PrivilegedExceptionAction<LockableHadoopFileSystem> fsFactory =
                      () -> {
                        // Do not use FileSystem#newInstance(Configuration) as it adds filesystem
                        // into the Hadoop cache :(
                        // Mimic instead Hadoop FileSystem#createFileSystem() method
                        final Class<? extends FileSystem> fsClass =
                            FileSystem.getFileSystemClass(scheme, fsConf);
                        final FileSystem fs = ReflectionUtils.newInstance(fsClass, fsConf);
                        fs.initialize(uri, fsConf);
                        return new LockableHadoopFileSystem(fs, expiresAtMillis);
                      };

                  try {
                    return ugi.doAs(fsFactory);
                  } catch (IOException | InterruptedException e) {
                    logger.error(
                        "Failed to create FileSystem for path: {} with user: {}",
                        key.getUri(),
                        key.getUserName(),
                        e);
                    throw UserException.ioExceptionError(e).buildSilently();
                  } catch (Exception e) {
                    logger.error(
                        "Failed to create FileSystem for path: {} with user: {}",
                        key.getUri(),
                        key.getUserName(),
                        e);
                    throw UserException.validationError(e)
                        .message(
                            "Credentials for the Storage Provider must be valid and have required access")
                        .buildSilently();
                  }
                });
  }

  /**
   * Entries expire {@code plugins.restcatalog.file_system.expire_after_write_minutes} after they
   * were created, or earlier when their configuration said so (see {@link #FS_EXPIRES_AT_MILLIS}).
   */
  protected Caffeine<DatasetFileSystemCacheKey, LockableHadoopFileSystem> buildCacheExpiration(
      Caffeine builder, OptionManager optionManager) {
    long expirationMinutes =
        optionManager.getOption(RESTCATALOG_PLUGIN_FILE_SYSTEM_EXPIRE_AFTER_WRITE_MINUTES);
    Preconditions.checkState(expirationMinutes > 0, "FS cache expiration must not be 0");

    final long maxLifetimeNanos = TimeUnit.MINUTES.toNanos(expirationMinutes);
    return builder.expireAfter(
        new Expiry<DatasetFileSystemCacheKey, LockableHadoopFileSystem>() {
          @Override
          public long expireAfterCreate(
              DatasetFileSystemCacheKey key, LockableHadoopFileSystem fs, long currentTime) {
            return lifetimeNanos(fs, maxLifetimeNanos, clockMillis.getAsLong());
          }

          @Override
          public long expireAfterUpdate(
              DatasetFileSystemCacheKey key,
              LockableHadoopFileSystem fs,
              long currentTime,
              long currentDuration) {
            return lifetimeNanos(fs, maxLifetimeNanos, clockMillis.getAsLong());
          }

          @Override
          public long expireAfterRead(
              DatasetFileSystemCacheKey key,
              LockableHadoopFileSystem fs,
              long currentTime,
              long currentDuration) {
            return currentDuration;
          }
        });
  }

  /**
   * How long a new cache entry lives: the configured maximum, or less if the FS expires sooner, but
   * at least {@link #MIN_EXPIRING_LIFETIME_NANOS}.
   */
  @VisibleForTesting
  static long lifetimeNanos(LockableHadoopFileSystem fs, long maxLifetimeNanos, long nowMillis) {
    long expiresAtMillis = fs.getExpiresAtMillis();
    if (expiresAtMillis == Long.MAX_VALUE) {
      return maxLifetimeNanos;
    }
    long remainingNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, expiresAtMillis - nowMillis));
    return Math.min(maxLifetimeNanos, Math.max(MIN_EXPIRING_LIFETIME_NANOS, remainingNanos));
  }

  private static URI injectDremioFsImpl(URI uri, Configuration conf) {
    try {
      URI modifiedURI = uri;
      String scheme = uri.getScheme();
      if (scheme == null || "file".equalsIgnoreCase(scheme)) {
        modifiedURI = HadoopFileSystem.getLocal(conf).makeQualified(Path.of(uri)).toURI();
      } else {
        scheme = scheme.toLowerCase(Locale.ROOT);
        if (S3_FILE_SYSTEM.contains(scheme)) {
          // The authority duplication here is intentional, it will break if removed.
          modifiedURI =
              new URI(
                  DREMIO_S3_SCHEME,
                  uri.getRawAuthority(),
                  "/" + uri.getRawAuthority() + uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
        } else if (AZURE_FILE_SYSTEM.contains(scheme)) {
          modifiedURI =
              new URI(
                  DREMIO_AZURE_SCHEME,
                  uri.getRawAuthority(),
                  "/" + uri.getUserInfo() + uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
          conf.set("old_scheme", scheme);
          conf.set("authority", uri.getRawAuthority());
        } else if (GCS_FILE_SYSTEM.contains(scheme)) {
          modifiedURI =
              new URI(
                  DREMIO_GCS_SCHEME,
                  uri.getRawAuthority(),
                  "/" + uri.getRawAuthority() + uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
        }
        // else if: HDFS: no URI manipulation required
        FileSystemConfUtil.initializeConfiguration(modifiedURI, conf);
      }
      return modifiedURI;
    } catch (IOException | URISyntaxException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  public com.dremio.io.file.FileSystem load(
      String filePath,
      String userName,
      String userId,
      List<String> dataset,
      OperatorStats stats,
      boolean isAsyncEnabled) {
    if (cache == null) {
      initCache();
    }

    Path path = Path.of(filePath);
    URI originalUri = path.toURI();
    DatasetFileSystemCacheKey key =
        new DatasetFileSystemCacheKey(
            originalUri, userName, isCachingPerDataset() ? dataset : null);

    boolean optimisticLocking =
        optionManager.getOption(RESTCATALOG_PLUGIN_FILE_SYSTEM_OPTIMISTIC_LOCKING);

    return new SelfManagingCachedFileSystem(key, cache, stats, isAsyncEnabled, optimisticLocking);
  }

  /**
   * Whether to store FileSystem instances on a per dataset (i.e. table) basis
   *
   * @return the setting
   */
  protected boolean isCachingPerDataset() {
    return cachingPerDataset;
  }

  /**
   * Drops (and closes once they are no longer in use) the cached FileSystems created for the
   * datasets that match, e.g. those holding the credentials of a table that was dropped or created.
   * FileSystems cached for no particular dataset are kept.
   */
  public void invalidateDatasets(Predicate<List<String>> datasetFilter) {
    LoadingCache<DatasetFileSystemCacheKey, LockableHadoopFileSystem> current = cache;
    if (current == null) {
      return;
    }
    current.invalidateAll(
        current.asMap().keySet().stream()
            .filter(key -> key.getDataset() != null && datasetFilter.test(key.getDataset()))
            .collect(Collectors.toList()));
  }

  @Override
  public void close() throws Exception {
    if (cache == null) {
      // Never used (e.g. the source failed to start): nothing to close.
      return;
    }
    // Empty cache
    cache.invalidateAll();
    cache.cleanUp();
  }
}
