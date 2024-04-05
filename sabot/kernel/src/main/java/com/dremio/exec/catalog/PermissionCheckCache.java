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
package com.dremio.exec.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Provider;

/** Thread-safe cache of permission checks. Caches up to maximumSize entries. */
class PermissionCheckCache {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PermissionCheckCache.class);

  public enum PermissionCheckAccessType {
    PERMISSION_CACHE_MISS,
    PERMISSION_CACHE_HIT,
    PERMISSION_CACHE_EXPIRED
  }

  private final Cache<Key, Value> permissionsCache;
  protected final Provider<StoragePlugin> plugin;
  private final Provider<Long> authTtlMs;

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  public PermissionCheckCache(
      Provider<StoragePlugin> plugin, Provider<Long> authTtlMs, final long maximumSize) {
    this.plugin = plugin;
    this.authTtlMs = authTtlMs;
    permissionsCache = CacheBuilder.newBuilder().maximumSize(maximumSize).build();
  }

  @VisibleForTesting
  Cache<Key, Value> getPermissionsCache() {
    return permissionsCache;
  }

  protected boolean checkPlugin(
      final String username,
      final NamespaceKey namespaceKey,
      final DatasetConfig config,
      final SourceConfig sourceConfig) {
    return plugin.get().hasAccessPermission(username, namespaceKey, config);
  }

  /**
   * Delegates call to the actual {@link StoragePlugin#hasAccessPermission permission check} and
   * caches the result based on the metadata policy defined in the source configuration.
   *
   * <p>See {@link StoragePlugin#hasAccessPermission}.
   *
   * @param username username to check access for
   * @param namespaceKey path to check access for
   * @param config dataset properties
   * @param metadataStatsCollector stat collector
   * @param sourceConfig source config
   * @return true iff user has access
   * @throws UserException if the underlying calls throws any exception
   */
  public boolean hasAccess(
      final String username,
      final NamespaceKey namespaceKey,
      final DatasetConfig config,
      final MetadataStatsCollector metadataStatsCollector,
      final SourceConfig sourceConfig) {
    final Stopwatch permissionCheck = Stopwatch.createStarted();

    // if we are unable to cache, go direct.  Also don't cache system sources checks.
    if (authTtlMs.get() == 0 || "ESYS".equals(sourceConfig.getType())) {
      boolean hasAccess = checkPlugin(username, namespaceKey, config, sourceConfig);
      permissionCheck.stop();
      metadataStatsCollector.addDatasetStat(
          namespaceKey.getSchemaPath(),
          PermissionCheckAccessType.PERMISSION_CACHE_MISS.name(),
          permissionCheck.elapsed(TimeUnit.MILLISECONDS));
      return hasAccess;
    }

    final Key key = new Key(username, namespaceKey);
    final long now = System.currentTimeMillis();

    final Callable<Value> loader =
        () -> {
          final boolean hasAccess = checkPlugin(username, namespaceKey, config, sourceConfig);
          if (!hasAccess) {
            throw NoAccessException.INSTANCE;
          }
          return new Value(true, now);
        };

    Value value;
    try {
      PermissionCheckAccessType permissionCheckAccessType;
      value = getFromPermissionsCache(key, loader);

      if (now == value.createdAt) {
        permissionCheckAccessType = PermissionCheckAccessType.PERMISSION_CACHE_MISS;
      } else {
        permissionCheckAccessType = PermissionCheckAccessType.PERMISSION_CACHE_HIT;
      }

      // check validity, and reload if expired
      if (now - value.createdAt > authTtlMs.get()) {
        permissionsCache.invalidate(key);
        value = getFromPermissionsCache(key, loader);
        permissionCheckAccessType = PermissionCheckAccessType.PERMISSION_CACHE_EXPIRED;
      }

      permissionCheck.stop();
      metadataStatsCollector.addDatasetStat(
          namespaceKey.getSchemaPath(),
          permissionCheckAccessType.name(),
          permissionCheck.elapsed(TimeUnit.MILLISECONDS));

      return value.hasAccess;
    } catch (ExecutionException e) {
      throw new RuntimeException(
          "Permission check loader should not throw a checked exception", e.getCause());
    } catch (UncheckedExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof UserException) {
        throw (UserException) cause;
      }
      throw UserException.permissionError(cause)
          .message("Access denied reading dataset %s.", namespaceKey.toString())
          .build(logger);
    }
  }

  protected Value getFromPermissionsCache(Key key, Callable<Value> loader)
      throws ExecutionException {
    Value value;

    try {
      value = permissionsCache.get(key, loader);
    } catch (UncheckedExecutionException e) {
      if (e.getCause() != NoAccessException.INSTANCE) {
        throw e;
      }

      value = new Value(false, System.currentTimeMillis());
    }

    return value;
  }

  /** Clears the permission cache */
  void clear() {
    getPermissionsCache().invalidateAll();
  }

  @VisibleForTesting
  static final class Key {
    final String username;
    final NamespaceKey namespaceKey;

    Key(String username, NamespaceKey namespaceKey) {
      this.username = checkNotNull(username);
      this.namespaceKey = checkNotNull(namespaceKey);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final Key that = (Key) obj;
      return Objects.equals(this.username, that.username)
          && Objects.equals(this.namespaceKey, that.namespaceKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(username, namespaceKey);
    }
  }

  @VisibleForTesting
  static final class Value {
    final boolean hasAccess;
    final long createdAt;

    Value(boolean hasAccess, long createdAt) {
      this.hasAccess = hasAccess;
      this.createdAt = createdAt;
    }
  }

  /**
   * Exception used if user has to access. This ensures that we do not cache any no-access
   * permissions.
   */
  protected static final class NoAccessException extends RuntimeException {
    // we create a singleton since we always catch and don't need a stack trace
    protected static final NoAccessException INSTANCE = new NoAccessException();
  }
}
