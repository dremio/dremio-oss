/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.SchemaTreeProvider.MetadataStatsCollector;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Thread-safe cache of permission checks. Caches up to maximumSize entries.
 */
public class PermissionCheckCache {

  public enum PermissionCheckAccessType {
    PERMISSION_CACHE_MISS,
    PERMISSION_CACHE_HIT,
    PERMISSION_CACHE_EXPIRED
  }

  private final Cache<Key, Value> permissionsCache;

  public PermissionCheckCache(final long maximumSize) {
    permissionsCache = CacheBuilder.newBuilder()
        .maximumSize(maximumSize)
        .build();
  }

  @VisibleForTesting
  Cache<Key, Value> getPermissionsCache() {
    return permissionsCache;
  }

  /**
   * Delegates call to the actual {@link StoragePlugin#hasAccessPermission permission check} and caches the
   * result based on the metadata policy defined in the source configuration.
   *
   * See {@link StoragePlugin#hasAccessPermission}.
   *
   * @param registry storage plugin to check access against
   * @param username username to check access for
   * @param namespaceKey path to check access for
   * @param config dataset properties
   * @param metadataPolicy metadata policy
   * @param metadataStatsCollector stat collector
   * @return true iff user has access
   * @throws UserException if the underlying calls throws any exception
   */
  public boolean hasAccess(final StoragePlugin registry, final String username, final NamespaceKey namespaceKey,
                           final DatasetConfig config, final MetadataPolicy metadataPolicy, final MetadataStatsCollector metadataStatsCollector) {
    final long authTtlMs = metadataPolicy == null ? 0 : metadataPolicy.getAuthTtlMs();
    final Stopwatch permissionCheck = Stopwatch.createStarted();

    if (authTtlMs == 0) {
      boolean hasAccess = registry.hasAccessPermission(username, namespaceKey, config);
      permissionCheck.stop();
      metadataStatsCollector.addDatasetStat(namespaceKey.getSchemaPath(), PermissionCheckAccessType.PERMISSION_CACHE_MISS.name(), permissionCheck.elapsed(TimeUnit.MILLISECONDS));
      return hasAccess;
    }

    final Key key = new Key(username, namespaceKey);
    final long now = System.currentTimeMillis();

    final Callable<Value> loader = new Callable<Value>() {
      @Override
      public Value call() {
        final boolean hasAccess = registry.hasAccessPermission(username, namespaceKey, config);
        return new Value(hasAccess, now);
      }
    };

    Value value;
    try {
      PermissionCheckAccessType permissionCheckAccessType;
      value = permissionsCache.get(key, loader);

      if (now == value.createdAt) {
        permissionCheckAccessType = PermissionCheckAccessType.PERMISSION_CACHE_MISS;
      } else {
        permissionCheckAccessType = PermissionCheckAccessType.PERMISSION_CACHE_HIT;
      }

      // check validity, and reload if expired
      if (now - value.createdAt > authTtlMs) {
        permissionsCache.invalidate(key);
        value = permissionsCache.get(key, loader);
        permissionCheckAccessType = PermissionCheckAccessType.PERMISSION_CACHE_EXPIRED;
      }

      permissionCheck.stop();
      metadataStatsCollector.addDatasetStat(namespaceKey.getSchemaPath(), permissionCheckAccessType.name(), permissionCheck.elapsed(TimeUnit.MILLISECONDS));

      return value.hasAccess;
    } catch (ExecutionException e) {
      throw new RuntimeException("permission check loader should not throw a checked exception", e.getCause());
    } catch (UncheckedExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof UserException) {
        throw (UserException) cause;
      }
      throw UserException.permissionError()
        .message("Access denied reading dataset %s. %s", namespaceKey.toString(), cause.getMessage())
        .build();
    }
  }

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
      return Objects.equals(this.username, that.username) &&
          Objects.equals(this.namespaceKey, that.namespaceKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(username, namespaceKey);
    }
  }

  static final class Value {
    final boolean hasAccess;
    final long createdAt;

    Value(boolean hasAccess, long createdAt) {
      this.hasAccess = hasAccess;
      this.createdAt = createdAt;
    }
  }
}
