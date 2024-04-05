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
package com.dremio.datastore.transientstore;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.KVStoreOptionUtility;
import com.dremio.datastore.api.options.VersionOption;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.AbstractMap;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-memory store providing Time-To-Live entry expiry.
 *
 * @param <K> key by which values are retrieved.
 * @param <V> the value stored in the store.
 */
public class InMemoryTransientStore<K, V> implements TransientStore<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryTransientStore.class);
  // The value stored in the cache comprises the value and a stringified UUID.
  private final Cache<K, Entry<V, String>> cache;
  // Time in seconds before an entry in the cache is expired.
  private final int timeToLive;

  /**
   * Creates an instance of this class.
   *
   * @param ttlInSeconds the number of seconds after which entries are expired from the store since
   *     they
   */
  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  public InMemoryTransientStore(int ttlInSeconds) {
    this.timeToLive = ttlInSeconds;

    // Setup the data-set loading-cache.
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(ttlInSeconds, TimeUnit.SECONDS)
            .concurrencyLevel(
                4) // setting default value here in case default changes in the future.
            .build();
  }

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  @VisibleForTesting
  public InMemoryTransientStore(int ttlInSeconds, Ticker ticker) {
    this.timeToLive = ttlInSeconds;

    // Setup the data-set loading-cache.
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(ttlInSeconds, TimeUnit.SECONDS)
            .concurrencyLevel(
                4) // setting default value here in case default changes in the future.
            .ticker(ticker)
            .build();
  }

  /**
   * Retrieves an entry for a key. This resets the TTL time for that entry.
   *
   * @param key the key to use to look for the value.
   * @param options extra options for GET operation.
   * @return
   */
  @Override
  public Document<K, V> get(K key, KVStore.GetOption... options) {
    Entry<V, String> value = cache.getIfPresent(key);
    if (null != value) {
      return new ImmutableDocument.Builder<K, V>()
          .setKey(key)
          .setValue(value.getKey())
          .setTag(value.getValue())
          .build();
    } else {
      return null;
    }
  }

  /**
   * Retrieves a list of entries for a list of keys. This resets the TTL time for these entries.
   *
   * @param keys a list of keys of which their values are to be retrieved.
   * @param options extra options for GET operations.
   * @return an iterable of retrieved keys and their values.
   */
  @Override
  public Iterable<Document<K, V>> get(List<K> keys, KVStore.GetOption... options) {

    ImmutableMap<K, Entry<V, String>> map = cache.getAllPresent(keys);
    Iterable<Document<K, V>> list =
        Iterables.transform(
            map.entrySet(),
            entry ->
                new ImmutableDocument.Builder<K, V>()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue().getKey())
                    .setTag(entry.getValue().getValue())
                    .build());

    return list;
  }

  /**
   * Puts or replaces a value in the store. This resets the TTL time for the entry put.
   *
   * @param key the key to save the value.
   * @param value the value to save.
   * @param options extra options for PUT operation.
   * @return the entry retrieved.
   */
  @Override
  public Document<K, V> put(K key, V value, KVStore.PutOption... options) {
    final Optional<KVStore.PutOption> ttlOption = KVStoreOptionUtility.getTTLOption(options);

    if (ttlOption.isPresent()) {
      throw new IllegalArgumentException("Fixed TTL Option cannot be overridden in put()");
    }

    final String tag = UUID.randomUUID().toString();
    final Entry<V, String> newValueEntry = new AbstractMap.SimpleEntry<>(value, tag);
    final Optional<KVStore.PutOption> putOrVersionOption =
        KVStoreOptionUtility.getCreateOrVersionOption(options);

    if (putOrVersionOption.isPresent()) {
      if (putOrVersionOption.get() == KVStore.PutOption.CREATE) {
        // If the version is set to VersionOption.CREATE, we need to ensure that the key is not in
        // the cache, and if it is, throw a CME.
        final Entry<V, String> returnedValueEntry = cache.asMap().putIfAbsent(key, newValueEntry);
        if (null != returnedValueEntry) {
          throw new ConcurrentModificationException("Key already exists");
        }
      } else if (putOrVersionOption.get() instanceof VersionOption) {
        // The VersionOption is specified to a value other than VersionOption.CREATE,
        // we need to validate the version supplied against what's in the cache
        // and throw a ConcurrentModificationException if they do not match.
        final VersionOption versionOption = (VersionOption) putOrVersionOption.get();
        cache
            .asMap()
            .compute(
                key,
                (k, v) -> {
                  if (null == v || !v.getValue().equals(versionOption.getTag())) {
                    throw new ConcurrentModificationException("Key already updated");
                  } else {
                    return newValueEntry;
                  }
                });
      }
    } else {
      // Store value with new tag value.
      cache.put(key, newValueEntry);
    }

    return new ImmutableDocument.Builder<K, V>().setKey(key).setValue(value).setTag(tag).build();
  }

  @Override
  public void delete(K key, KVStore.DeleteOption... options) {
    final Optional<KVStore.PutOption> putOrVersionOption =
        KVStoreOptionUtility.getCreateOrVersionOption(options);
    if (putOrVersionOption.isPresent() && putOrVersionOption.get() instanceof VersionOption) {
      final VersionOption versionOption = (VersionOption) putOrVersionOption.get();
      cache
          .asMap()
          .compute(
              key,
              (k, v) -> {
                if (null == v) {
                  throw new ConcurrentModificationException("Key already deleted");
                } else if (!v.getValue().equals(versionOption.getTag())) {
                  throw new ConcurrentModificationException("Version mismatch");
                } else {
                  return null;
                }
              });
    } else {
      // if no version option is specified, the key can be deleted anyway.
      cache.invalidate(key);
    }
  }

  /**
   * Tests if the key is present in the store and not expired. Calling this method does not reset
   * the TTL timer.
   *
   * @param key the key of the document to search for.
   * @return true if the key is contained in the store.
   */
  @Override
  public boolean contains(K key) {
    return cache.asMap().containsKey(key);
  }
}
