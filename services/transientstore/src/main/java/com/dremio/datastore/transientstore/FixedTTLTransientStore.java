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
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.KVStoreOptionUtility;
import com.dremio.datastore.api.options.VersionOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A wrapper for TransientStore that enforces a fixed TTL set on object creation.
 *
 * @param <K> the key to store
 * @param <V> the value to store
 */
public class FixedTTLTransientStore<K, V> implements TransientStore<K, V> {
  private final TransientStore<K, V> delegate;
  // Time to live in seconds
  private final int ttl;

  /**
   * A wrapper providing fixed ttl to D.
   *
   * @param delegate the object on which a fixed TTL is being forced.
   * @param ttl Time to live in seconds
   */
  public FixedTTLTransientStore(TransientStore<K, V> delegate, int ttl) {
    this.delegate = delegate;
    this.ttl = ttl;
  }

  @Override
  public Document<K, V> get(K key, KVStore.GetOption... options) {
    return delegate.get(key, options);
  }

  @Override
  public Iterable<Document<K, V>> get(List<K> keys, KVStore.GetOption... options) {
    return delegate.get(keys, options);
  }

  @Override
  public Document<K, V> put(K key, V value, KVStore.PutOption... options) {
    final Optional<KVStore.PutOption> ttlOption = KVStoreOptionUtility.getTTLOption(options);
    if (ttlOption.isPresent()) {
      throw new IllegalArgumentException("Fixed TTL Option cannot be overridden in put()");
    }
    // merge options and ttl option.
    List<KVStore.PutOption> putOptions = Arrays.stream(options).collect(Collectors.toList());
    putOptions.add(VersionOption.getTTLOption(ttl));
    return delegate.put(key, value, putOptions.toArray(new KVStore.PutOption[putOptions.size()]));
  }

  @Override
  public void delete(K key, KVStore.DeleteOption... options) {
    delegate.delete(key, options);
  }

  @Override
  public boolean contains(K key) {
    return delegate.contains(key);
  }

  @Override
  public Iterable<Document<K, V>> find(String pattern, KVStore.GetOption... options) {
    return delegate.find(pattern, options);
  }
}
