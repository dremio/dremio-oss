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
package com.dremio.common.map;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import java.util.Map;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A special type of {@link ImmutableBiMap} with {@link String}s as keys, and the case of a key is
 * ignored for get operations
 *
 * @param <VALUE> the type of values to be stored in the map
 */
public final class CaseInsensitiveImmutableBiMap<VALUE> implements BiMap<String, VALUE> {
  private final ImmutableBiMap<String, VALUE> underlyingMap;

  private CaseInsensitiveImmutableBiMap(final ImmutableBiMap<String, VALUE> underlyingMap) {
    this.underlyingMap = underlyingMap;
  }

  /**
   * Returns a new instance of {@link ImmutableBiMap} with key case-insensitivity.
   *
   * @param map map to copy from
   * @param <VALUE> type of values to be stored in the map
   * @return key case-insensitive immutable bi-map
   */
  public static <VALUE> CaseInsensitiveImmutableBiMap<VALUE> newImmutableMap(
      final Map<? extends String, ? extends VALUE> map) {
    final ImmutableBiMap.Builder<String, VALUE> builder = ImmutableBiMap.builder();
    for (final Entry<? extends String, ? extends VALUE> entry : map.entrySet()) {
      builder.put(entry.getKey().toLowerCase(), entry.getValue());
    }
    return new CaseInsensitiveImmutableBiMap<>(builder.build());
  }

  @Override
  public int size() {
    return underlyingMap.size();
  }

  @Override
  public boolean isEmpty() {
    return underlyingMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return key instanceof String && underlyingMap.containsKey(((String) key).toLowerCase());
  }

  @Override
  public boolean containsValue(Object value) {
    return underlyingMap.containsValue(value);
  }

  @Override
  public VALUE get(Object key) {
    return key instanceof String ? underlyingMap.get(((String) key).toLowerCase()) : null;
  }

  @Nullable
  @Override
  public VALUE put(@Nullable String s, @Nullable VALUE value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VALUE remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public VALUE forcePut(@Nullable String s, @Nullable VALUE value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ? extends VALUE> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> keySet() {
    return underlyingMap.keySet();
  }

  @Override
  public Set<VALUE> values() {
    return underlyingMap.values();
  }

  @Override
  public Set<Entry<String, VALUE>> entrySet() {
    return underlyingMap.entrySet();
  }

  @Override
  public BiMap<VALUE, String> inverse() {
    return underlyingMap.inverse();
  }
}
