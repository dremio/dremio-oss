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
package com.dremio.datastore.format.compound;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;

import java.util.AbstractList;
import java.util.List;

/**
 * {@link CompoundKey} representation for 2 keys.
 *
 * There are multiple key elements per instance where K1 and K2 denote the key elements.
 *
 * @param <K1> - The type of the first key.
 * @param <K2> - The type of the second key.
 */
public final class KeyPair<K1, K2> extends AbstractList<Object> implements CompoundKey {

  private final K1 key1;
  private final K2 key2;

  public KeyPair(K1 key1, K2 key2) {
    this.key1 = key1;
    this.key2 = key2;
  }

  /**
   * Converts a list into a KeyPair instance
   *
   * @param <K1> - The type of the first key.
   * @param <K2> - The type of the second key.
   * @param list - The List of object instances.
   * @return A KeyPair instance.
   * @throws IllegalArgumentException if the list does not have exactly 2 elements.
   */
  @SuppressWarnings("unchecked")
  public static <K1, K2> KeyPair<K1, K2> of(List<Object> list) {
    checkArgument(list.size() == 2, "list should be of size 2, had actually %s elements", list);

    final K1 value1 = (K1) list.get(0);
    final K2 value2 = (K2) list.get(1);

    return new KeyPair<>(value1, value2);
  }

  @Override
  public Object get(int index) {
    checkElementIndex(index, size());
    switch(index) {
    case 0: return key1;
    case 1: return key2;
    default:
      throw new AssertionError("unexpected index " + index);
    }
  }

  @Override
  public int size() {
    return 2;
  }

  public K1 getKey1() {
    return key1;
  }

  public K2 getKey2() {
    return key2;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof KeyPair)) {
      return false;
    }

    final KeyPair<?, ?> keyPair = (KeyPair<?, ?>) o;

    return KeyUtils.equals(key1, keyPair.key1) &&
      KeyUtils.equals(key2, keyPair.key2);
  }

  @Override
  public int hashCode() {
    return KeyUtils.hash(key1, key2);
  }

  @Override
  public String toString() {
    return "KeyPair{ key1=" + KeyUtils.toString(key1) +
      ", key2=" + KeyUtils.toString(key2) + '}';
  }
}
