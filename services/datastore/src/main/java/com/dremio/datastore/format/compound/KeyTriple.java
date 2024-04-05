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
 * {@link CompoundKey} representation for 3 keys.
 *
 * <p>There are multiple key elements per instance where K1, K2 and K3 denote the key elements.
 *
 * @param <K1> - The type of the first key.
 * @param <K2> - The type of the second key.
 * @param <K3> - The type of the third key.
 */
public final class KeyTriple<K1, K2, K3> extends AbstractList<Object> implements CompoundKey {

  private final K1 key1;
  private final K2 key2;
  private final K3 key3;

  public KeyTriple(K1 key1, K2 key2, K3 key3) {
    this.key1 = key1;
    this.key2 = key2;
    this.key3 = key3;
  }

  /**
   * Converts a list into a KeyTriple instance
   *
   * @param <K1> - The type of the first key.
   * @param <K2> - The type of the second key.
   * @param <K3> - The type of the third key.
   * @param list - The List of object instances.
   * @return A KeyTriple instance.
   * @throws IllegalArgumentException if the list does not have exactly 3 elements.
   */
  @SuppressWarnings("unchecked")
  public static <K1, K2, K3> KeyTriple<K1, K2, K3> of(List<Object> list) {
    checkArgument(list.size() == 3, "list should be of size 3, had actually %s elements", list);

    final K1 value1 = (K1) list.get(0);
    final K2 value2 = (K2) list.get(1);
    final K3 value3 = (K3) list.get(2);

    return new KeyTriple<>(value1, value2, value3);
  }

  @Override
  public Object get(int index) {
    checkElementIndex(index, size());
    switch (index) {
      case 0:
        return key1;
      case 1:
        return key2;
      case 2:
        return key3;
      default:
        throw new AssertionError("unexpected index " + index);
    }
  }

  @Override
  public int size() {
    return 3;
  }

  public K1 getKey1() {
    return key1;
  }

  public K2 getKey2() {
    return key2;
  }

  public K3 getKey3() {
    return key3;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof KeyTriple)) {
      return false;
    }

    final KeyTriple<?, ?, ?> keyTriple = (KeyTriple<?, ?, ?>) o;

    return KeyUtils.equals(key1, keyTriple.key1)
        && KeyUtils.equals(key2, keyTriple.key2)
        && KeyUtils.equals(key3, keyTriple.key3);
  }

  @Override
  public int hashCode() {
    return KeyUtils.hash(key1, key2, key3);
  }

  @Override
  public String toString() {
    return "KeyTriple{ key1="
        + KeyUtils.toString(key1)
        + ", key2="
        + KeyUtils.toString(key2)
        + ", key3="
        + KeyUtils.toString(key3)
        + '}';
  }

  private static void checkValueValidity(boolean operation, List<Object> list) {
    checkArgument(
        operation,
        "any following components must not have a value if preceding components are null: %s",
        list);
  }
}
