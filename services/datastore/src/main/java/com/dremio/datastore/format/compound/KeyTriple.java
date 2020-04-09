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

/**
 * Compound Key representation for 3 keys.
 *
 * @param <K1> - The type of the first key.
 * @param <K2> - The type of the second key.
 * @param <K3> - The type of the third key.
 */
public final class KeyTriple<K1, K2, K3> implements CompoundKey {

  private final K1 key1;
  private final K2 key2;
  private final K3 key3;

  public KeyTriple(K1 key1, K2 key2, K3 key3) {
    this.key1 = key1;
    this.key2 = key2;
    this.key3 = key3;
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

    KeyTriple<?, ?, ?> keyTriple = (KeyTriple<?, ?, ?>) o;

    return KeyUtils.equals(key1, keyTriple.key1) &&
      KeyUtils.equals(key2, keyTriple.key2) &&
      KeyUtils.equals(key3, keyTriple.key3);
  }

  @Override
  public int hashCode() {
    return KeyUtils.hash(key1, key2, key3);
  }

  @Override
  public String toString() {
    return "KeyTriple{ key1=" + KeyUtils.toString(key1) +
      ", key2=" + KeyUtils.toString(key2) +
      ", key3=" + KeyUtils.toString(key3) +
      '}';
  }
}
