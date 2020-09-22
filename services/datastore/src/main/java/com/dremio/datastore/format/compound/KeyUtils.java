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

import java.util.Arrays;
import java.util.Objects;

/**
 * Utilities for working with values in CompoundKey classes.
 */
public final class KeyUtils {

  private KeyUtils() {
  }

  /**
   * Tests Objects for equality.
   *
   * @param left  one Object or byte[] to be tested for equality
   * @param right the other Object or byte[] to be tested for equality
   * @return <tt>true</tt> if the values of the two Objects are equal
   */
  public static boolean equals(Object left, Object right) {
    if (left instanceof byte[] && right instanceof byte[]) {
      return Arrays.equals((byte[]) left, (byte[]) right);
    }
    return Objects.equals(left, right);
  }

  /**
   * Returns the combined hashcode for all Objects..
   *
   * @param keys and array of byte[] or Objects.
   * @return the combined hashcode for all Objects..
   * @see Object#hashCode
   */
  public static int hash(Object... keys) {
    if (null == keys) {
      return 0;
    }

    int result = 1;
    for (Object key : keys) {
      result = 31 * result + hashCode(key);
    }
    return result;
  }

  /**
   * Returns a string representation of a byte[] or Object.
   *
   * @param key the byte[] or Object.
   * @return the string representation of the byte[] or Object.
   */
  public static String toString(Object key) {
    if (null == key) {
      return null;
    }

    return (key instanceof byte[] ? Arrays.toString((byte[]) key) : key.toString());
  }

  private static int hashCode(Object key) {
    if (null == key) {
      return 0;
    }

    return (key instanceof byte[] ? Arrays.hashCode((byte[]) key) : Objects.hashCode(key));
  }
}
