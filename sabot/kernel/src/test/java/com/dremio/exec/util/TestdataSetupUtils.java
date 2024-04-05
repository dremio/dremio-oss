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
package com.dremio.exec.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/** Simple utilites, that can be used from test classes */
public class TestdataSetupUtils {

  private TestdataSetupUtils() {
    // Not to be instantiated
  }

  /**
   * Prepares a test Map. Expects arguments in order of key1, val1, key2, val2,...
   *
   * @param map entries in order of key1, val1, key2, val2
   * @return
   */
  public static <K, V> Map<K, V> newHashMap(Object... e) {
    if (e.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Expects arguments in order of key1, val1, key2, val2. Number of arguments cannot be odd.");
    }

    final Map<K, V> returnMap = new LinkedHashMap<>();
    for (int i = 0; i < e.length; i += 2) {
      returnMap.put((K) e[i], (V) e[i + 1]);
    }
    return returnMap;
  }

  /**
   * Creates a list of random UUIDs to test with.
   *
   * @param size
   * @return
   */
  public static List<String> randomStrings(int size) {
    List<String> strings = new ArrayList<>(size);
    Random rand = new Random();
    for (int i = 0; i < size; i++) {
      if (rand.nextInt(4) == 0) {
        strings.add(null);
      } else {
        strings.add(UUID.randomUUID().toString());
      }
    }
    return strings;
  }
}
