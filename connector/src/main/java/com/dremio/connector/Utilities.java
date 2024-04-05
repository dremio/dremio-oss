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
package com.dremio.connector;

import java.util.List;
import java.util.Objects;

/** Contains a set of utility methods. */
public final class Utilities {

  /**
   * Throws if the given list is null or empty.
   *
   * @param list list
   * @param <T> type
   * @return given list
   */
  public static <T> List<T> checkNotNullOrEmpty(List<T> list) {
    Objects.requireNonNull(list);
    if (list.isEmpty()) {
      throw new IllegalArgumentException();
    }

    return list;
  }

  /**
   * Returns the last element in the list.
   *
   * @param list list
   * @param <T> type
   * @return last element
   */
  public static <T> T last(List<T> list) {
    return list.get(list.size() - 1);
  }

  // prevent instantiation
  private Utilities() {}
}
