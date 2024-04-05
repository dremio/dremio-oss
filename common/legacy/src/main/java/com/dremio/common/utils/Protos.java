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
package com.dremio.common.utils;

import java.util.Collections;
import java.util.List;

/** utilities to use with Protos generated code */
public final class Protos {

  private Protos() {}

  /**
   * empty list get read back as null
   *
   * @param list got from a protos getter
   * @return the list or an immutable empty list if null
   */
  public static <T> List<T> listNotNull(List<T> list) {
    if (list == null) {
      return Collections.emptyList();
    }
    return list;
  }

  /**
   * empty lists get read back as null So there is no distinction between null and empty
   *
   * @param list got from a protos getter
   * @return whether the list is null or empty
   */
  public static <T> boolean isEmpty(List<T> list) {
    return list == null || list.isEmpty();
  }

  /**
   * empty lists get read back as null So there is no distinction between null and empty
   *
   * @param list got from a protos getter
   * @return whether the list is null or empty
   */
  public static <T> boolean notEmpty(List<T> list) {
    return !isEmpty(list);
  }
}
