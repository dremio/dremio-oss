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
package com.dremio.common.util;

import java.util.Comparator;
import java.util.List;

/**
 * Common comparator implementations
 */
public class DremioComparator {
  public static final Comparator<List<String>> StringLexographicSizeThenReverseElementOrder =
    lexographicSizeThenReverseElementOrder(String::compareTo);
  private DremioComparator() {
  }

  /**
   * Compares list first on size then on reverse element order.  Primarily useful when an algorithm
   * cares only about speed and lists are likely to start with common elements.
   *
   * @param valueComparator
   * @param <VALUE>
   * @return
   */
  public static <VALUE> Comparator<List<VALUE>> lexographicSizeThenReverseElementOrder(
      Comparator<VALUE> valueComparator) {
    return (o1, o2) -> {
      if (o1 == o2) {
        return 0;
      }
      int sizeComp = Integer.compare(o1.size(), o2.size());
      if (sizeComp != 0) {
        return sizeComp;
      }
      for (int i = o1.size() - 1; i >= 0; i--) {
        VALUE v1 = o1.get(i);
        VALUE v2 = o2.get(i);
        int vDiff = valueComparator.compare(v1, v2);
        if (vDiff != 0) {
          return vDiff;
        }
      }
      return 0;
    };
  }
}
