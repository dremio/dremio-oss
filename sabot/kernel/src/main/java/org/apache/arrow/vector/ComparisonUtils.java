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

package org.apache.arrow.vector;

/**
 * A set of comparison functions for different types
 */
public class ComparisonUtils {
  public static int compareByteArrays(final byte[] o1, final byte[] o2) {
    if (o1 == o2) {
      return 0;
    }

    for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
      final int itemComparisonResult = Byte.compare(o1[i], o2[i]);
      if (itemComparisonResult != 0) {
        return itemComparisonResult;
      }
    }

    if (o1.length != o2.length) {
      return Integer.compare(o1.length, o2.length);
    }

    return 0;
  }
}
