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
package com.dremio.exec.planner.logical;

import org.apache.calcite.rel.type.RelDataType;

/**
 * Util class for determining if two RelDataTypes are equal under certain conditions. For example,
 * we can compare them with and without the nullability and precision.
 */
public final class RelDataTypeEqualityUtil {
  private RelDataTypeEqualityUtil() {}

  public static boolean areEquals(
      RelDataType first,
      RelDataType second,
      boolean considerPrecision,
      boolean considerNullability) {
    if ((first == null) && (second == null)) {
      return true;
    }

    if ((first == null) || second == null) {
      return false;
    }

    boolean areEquals = (first.getSqlTypeName() == second.getSqlTypeName());
    if (considerPrecision) {
      areEquals &= (first.getPrecision() == second.getPrecision());
    }

    if (considerNullability) {
      areEquals &= (first.isNullable() == second.isNullable());
    }

    if (!areEquals) {
      return false;
    }

    return areEquals(
        first.getComponentType(),
        second.getComponentType(),
        considerPrecision,
        considerNullability);
  }

  public static boolean areEquals(RelDataType first, RelDataType second) {
    return areEquals(first, second, true, true);
  }
}
