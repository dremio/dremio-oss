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
package com.dremio.service.reflection.analysis;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFamily;

import com.dremio.service.reflection.analysis.ReflectionAnalyzer.RField;

/**
 * Utility methods used to inspect sql types.
 */
public final class TypeUtils {
  private TypeUtils() {
  }

  public static boolean isText(final RField field) {
    final Optional<SqlTypeFamily> familyOpt = getSqlTypeFamily(field);
    if (!familyOpt.isPresent()) {
      return false;
    }

    final SqlTypeFamily family = familyOpt.get();
    return family == SqlTypeFamily.CHARACTER;
  }

  public static boolean isNumeric(final RField field) {
    final Optional<SqlTypeFamily> familyOpt = getSqlTypeFamily(field);
    if (!familyOpt.isPresent()) {
      return false;
    }

    final SqlTypeFamily family = familyOpt.get();
    return family == SqlTypeFamily.NUMERIC;
  }

  public static boolean isTemporal(final RField field) {
    final Optional<SqlTypeFamily> familyOpt = getSqlTypeFamily(field);
    if (!familyOpt.isPresent()) {
      return false;
    }

    final SqlTypeFamily family = familyOpt.get();
    switch (family) {
      case DATETIME:
      case TIMESTAMP:
      case DATE:
      case TIME:
        return true;
      default:
        return false;
    }
  }

  public static boolean isBoolean(final RField field) {
    final Optional<SqlTypeFamily> familyOpt = getSqlTypeFamily(field);
    if (!familyOpt.isPresent()) {
      return false;
    }
    final SqlTypeFamily family = familyOpt.get();
    return family == SqlTypeFamily.BOOLEAN;
  }

  public static boolean isComplex(final RField field) {
    final Optional<SqlTypeFamily> familyOpt = getSqlTypeFamily(field);
    if (!familyOpt.isPresent()) {
      return false;
    }
    final SqlTypeFamily family = familyOpt.get();
    return family == SqlTypeFamily.ANY;
  }

  public static Optional<SqlTypeFamily> getSqlTypeFamily(final RField field) {
    try {
      return Optional.of(SqlTypeFamily.valueOf(field.getTypeFamily()));
    } catch (final IllegalArgumentException ex) {
      return Optional.empty();
    }
  }

  public static RField fromCalciteField(final RelDataTypeField field) {
    return new RField()
        .setName(field.getName())
        .setTypeFamily(field.getType().getFamily().toString());
  }
}
