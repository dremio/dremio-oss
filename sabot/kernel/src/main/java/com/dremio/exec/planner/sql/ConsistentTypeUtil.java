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
package com.dremio.exec.planner.sql;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Utility class that contains methods for coercing operands to consistent type
 */
public class ConsistentTypeUtil {

  private static final int MAX_PRECISION = 38;

  public static boolean allExactNumeric(List<RelDataType> types) {
    return types.stream().allMatch(SqlTypeUtil::isExactNumeric);
  }

  public static boolean anyDecimal(List<RelDataType> types) {
    return types.stream().anyMatch(t -> t.getSqlTypeName() == SqlTypeName.DECIMAL);
  }

  public static RelDataType consistentDecimalType(RelDataTypeFactory factory, List<RelDataType> type) {
    final int scale = type.stream().mapToInt(RelDataType::getScale).max().orElse(0);
    final int maxPrecision = type.stream().mapToInt(RelDataType::getPrecision).max().orElse(0);

    final int precision = Math.min(maxPrecision, MAX_PRECISION);

    return factory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
  }
}
