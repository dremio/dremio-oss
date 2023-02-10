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
package com.dremio.service.functions.model;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.ARRAY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.MAP;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARBINARY;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import org.apache.calcite.sql.type.SqlTypeName;

public final class SqlTypeNameToParameterType {
  private SqlTypeNameToParameterType() {}

  public static ParameterType convert(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
    case ANY:
      return ParameterType.ANY;

    case BOOLEAN:
      return ParameterType.BOOLEAN;

    case BINARY:
    case VARBINARY:
      return ParameterType.BYTES;

    case FLOAT:
      return ParameterType.FLOAT;

    case DECIMAL:
      return ParameterType.DECIMAL;

    case DOUBLE:
      return ParameterType.DOUBLE;

    case INTEGER:
      return ParameterType.INT;

    case BIGINT:
      return ParameterType.BIGINT;

    case CHAR:
    case VARCHAR:
      return ParameterType.CHARACTERS;

    case DATE:
      return ParameterType.DATE;

    case TIME:
      return ParameterType.TIME;

    case TIMESTAMP:
      return ParameterType.TIMESTAMP;

    case ARRAY:
      return ParameterType.LIST;

    case MAP:
      return ParameterType.STRUCT;

    default:
      throw new UnsupportedOperationException("UNKNOWN KIND: " + sqlTypeName);
    }
  }
}
