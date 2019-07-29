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
package com.dremio.common.expression;

import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.types.TypeProtos;
import com.google.common.collect.ImmutableMap;

class CalciteTypeMaps {

  /**
   * Kept outside CompleteType to avoid inclusion in JDBC driver.
   */
  static final ImmutableMap<TypeProtos.MinorType, SqlTypeName> MINOR_TO_CALCITE_TYPE_MAPPING = ImmutableMap.<TypeProtos.MinorType, SqlTypeName> builder()
      .put(TypeProtos.MinorType.INT, SqlTypeName.INTEGER)
      .put(TypeProtos.MinorType.BIGINT, SqlTypeName.BIGINT)
      .put(TypeProtos.MinorType.FLOAT4, SqlTypeName.FLOAT)
      .put(TypeProtos.MinorType.FLOAT8, SqlTypeName.DOUBLE)
      .put(TypeProtos.MinorType.VARCHAR, SqlTypeName.VARCHAR)
      .put(TypeProtos.MinorType.BIT, SqlTypeName.BOOLEAN)
      .put(TypeProtos.MinorType.DATE, SqlTypeName.DATE)
      .put(TypeProtos.MinorType.DECIMAL, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL9, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL18, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL28SPARSE, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL38SPARSE, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.TIME, SqlTypeName.TIME)
      .put(TypeProtos.MinorType.TIMESTAMP, SqlTypeName.TIMESTAMP)
      .put(TypeProtos.MinorType.VARBINARY, SqlTypeName.VARBINARY)
      .put(TypeProtos.MinorType.INTERVALYEAR, SqlTypeName.INTERVAL_YEAR_MONTH)
      .put(TypeProtos.MinorType.INTERVALDAY, SqlTypeName.INTERVAL_DAY_SECOND)
      .put(TypeProtos.MinorType.STRUCT, SqlTypeName.MAP)
      .put(TypeProtos.MinorType.LIST, SqlTypeName.ARRAY)
      .put(TypeProtos.MinorType.LATE, SqlTypeName.ANY)

      // These are defined in the Dremio type system but have been turned off for now
      // .put(TypeProtos.MinorType.TINYINT, SqlTypeName.TINYINT)
      // .put(TypeProtos.MinorType.SMALLINT, SqlTypeName.SMALLINT)
      // Calcite types currently not supported by Dremio, nor defined in the Dremio type list:
      //      - CHAR, SYMBOL, MULTISET, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST
      .build();

}
