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
package com.dremio.exec.expr.fn.listagg;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.types.SqlTypeFactoryImpl;

public class ListAgg {

  public static class SqlLocalListAggFunction extends SqlAggFunction {
    public SqlLocalListAggFunction() {
      super("local_listagg",
        null,
        SqlKind.LISTAGG,
        ReturnTypes.explicit(
          SqlTypeFactoryImpl.INSTANCE.createTypeWithNullability(
            SqlTypeFactoryImpl.INSTANCE.createArrayType(
              SqlTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.VARCHAR), -1), true)),
        null,
        OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING),
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false
      );
    }
  }

  public static class SqlListAggMergeFunction extends SqlAggFunction {
    public SqlListAggMergeFunction() {
      super("listagg_merge",
        null,
        SqlKind.LISTAGG,
        ReturnTypes.explicit(
          SqlTypeFactoryImpl.INSTANCE.createTypeWithNullability(
            SqlTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.VARCHAR), true)),
        null,
        OperandTypes.or(OperandTypes.ARRAY, OperandTypes.STRING_STRING),
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false
      );
    }
  }

}
