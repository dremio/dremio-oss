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
package com.dremio.exec.expr.fn.hll;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class HyperLogLog {

  public static final int HLL_VARBINARY_SIZE = 65536;
  public static final SqlAggFunction HLL = new SqlHllAggFunction();
  public static final SqlAggFunction HLL_MERGE = new SqlHllMergeAggFunction();
  public static final SqlAggFunction NDV = new SqlNdvAggFunction();
  public static final SqlFunction HLL_DECODE = new SqlHllDecodeOperator();

  public static class SqlHllDecodeOperator extends SqlFunction {
    public SqlHllDecodeOperator() {
      super(new SqlIdentifier("HLL_DECODE", SqlParserPos.ZERO), ReturnTypes.BIGINT, null, OperandTypes.BINARY, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof SqlHllDecodeOperator;
    }

    @Override
    public int hashCode() {
      return 3;
    }

  }

  public static class SqlHllAggFunction extends SqlAggFunction {
    public SqlHllAggFunction() {
      super("HLL",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY, HLL_VARBINARY_SIZE),
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false
      );
    }
  }

  public static class SqlHllMergeAggFunction extends SqlAggFunction {
    public SqlHllMergeAggFunction() {
      super("HLL_MERGE",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY, HLL_VARBINARY_SIZE),
        null,
        OperandTypes.BINARY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false
      );
    }
  }

  public static class SqlNdvAggFunction extends SqlAggFunction {
    public SqlNdvAggFunction() {
      super("NDV",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false
      );
    }
  }

}
