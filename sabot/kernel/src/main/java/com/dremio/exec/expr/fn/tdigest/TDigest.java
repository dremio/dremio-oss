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
package com.dremio.exec.expr.fn.tdigest;

import static org.apache.calcite.sql.type.OperandTypes.family;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import com.google.common.collect.ImmutableList;

public class TDigest {

  public static class SqlTDigestMergeAggFunction extends SqlAggFunction {
    private final RelDataType type;

    public SqlTDigestMergeAggFunction(RelDataType type) {
      super("TDIGEST_MERGE",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0, // use the inferred return type of SqlCountAggFunction
        null,
        OperandTypes.BINARY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false);

      this.type = type;
    }

    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
      return ImmutableList.of(type);
    }

    public RelDataType getType() {
      return type;
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return type;
    }

  }

  public static final class SqlTDigestAggFunction extends SqlAggFunction {
    private final RelDataType type;

    public SqlTDigestAggFunction(RelDataType type) {
      super("TDIGEST",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.family(SqlTypeFamily.NUMERIC),
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false);

      this.type = type;
    }

    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
      return ImmutableList.of(type);
    }

    public RelDataType getType() {
      return type;
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return type;
    }

  }

  public static final class SqlApproximatePercentileFunction extends SqlAggFunction {
    public SqlApproximatePercentileFunction() {
      super("APPROX_PERCENTILE",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
        SqlFunctionCategory.SYSTEM,
        false,
        false);
    }

  }

  public static final class SqlTDigestQuantileFunction extends SqlFunction {
    public SqlTDigestQuantileFunction() {
      super("TDIGEST_QUANTILE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        family(SqlTypeFamily.NUMERIC, SqlTypeFamily.BINARY),
        SqlFunctionCategory.SYSTEM
      );
    }

  }
}
