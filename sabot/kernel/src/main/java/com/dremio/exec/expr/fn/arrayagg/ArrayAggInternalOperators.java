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

package com.dremio.exec.expr.fn.arrayagg;

import com.dremio.exec.planner.sql.DremioReturnTypes;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

public class ArrayAggInternalOperators {

  public static class Phase1ArrayAgg extends SqlAggFunction {
    public Phase1ArrayAgg() {
      super(
          "PHASE1_ARRAY_AGG",
          null,
          SqlKind.LISTAGG,
          DremioReturnTypes.TO_ARRAY,
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.FORBIDDEN);
    }
  }

  public static class Phase2ArrayAgg extends SqlAggFunction {
    public Phase2ArrayAgg() {
      super(
          "PHASE2_ARRAY_AGG",
          null,
          SqlKind.LISTAGG,
          ReturnTypes.ARG0,
          null,
          OperandTypes.ARRAY,
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.FORBIDDEN);
    }
  }
}
