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

package com.dremio.exec.expr.fn;

import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import com.dremio.exec.planner.sql.SqlOperatorImpl;


public class GandivaOperator  {

  public static final boolean IS_DETERMINISTIC = true;
  public static final boolean IS_DYNAMIC = false;

  /**
   * Gets a default operator that is
   *
   * 1. deterministic i.e. same output for same input always
   * 2. not dynamic i.e. plans can be safely cached for this operator.
   * 3. is of syntax function i.e. always called with paranthesis even if no args
   *
   * @param name - name of the function
   * @param minParamCount - number of parameters to the function
   * @param maxParamCount - max number of paramters for a function with this name
   * @param returnTypeInference
   * @return
   */
  public static SqlOperatorImpl getSimpleFunction(String name, int minParamCount, int
    maxParamCount, SqlReturnTypeInference returnTypeInference) {

    return new SqlOperatorImpl(
      name,
      minParamCount,
      maxParamCount,
      IS_DETERMINISTIC,
      IS_DYNAMIC,
      returnTypeInference,
      SqlSyntax.FUNCTION);

  }
}
