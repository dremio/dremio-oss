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

import com.dremio.common.expression.FunctionCall;
import com.dremio.exec.planner.sql.OperatorTable;

public interface PluggableFunctionRegistry {

  /**
   * Register functions in given operator table. There are two methods to add operators.
   * One is addOperatorWithInference whose added operators will be used
   * when planner.type_inference.enable is set to true;
   * The other is addOperatorWithoutInference whose added operators will be used
   * when planner.type_inference.enable is set to false;
   * @param operatorTable
   * @param isDecimalV2Enabled
   */
  void register(OperatorTable operatorTable, boolean isDecimalV2Enabled);

  /**
   * If exists return the function implementation holder matching the given <code>functionCall</code> expression,
   * otherwise null.
   *
   * @param functionCall
   * @return
   */
  AbstractFunctionHolder getFunction(FunctionCall functionCall);
}
