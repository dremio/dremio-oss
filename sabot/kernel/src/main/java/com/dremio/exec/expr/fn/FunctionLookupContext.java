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
import com.dremio.options.OptionManager;

public interface FunctionLookupContext {

  /**
   * Method returns the materialized Dremio function corresponding to the function call that
   * is passed in. Returns only if the function matches the call exactly.
   *
   * @param functionCall - Specifies function name and type of arguments
   * @param allowGandivaFunctions - If we should use gandiva functions in this materialization.
   * @return AbstractFunctionHolder
   */
  public AbstractFunctionHolder findExactFunction(FunctionCall functionCall, boolean
    allowGandivaFunctions);

  /**
   * Method returns the materialized Dremio function corresponding to the function call that
   * is passed in. Returns the function that best matches the input.
   *
   * @param functionCall - Specifies function name and type of arguments
   * @param allowGandivaFunctions - If we should use gandiva functions in this materialization.
   * @return AbstractFunctionHolder
   */
  public AbstractFunctionHolder findFunctionWithCast(FunctionCall functionCall, boolean
    allowGandivaFunctions);


  /**
   * Find function implementation for given <code>functionCall</code> in non-Dremio function registries such as Hive UDF
   * registry.
   *
   * Note: Order of searching is same as order of {@link com.dremio.exec.expr.fn.PluggableFunctionRegistry}
   * implementations found on classpath.
   *
   * @param functionCall - Specifies function name and type of arguments
   * @return
   */
  public AbstractFunctionHolder findNonFunction(FunctionCall functionCall);

  public OptionManager getOptionManager();

  public boolean isDecimalV2Enabled();

}
