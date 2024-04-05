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
package com.dremio.exec.expr;

import com.dremio.common.expression.LogicalExpression;

/**
 * Helper methods to aid in splitting expressions. Intended to be implemented by code generators
 * like the GandivaPushDownSieve
 */
public interface ExpressionSplitHelper {
  /**
   * Decides if the input sub-expression can be split at this point. Called by ExpressionSplitter to
   * handle cases where the expression can be handled by the code generator, but the expression's
   * output type cannot
   *
   * @param e Expression that needs to be split
   * @return true (default) if the expression can be split
   */
  public default boolean canSplitAt(LogicalExpression e) {
    return true;
  }
}
