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

import com.google.common.collect.Lists;
import java.util.List;

/** This class represents a branch in an if-expression */
class IfExprBranch {
  public static final List<IfExprBranch> EMPTY_LIST = Lists.newArrayList();

  private final ExpressionSplit ifCondition;
  private final boolean partOfThenExpr;

  IfExprBranch(ExpressionSplit condSplit, boolean partOfThenExpr) {
    this.ifCondition = condSplit;
    this.partOfThenExpr = partOfThenExpr;
  }

  ExpressionSplit getIfCondition() {
    return ifCondition;
  }

  boolean isPartOfThenExpr() {
    return partOfThenExpr;
  }
}
