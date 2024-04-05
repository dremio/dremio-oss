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
package com.dremio.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.util.SqlBasicVisitor;

/**
 * A visitor to check if the given SqlNodeCondition is tested as true or not. If the condition is
 * true, mark flag 'find' as true.
 */
class ExprFinder extends SqlBasicVisitor<Void> {
  private boolean find = false;
  private final SqlNodeCondition condition;

  public ExprFinder(SqlNodeCondition condition) {
    this.find = false;
    this.condition = condition;
  }

  public boolean find() {
    return this.find;
  }

  @Override
  public Void visit(SqlCall call) {
    if (this.condition.test(call)) {
      this.find = true;
    }
    return super.visit(call);
  }
}
