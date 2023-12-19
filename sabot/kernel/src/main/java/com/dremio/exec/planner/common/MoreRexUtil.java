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
package com.dremio.exec.planner.common;

import java.util.function.Function;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;

/**
 * Utility class on RexNodes
 */
public final class MoreRexUtil {
  private MoreRexUtil() {}

  public static boolean hasFunction(RexNode node, Function<SqlOperator, Boolean> predicate) {
    FunctionFinder finder = new FunctionFinder(predicate);
    node.accept(finder);
    return finder.found;
  }

  private static class FunctionFinder extends RexShuttle {
    private final Function<SqlOperator, Boolean> predicate;
    private boolean found;

    public FunctionFinder(Function<SqlOperator, Boolean> predicate) {
      this.predicate = predicate;
      this.found = false;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (!predicate.apply(call.getOperator())) {
        return super.visitCall(call);
      }

      found = true;
      return call; // Short circuit
    }
  }
}
