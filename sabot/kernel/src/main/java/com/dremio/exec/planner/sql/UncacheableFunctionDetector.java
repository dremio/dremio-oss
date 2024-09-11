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
package com.dremio.exec.planner.sql;

import org.apache.calcite.sql.SqlOperator;

public final class UncacheableFunctionDetector {
  private UncacheableFunctionDetector() {}

  public static boolean isA(SqlOperator sqlOperator) {
    // Flatten Operator is marked as non-deterministic, so that reduce expression doesn't replace it
    // with a literal
    // This is due to a limitation with reduce rules and complex types but, we want it to be cached,
    // so we are making an
    // exception.
    // Eventually we are going to deprecate the flatten operator and will get rid of this.
    if (sqlOperator instanceof SqlFlattenOperator) {
      return false;
    }

    if (sqlOperator.isDynamicFunction()) {
      return true;
    }

    if (!sqlOperator.isDeterministic()) {
      return true;
    }

    return false;
  }
}
