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
package com.dremio.exec.planner.sql.convertlet;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;

public abstract class RexSubqueryConvertlet implements RexNodeConvertlet {
  @Override
  public boolean matches(RexNode rexNode) {
    if (!(rexNode instanceof RexSubQuery)) {
      return false;
    }

    RexSubQuery rexSubQuery = (RexSubQuery) rexNode;
    return matchesSubquery(rexSubQuery);
  }

  public abstract boolean matchesSubquery(RexSubQuery rexSubQuery);

  @Override
  public RexNode convert(ConvertletContext cx, RexNode node) {
    return convertSubquery(cx, (RexSubQuery) node);
  }

  public abstract RexNode convertSubquery(ConvertletContext cx, RexSubQuery rexSubQuery);
}
