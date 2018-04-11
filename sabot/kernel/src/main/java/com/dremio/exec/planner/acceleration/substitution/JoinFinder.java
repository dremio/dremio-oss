/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.acceleration.substitution;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import com.dremio.exec.planner.RoutingShuttle;

/**
 * AggJoinFinder finds join in the tree.
 * Also, it finds aggregate on top of join.
 */
class JoinFinder extends RoutingShuttle {

  private boolean foundJoin = false;

  public boolean isFoundJoin() {
    return foundJoin;
  }

  @Override
  public RelNode visit(RelNode other) {
    return super.visit(other);
  }

  public RelNode visit(LogicalJoin join) {
    foundJoin = true;
    return join; // No need to recurse any further.  We found our first completion state.
  }

}
