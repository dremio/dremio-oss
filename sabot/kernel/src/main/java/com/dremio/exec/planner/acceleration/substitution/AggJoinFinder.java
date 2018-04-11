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
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.MultiJoin;

import com.dremio.exec.planner.RoutingShuttle;

/**
 * AggJoinFinder finds join in the tree.
 * Also, it finds aggregate on top of join.
 */
class AggJoinFinder extends RoutingShuttle {

  private State state = State.OUT;

  private static enum State {
    /**
     * Not in a candidate subtree.
     */
    OUT,

    /**
     * Directly below an aggregate
     */
    AGG,

    /**
     * Directly below an aggregate below a project.
     */
    AGG_PROJECT,

    AGG_JOIN,
    AGG_PROJECT_JOIN
  }

  private boolean foundAggOnJoin = false;

  public boolean isFoundAggOnJoin() {
    return foundAggOnJoin;
  }

  public RelNode visit(LogicalAggregate aggregate) {
    toState(aggregate, State.AGG);
    return visitChildren(aggregate);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    if(state == State.AGG) {
      toState(project, State.AGG_PROJECT);
    } else if(state == State.AGG_PROJECT) {
      // treat one or many projects the same.
    } else {
      toState(project, State.OUT);
    }
    return visitChildren(project);
  }

  @Override
  public RelNode visit(RelNode other) {
    if(other instanceof MultiJoin) {
      return visitJoin(other);
    }
    toState(other, State.OUT);
    return super.visit(other);
  }

  private RelNode visitJoin(RelNode join) {
    if (state == State.AGG) {
      toState(join, State.AGG_JOIN);
      foundAggOnJoin = true;
      return join; // No need to recurse any further.  We found our first completion state.
    } else if(state == State.AGG_JOIN) {
      toState(join, State.AGG_PROJECT_JOIN);
      foundAggOnJoin = true;
      return join; // No need to recurse any further.  We found our first completion state.
    } else {
      state = State.OUT;
    }
    return visitChildren(join);
  }

  public RelNode visit(LogicalJoin join) {
    return visitJoin(join);
  }

  private void toState(RelNode val, State state) {
    // System.out.println(this.state.name() + " => " + state.name() + " " + val.getClass().getSimpleName());
    this.state = state;
  }
}
