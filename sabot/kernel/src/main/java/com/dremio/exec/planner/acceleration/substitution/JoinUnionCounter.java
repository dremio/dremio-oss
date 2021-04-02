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
package com.dremio.exec.planner.acceleration.substitution;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.MultiJoin;

import com.dremio.exec.planner.StatelessRelShuttleImpl;


/**
 * Counts the number of unions and joins in the tree starting at the input root
 */
public class JoinUnionCounter extends StatelessRelShuttleImpl {

  private int numJoins = 0;
  private int numUnions = 0;

  public int numJoins() {
    return numJoins;
  }

  public int numUnions() {
    return numUnions;
  }

  public int numUnionsAndJoins() {
    return numJoins() + numUnions();
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    numUnions++;
    return  super.visit(union);
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    numJoins++;
    return  super.visit(join);
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof MultiJoin) {
      if (other.getInputs().size() <= 1) {
        throw new UnsupportedOperationException();
      }
      numJoins += (other.getInputs().size() - 1);
    }
    return super.visit(other);
  }

}
