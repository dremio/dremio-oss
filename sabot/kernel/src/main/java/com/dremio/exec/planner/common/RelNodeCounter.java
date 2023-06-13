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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.JoinPrel;

/**
 Generic counter for counting the number of nodes of certain type
 in the logical plan. Primarily written for reporting / logging.
*/
public class RelNodeCounter extends StatelessRelShuttleImpl {
  private int nodesCount = 0;

  protected void increment(int value) {
    nodesCount += value;
  }

  public int getCount() {
    return nodesCount;
  }

  public static class LogicalJoinCounter extends RelNodeCounter {
    @Override
    public RelNode visit(LogicalJoin join) {
      super.increment(1);
      return super.visit(join);
    }
  }

  public static class JoinPrelCounter extends RelNodeCounter {
    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof JoinPrel) {
        super.increment(1);
      }
      return super.visit(other);
    }
  }
}
