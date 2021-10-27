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
package com.dremio.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.LimitRel;
import com.dremio.exec.planner.logical.SortRel;

/**
 * DremioSortInJoinRemover removes a LogicalSort if it occurs in a LogicalJoin Subtree
 * In an In SubQuery, after expansion, we sometimes maintain sort in a Join Subtree when it is not needed
 * For example for query -
 *      SELECT o_custkey FROM  tpch."orders.parquet"
 *        WHERE o_custkey in (SELECT o_custkey FROM tpch."orders.parquet" ORDER BY o_custkey)
 *       Order By o_custkey
 * The generated plan is Join/Aggregate/Sort
 * Here the sort is not needed and is removed by this remover.
 */

public class DremioSortInJoinRemover {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioSortInJoinRemover.class);

  public static RelNode remove(RelNode root) {
    SortRemoverShuttle sortRemoverShuttle = new SortRemoverShuttle();
    return sortRemoverShuttle.visit(root);
  }

  /**
   * SortRemoverShuttle is a Visitor that removes a LogicalSort if it occurs in a LogicalJoin Subtree
   */
  private static class SortRemoverShuttle extends StatelessRelShuttleImpl {
    private boolean inJoin = false;

    // returns true if rel Contains a sort or an offset which is not null
    private boolean doesRelContainsSortOrOffset(RelNode rel){
      if(rel instanceof SortRel){
        return ( ((SortRel)rel).fetch != null || ((SortRel)rel).offset != null);
      } else if(rel instanceof LimitRel){
        return ( ((LimitRel)rel).fetch != null || ((LimitRel)rel).offset != null);
      }
      return false;
    }

    public RelNode visitSortRel(SortRel sort) {
      if (this.inJoin && !doesRelContainsSortOrOffset(sort)) {
        RelNode output = super.visitChildren(sort);
        return visit(output.getInput(0));
      }
      this.inJoin = false;
      return super.visitChildren(sort);
    }

    public RelNode visitJoinRel(JoinRel join) {
      this.inJoin = true;
      RelNode output = super.visitChildren(join);
      this.inJoin = false;
      return output;
    }

    @Override
    public RelNode visit(RelNode other) {
      if(other instanceof JoinRel ){
        return visitJoinRel((JoinRel) other);
      }else if (other instanceof SortRel){
        return visitSortRel((SortRel) other);
      } else if (other instanceof LimitRel) {
        if(doesRelContainsSortOrOffset(other)){
          // do not remove sort in this subtree unless there is another Join below
          this.inJoin = false;
        }
      }
      return super.visit(other);
    }

  }

}
