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
package com.dremio.exec.planner.logical;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.sql.SqlFlattenOperator;
import com.dremio.service.Pointer;

public class FlattenVisitors {

  public static boolean hasFlatten(RexNode node){
    return node.accept(new FlattenFinder());
  }

  public static boolean hasFlatten(Project project){
    for(RexNode n : project.getChildExps()){
      if(n.accept(new FlattenFinder())){
        return true;
      }
    }
    return false;
  }

  public static int count(Iterable<RexNode> rexs) {
    FlattenCounter fc = new FlattenCounter();
    for(RexNode r : rexs){
      r.accept(fc);
    }
    return fc.getCount();
  }

  public static class FlattenFinder extends RexVisitorImpl<Boolean> {
    protected FlattenFinder() {
      super(true);
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      return false;
    }

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef localRef) {
      return doUnknown(localRef);
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
      return doUnknown(literal);
    }

    @Override
    public Boolean visitOver(RexOver over) {
      return doUnknown(over);
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return doUnknown(correlVariable);
    }

    @Override
    public Boolean visitCall(RexCall call) {
      if (call.getOperator() instanceof SqlFlattenOperator) {
        return true;
      }

      for (RexNode op : call.getOperands()) {
        if (op.accept(this)) {
          return true;
        }
      }

      return false;
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return doUnknown(dynamicParam);
    }

    @Override
    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return doUnknown(rangeRef);
    }

    private boolean doUnknown(Object o) {
      return false;
    }
  }


  public static class FlattenCounter extends RexVisitorImpl<Void> {
    private final Set<Integer> flattens = new HashSet<>();

    /**
     * Count the total number of flattens in Project node
     * @param project
     * @return the total number of flattens in the project
     */
    public static int count(Project project) {
      FlattenCounter c = new FlattenCounter();
      c.add(project);
      return c.getCount();
    }

    /**
     * Count the total number of flatten operators in a tree
     * @param relNode the root of the tree
     * @return the number of flattens
     */
    public static int countFlattensInPlan(RelNode relNode) {
      final Pointer<Integer> totalCount = new Pointer<>(0);
      relNode.accept(new RoutingShuttle() {
        @Override
        public RelNode visit(RelNode relNode) {
          RelNode rel = super.visit(relNode);
          if (relNode instanceof Project) {
            totalCount.value += FlattenCounter.count((Project) relNode);
          }
          return rel;
        }
      });
      return totalCount.value;
    }

    public FlattenCounter() {
      super(true);
    }

    public void add(Project project){
      for(RexNode e : project.getChildExps()){
        e.accept(this);
      }
    }

    public int getCount(){
      return flattens.size();
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      return null;
    }

    @Override
    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public Void visitLocalRef(RexLocalRef localRef) {
      return doUnknown(localRef);
    }

    @Override
    public Void visitLiteral(RexLiteral literal) {
      return doUnknown(literal);
    }

    @Override
    public Void visitOver(RexOver over) {
      return doUnknown(over);
    }

    @Override
    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
      return doUnknown(correlVariable);
    }

    @Override
    public Void visitCall(RexCall call) {
      if (call.getOperator() instanceof SqlFlattenOperator) {
        flattens.add(((SqlFlattenOperator)call.getOperator()).getIndex());
      }

      for (RexNode op : call.getOperands()) {
        op.accept(this);
      }

      return null;
    }

    @Override
    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
      return doUnknown(dynamicParam);
    }

    @Override
    public Void visitRangeRef(RexRangeRef rangeRef) {
      return doUnknown(rangeRef);
    }

    private Void doUnknown(Object o) {
      return null;
    }
  }

}
