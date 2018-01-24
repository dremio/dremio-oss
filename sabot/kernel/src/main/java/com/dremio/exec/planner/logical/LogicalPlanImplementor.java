/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.Set;

import org.apache.calcite.rel.RelNode;

import com.dremio.common.logical.LogicalPlan;
import com.dremio.common.logical.LogicalPlanBuilder;
import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.common.logical.PlanProperties.PlanType;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.common.logical.data.visitors.AbstractLogicalVisitor;
import com.google.common.collect.Sets;

/**
 * Context for converting a tree of {@link Rel} nodes into a Dremio logical plan.
 */
public class LogicalPlanImplementor {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogicalPlanImplementor.class);

  private Set<String> storageEngineNames = Sets.newHashSet();
  private LogicalPlanBuilder planBuilder = new LogicalPlanBuilder();
  private LogicalPlan plan;
  private final ParseContext context;


  public LogicalPlanImplementor(ParseContext context, ResultMode mode) {
    planBuilder.planProperties(PlanType.LOGICAL, 1, LogicalPlanImplementor.class.getName(), "", mode);
    this.context = context;
  }

  public ParseContext getContext(){
    return context;
  }

  public void go(Rel root) {
    LogicalOperator rootLOP = root.implement(this);
    rootLOP.accept(new AddOpsVisitor(), null);
  }

  public LogicalPlan getPlan(){
    if(plan == null){
      plan = planBuilder.build();
      planBuilder = null;
    }
    return plan;
  }

  public LogicalOperator visitChild(Rel parent, int ordinal, RelNode child) {
    return ((Rel) child).implement(this);
  }

  private class AddOpsVisitor extends AbstractLogicalVisitor<Void, Void, RuntimeException> {
    @Override
    public Void visitOp(LogicalOperator op, Void value) throws RuntimeException {
      planBuilder.addLogicalOperator(op);
      for(LogicalOperator o : op){
        o.accept(this, null);
      }
      return null;
    }
  }

}
