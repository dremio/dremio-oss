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

import static com.dremio.exec.planner.sql.handlers.RexFieldAccessUtils.STRUCTURED_WRAPPER;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Pair;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.planner.common.MoreRelOptUtil.ContainsRexVisitor;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.PrelUtil;

/**
 *
 * Base class for logical and physical Project implemented in Dremio
 */
public abstract class ProjectRelBase extends Project {
  private final int nonSimpleFieldCount;
  private final int simpleFieldCount;
  private final boolean hasContains;

  protected ProjectRelBase(Convention convention,
                                RelOptCluster cluster,
                                RelTraitSet traits,
                                RelNode child,
                                List<? extends RexNode> exps,
                                RelDataType rowType) {
    super(cluster, traits, child, exps, rowType);
    assert getConvention() == convention;
    this.simpleFieldCount = getSimpleFieldCount();
    this.nonSimpleFieldCount = this.getRowType().getFieldCount() - simpleFieldCount;

    boolean foundContains = false;
    int i = 0;
    for (RexNode rex : this.getChildExps()) {
      if (ContainsRexVisitor.hasContainsCheckOrigin(this, rex, i)) {
        foundContains = true;
        break;
      }
      i++;
    }
    this.hasContains = foundContains;
  }

  protected static RelTraitSet adjustTraits(RelOptCluster cluster, RelNode input, List<? extends RexNode> exps, RelTraitSet traits) {
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    return traits.replaceIfs(
        RelCollationTraitDef.INSTANCE,
        () -> RelMdCollation.project(mq, input, exps)
    );
  }

  public boolean hasComplexFields() {
    return nonSimpleFieldCount > 0;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    // Elasticsearch does not support contains() in project (only in filter).
    if (hasContains) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    // cost is proportional to the number of rows and number of columns being projected
    double rowCount = relMetadataQuery.getRowCount(this);
    // cpu is proportional to the number of columns and row count.  For complex expressions, we also add
    // additional cost for those columns (multiply by DremioCost.PROJECT_CPU_COST).
    double cpuCost = (DremioCost.PROJECT_SIMPLE_CPU_COST * rowCount * simpleFieldCount) + (DremioCost.PROJECT_CPU_COST * (nonSimpleFieldCount > 0 ? rowCount : 0) * nonSimpleFieldCount);
    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, 0, 0);
  }

  private List<Pair<RexNode, String>> projects() {
    return Pair.zip(exps, getRowType().getFieldNames());
  }

  protected List<NamedExpression> getProjectExpressions(ParseContext context) {
    return RexToExpr.projectToExpr(context, projects(), getInput());
  }

  private int getSimpleFieldCount() {
    int cnt = 0;

    final ComplexFieldWithNamedSegmentIdentifier complexFieldIdentifer = new ComplexFieldWithNamedSegmentIdentifier();
    // SimpleField, either column name, or complex field reference with only named segment ==> no array segment
    // a, a.b.c are simple fields.
    // a[1].b.c, a.b[1], a.b.c[1] are not simple fields, since they all contain array segment.
    //  a + b, a * 10 + b, etc are not simple fields, since they are expressions.
    for (RexNode expr : this.getProjects()) {
      if ((expr instanceof RexInputRef)) {
        // Simple Field reference.
        cnt ++;
      } else if ((expr instanceof RexFieldAccess) && (((RexFieldAccess) expr).getReferenceExpr() instanceof RexInputRef)) {
        cnt ++;
      } else if (expr instanceof RexCall && expr.accept(complexFieldIdentifer)) {
        // Complex field with named segments only.
        cnt ++;
      }
    }
    return cnt;
  }

  private static class ComplexFieldWithNamedSegmentIdentifier extends RexVisitorImpl<Boolean> {
    protected ComplexFieldWithNamedSegmentIdentifier() {
      super(true);
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      return true;
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
      if (call.getOperator() == SqlStdOperatorTable.ITEM) {
        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);

        if (op0 instanceof RexInputRef &&
            op1 instanceof RexLiteral && ((RexLiteral) op1).getTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
          return true;
        } else if (op0 instanceof RexCall &&
            op1 instanceof RexLiteral && ((RexLiteral) op1).getTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
          return op0.accept(this);
        }
      } else if (call.getOperator().getName().equalsIgnoreCase(STRUCTURED_WRAPPER.getName())) {
        return call.getOperands().get(0).accept(this);
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

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    private boolean doUnknown(Object o) {
      return false;
    }
  }

}
