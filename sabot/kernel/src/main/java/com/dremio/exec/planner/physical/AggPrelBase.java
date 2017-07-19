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
package com.dremio.exec.planner.physical;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.planner.common.AggregateRelBase;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public abstract class AggPrelBase extends AggregateRelBase implements Prel {

  protected static enum OperatorPhase {PHASE_1of1, PHASE_1of2, PHASE_2of2};

  protected OperatorPhase operPhase = OperatorPhase.PHASE_1of1 ; // default phase
  protected List<NamedExpression> keys;
  protected List<NamedExpression> aggExprs;
  protected List<AggregateCall> phase2AggCallList = Lists.newArrayList();

  public AggPrelBase(RelOptCluster cluster,
                     RelTraitSet traits,
                     RelNode child,
                     boolean indicator,
                     ImmutableBitSet groupSet,
                     List<ImmutableBitSet> groupSets,
                     List<AggregateCall> aggCalls,
                     OperatorPhase phase) throws InvalidRelException {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    this.operPhase = phase;
    this.keys = RexToExpr.groupSetToExpr(child, groupSet);
    this.aggExprs = RexToExpr.aggsToExpr(getRowType(), child, groupSet, aggCalls);

    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      int aggExprOrdinal = groupSet.cardinality() + aggCall.i;
      if (getOperatorPhase() == OperatorPhase.PHASE_1of2) {
        if (aggCall.e.getAggregation().getName().equals("COUNT")) {
          // If we are doing a COUNT aggregate in Phase1of2, then in Phase2of2 we should SUM the COUNTs,
          SqlAggFunction sumAggFun = SqlStdOperatorTable.SUM0;
          AggregateCall newAggCall =
            AggregateCall.create(
              sumAggFun,
              aggCall.e.isDistinct(),
              Collections.singletonList(aggExprOrdinal),
              -1,
              aggCall.e.getType(),
              aggCall.e.getName());

          phase2AggCallList.add(newAggCall);
        } else if (aggCall.e.getAggregation().getName().equals("HLL")) {
          SqlAggFunction hllMergeFunction = new SqlHllMergeAggFunction();
          AggregateCall newAggCall =
            AggregateCall.create(
              hllMergeFunction,
              aggCall.e.isDistinct(),
              Collections.singletonList(aggExprOrdinal),
              -1,
              cluster.getTypeFactory().createTypeWithNullability(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true),
              aggCall.e.getName());

          phase2AggCallList.add(newAggCall);
        } else {
          AggregateCall newAggCall =
              AggregateCall.create(
                  aggCall.e.getAggregation(),
                  aggCall.e.isDistinct(),
                  Collections.singletonList(aggExprOrdinal),
                  -1,
                  aggCall.e.getType(),
                  aggCall.e.getName());

          phase2AggCallList.add(newAggCall);
        }
      }
    }
  }

  public OperatorPhase getOperatorPhase() {
    return operPhase;
  }

  public List<NamedExpression> getKeys() {
    return keys;
  }

  public List<NamedExpression> getAggExprs() {
    return aggExprs;
  }

  public List<AggregateCall> getPhase2AggCalls() {
    return phase2AggCallList;
  }

  public ImmutableBitSet getPhase2GroupSet() {
    return ImmutableBitSet.range(0, groupSet.cardinality());
  }


  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  public static class SqlHllAggFunction extends SqlAggFunction {

    public SqlHllAggFunction() {
      super("HLL",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.BINARY),
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false
      );
    }
  }

  public static class SqlHllMergeAggFunction extends SqlAggFunction {

    public SqlHllMergeAggFunction() {
      super("NDV_MERGE",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_FORCE_NULLABLE,
        null,
        OperandTypes.BINARY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false
      );
    }
  }
}
