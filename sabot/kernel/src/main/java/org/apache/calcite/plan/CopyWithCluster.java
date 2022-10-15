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
package org.apache.calcite.plan;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCopierWithOver;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import com.dremio.common.VM;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.RelOptTableWrapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * RelNode visitor that copies the nodes to the target cluster. Useful when replacing a materialized view
 * that was initially expanded using a planner A to a plan for planner B
 */
public class CopyWithCluster extends StatelessRelShuttleImpl {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopyWithCluster.class);

  private final Function<AggregateCall, AggregateCall> COPY_AGG_CALL = new Function<AggregateCall, AggregateCall>() {
    @Override
    public AggregateCall apply(AggregateCall input) {
      return copyOf(input);
    }
  };

  private final Function<RexNode, RexNode> COPY_REX_NODE = new Function<RexNode, RexNode>() {
    @Override
    public RexNode apply(RexNode input) {
      return copyOf(input);
    }
  };

  private final Function<RexLiteral, RexLiteral> COPY_REX_LITERAL = new Function<RexLiteral, RexLiteral>() {
    @Override
    public RexLiteral apply(RexLiteral input) {
      return (RexLiteral) copyOf(input);
    }
  };

  private final Function<RexLocalRef, RexLocalRef> COPY_REX_LOCAL_REF = new Function<RexLocalRef, RexLocalRef>() {
    @Nullable
    @Override
    public RexLocalRef apply(@Nullable RexLocalRef input) {
      // Note: RexCopier throws an UnsupportedException if we try to copy a RexLocalRef, so the assumption for now
      // is that we'll never hit this in practice
      return input == null ? null : (RexLocalRef) copyOf(input);
    }
  };

  private final Function<RelNode, RelNode> VISIT_REL_NODE = new Function<RelNode, RelNode>() {
    @Override
    public RelNode apply(RelNode input) {
      return input.accept(CopyWithCluster.this);
    }
  };

  private final RelOptCluster cluster;

  private List<String> notSupportedRels = Lists.newArrayList();

  public CopyWithCluster(RelOptCluster cluster) {
    this.cluster = cluster;
  }

  private void notSupported(Object rel) {
    notSupportedRels.add(rel.getClass().getName());
  }

  /**
   * If assertions are enabled and any of the visited RelNodes was not copied, throw an UNSUPPORTED UserException
   */
  public void validate() {
    if (!notSupportedRels.isEmpty()) {
      final String msg = String.format("Following Rels cannot be copied: %s", notSupportedRels);
      if (VM.areAssertsEnabled()) {
        throw UserException.unsupportedError().message(msg).build(logger);
      } else {
        logger.warn(msg);
      }
    }
  }

  public RelOptCluster getCluster() {
    return cluster;
  }

  public RelTraitSet copyOf(RelTraitSet original) {
    return original;
  }

  public RelDataType copyOf(RelDataType type) {
    return cluster.getTypeFactory().copyType(type);
  }

  public RexNode copyOf(RexNode expr) {
    return expr == null ? null : expr.accept(new RexCopierWithOver(cluster.getRexBuilder()));
  }

  public List<RexNode> copyRexNodes(List<RexNode> exprs) {
    return Lists.transform(exprs, COPY_REX_NODE);
  }

  public RelOptTable copyOf(RelOptTable table) {
    if (table instanceof RelOptTableWrapper) {
      final RelOptTableWrapper wrapper = (RelOptTableWrapper) table;
      return new RelOptTableWrapper(
        wrapper.getQualifiedName(),
        copyOf(wrapper.getRelOptTable())
      );
    } else if (table instanceof RelOptTableImpl) {
      final RelOptTableImpl impl = (RelOptTableImpl) table;
      return impl.copy(copyOf(impl.getRowType())); // this won't copy the RelOptSchema
    }
    notSupported(table);
    return table;
  }

  public List<AggregateCall> copyOf(List<AggregateCall> aggCalls) {
    return Lists.transform(aggCalls, COPY_AGG_CALL);
  }

  public RexProgram copyOf(RexProgram program) {
    return new RexProgram(
      copyOf(program.getInputRowType()),
      copyRexNodes(program.getExprList()),
      Lists.transform(program.getProjectList(), COPY_REX_LOCAL_REF),
      (RexLocalRef) copyOf(program.getCondition()),
      copyOf(program.getOutputRowType())
    );
  }

  public ImmutableList<ImmutableList<RexLiteral>> copyOf(ImmutableList<ImmutableList<RexLiteral>> tuples) {
    ImmutableList.Builder<ImmutableList<RexLiteral>> copied = ImmutableList.builder();
    for (ImmutableList<RexLiteral> literals : tuples) {
      copied.add(ImmutableList.copyOf(Iterables.transform(literals, COPY_REX_LITERAL)));
    }
    return copied.build();
  }

  public List<RelNode> visitAll(List<RelNode> nodes) {
    return Lists.transform(nodes, VISIT_REL_NODE);
  }

  private AggregateCall copyOf(AggregateCall call) {
    return AggregateCall.create(
      call.getAggregation(), // doesn't look we need to copy this
      call.isDistinct(),
      call.getArgList(),
      call.filterArg,
      copyOf(call.getType()),
      call.getName());
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    final RelNode input = aggregate.getInput().accept(this);
    return new LogicalAggregate(
      cluster,
      copyOf(aggregate.getTraitSet()),
      input,
      aggregate.getGroupSet(),
      aggregate.getGroupSets(),
      copyOf(aggregate.getAggCallList())
    );
  }

  @Override
  public RelNode visit(LogicalValues values) {

    return new LogicalValues(
      cluster,
      copyOf(values.getTraitSet()),
      copyOf(values.getRowType()),
      copyOf(values.getTuples())
    );
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    final RelNode input = filter.getInput().accept(this);
    return new LogicalFilter(
      cluster,
      copyOf(filter.getTraitSet()),
      input,
      copyOf(filter.getCondition()),
      ImmutableSet.copyOf(filter.getVariablesSet())
    );
  }

  @Override
  public RelNode visit(LogicalProject project) {
    final RelNode input = project.getInput().accept(this);
    return new LogicalProject(
      cluster,
      copyOf(project.getTraitSet()),
      input,
      Lists.transform(project.getProjects(), COPY_REX_NODE),
      copyOf(project.getRowType())
    );
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    // to the best of my knowledge join.systemFieldList is always empty
    Preconditions.checkState(join.getSystemFieldList().isEmpty(), "join.systemFieldList is not empty!");

    final RelNode left = join.getLeft().accept(this);
    final RelNode right = join.getRight().accept(this);

    return new LogicalJoin(
      cluster,
      copyOf(join.getTraitSet()),
      left,
      right,
      copyOf(join.getCondition()),
      join.getVariablesSet(),
      join.getJoinType(),
      join.isSemiJoinDone(),
      ImmutableList.<RelDataTypeField>of()
    );
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    final RelNode left = correlate.getLeft().accept(this);
    final RelNode right = correlate.getRight().accept(this);

    return new LogicalCorrelate(
      cluster,
      copyOf(correlate.getTraitSet()),
      left,
      right,
      correlate.getCorrelationId(),
      correlate.getRequiredColumns(),
      correlate.getJoinType()
    );
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    return new LogicalUnion(
      cluster,
      copyOf(union.getTraitSet()),
      visitAll(union.getInputs()),
      union.all
    );
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    return new LogicalIntersect(
      cluster,
      copyOf(intersect.getTraitSet()),
      visitAll(intersect.getInputs()),
      intersect.all
    );
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    return new LogicalMinus(
      cluster,
      copyOf(minus.getTraitSet()),
      visitAll(minus.getInputs()),
      minus.all
    );
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    final RelNode input = sort.getInput().accept(this);
    return LogicalSort.create(
      input,
      sort.getCollation(),
      copyOf(sort.offset),
      copyOf(sort.fetch)
    );
  }

  @Override
  public RelNode visit(LogicalExchange exchange) {
    final RelNode input = exchange.getInput().accept(this);
    return LogicalExchange.create(input, exchange.getDistribution());
  }

  private RelNode copyOf(LogicalWindow window) {
    final RelNode input = window.getInput().accept(this);
    return new LogicalWindow(
      cluster,
      copyOf(window.getTraitSet()),
      input,
      Lists.transform(window.constants, COPY_REX_LITERAL),
      copyOf(window.getRowType()),
      window.groups
    );
  }

  private RelNode copyOf(LogicalTableScan rel) {
    return new LogicalTableScan(
      cluster,
      copyOf(rel.getTraitSet()),
      rel.getTable()
    );
  }

  private RelNode copyOf(LogicalTableFunctionScan rel) {
    return new LogicalTableFunctionScan(
      cluster,
      copyOf(rel.getTraitSet()),
      visitAll(rel.getInputs()),
      copyOf(rel.getCall()),
      rel.getElementType(),
      copyOf(rel.getRowType()),
      rel.getColumnMappings()
    );
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof CopyToCluster) {
      return ((CopyToCluster) other).copyWith(this);
    } else if (other instanceof LogicalWindow) {
      return copyOf((LogicalWindow) other);
    }

    notSupported(other);
    return super.visit(other);
  }

  @Override
  public RelNode visit(TableScan scan) {
    if (scan instanceof CopyToCluster) {
      return ((CopyToCluster) scan).copyWith(this);
    } else if (scan instanceof LogicalTableScan) {
      return copyOf((LogicalTableScan) scan);
    }
    notSupported(scan);
    return super.visit(scan);
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
    if (scan instanceof LogicalTableFunctionScan) {
      return copyOf((LogicalTableFunctionScan) scan);
    }
    notSupported(scan);
    return super.visit(scan);
  }

  public interface CopyToCluster {
    RelNode copyWith(CopyWithCluster copier);
  }
}
