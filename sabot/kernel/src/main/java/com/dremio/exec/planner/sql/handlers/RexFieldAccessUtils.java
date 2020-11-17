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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.ProjectRel;
import com.google.common.collect.ImmutableList;

/**
 * Utility Class for RexFieldAccess
 */
public final class RexFieldAccessUtils {
  public static final SqlSpecialOperator STRUCTURED_WRAPPER = new SqlSpecialOperator("STRUCTURED_WRAPPER", SqlKind.OTHER);

  public static RelNode wrap(RelNode rel) {
    return rel.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof Project) {
          return wrapProject((Project) other, ((Project) other).getInput().accept(this), false);
        }
        if (other instanceof Filter) {
          return wrapFilter((Filter) other, ((Filter) other).getInput().accept(this), false);
        }
        return super.visit(other);
      }
    });
  }

  public static RelNode unwrap(RelNode rel) {
    return rel.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof Project) {
          return unwrapProject((Project) other, ((Project) other).getInput().accept(this), false);
        }
        if (other instanceof Filter) {
          return unwrapFilter((Filter) other, ((Filter) other).getInput().accept(this), false);
        }
        return super.visit(other);
      }

      @Override
      public RelNode visit(LogicalProject project) {
        return unwrapProject((Project) project, ((Project) project).getInput().accept(this), true);
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        return unwrapFilter((Filter) filter, ((Filter) filter).getInput().accept(this), true);
      }
    });
  }

  public static Project wrapProject(Project project, RelNode input, boolean crel) {
    final StructuredReferenceWrapper wrapper = new StructuredReferenceWrapper(project.getCluster().getRexBuilder(), true);
    List<RexNode> wrappedExpr = project.getChildExps().stream().map(expr -> expr.accept(wrapper)).collect(Collectors.toList());
    if (crel) {
      return LogicalProject.create(input, wrappedExpr, project.getRowType());
    }
    return ProjectRel.create(project.getCluster(), project.getTraitSet(), input, wrappedExpr, project.getRowType());
  }

  private static RelNode unwrapProject(Project project, RelNode input, boolean crel) {
    final StructuredReferenceWrapper unwrapper = new StructuredReferenceWrapper(project.getCluster().getRexBuilder(), false);
    List<RexNode> unwrappedExpr = project.getChildExps().stream().map(expr -> expr.accept(unwrapper)).collect(Collectors.toList());
    if (crel) {
      return LogicalProject.create(input, unwrappedExpr, project.getRowType());
    }
    return ProjectRel.create(project.getCluster(), project.getTraitSet(), input, unwrappedExpr, project.getRowType());
  }

  private static Filter wrapFilter(Filter filter, RelNode input, boolean crel) {
    final StructuredReferenceWrapper wrapper = new StructuredReferenceWrapper(filter.getCluster().getRexBuilder(), true);
    RexNode wrappedCondition = filter.getCondition().accept(wrapper);
    if (crel) {
      return LogicalFilter.create(input, wrappedCondition);
    }
    return FilterRel.create(input, wrappedCondition);
  }

  private static RelNode unwrapFilter(Filter filter, RelNode input, boolean crel) {
    final StructuredReferenceWrapper unwrapper = new StructuredReferenceWrapper(filter.getCluster().getRexBuilder(), false);
    RexNode unwrappedCondition = filter.getCondition().accept(unwrapper);
    if (crel) {
      return LogicalFilter.create(input, unwrappedCondition);
    }
    return FilterRel.create(input, unwrappedCondition);
  }

  /**
   *
   */
  private static final class StructuredReferenceWrapper extends RexShuttle {

    private RexBuilder builder;
    private boolean wrap;

    StructuredReferenceWrapper(RexBuilder builder, boolean wrap) {
      this.builder = builder;
      this.wrap = wrap;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess ref) {
      if(wrap) {
        return builder.makeCall(ref.getType(), STRUCTURED_WRAPPER, ImmutableList.of(ref));
      }
      return super.visitFieldAccess(ref);
    }

    @Override
    public RexNode visitCall(RexCall rexCall) {
      if (!wrap) {
        if(rexCall.getOperator().equals(STRUCTURED_WRAPPER)) {
          return rexCall.getOperands().get(0);
        }
      }
      return super.visitCall(rexCall);
    }
  }

  private RexFieldAccessUtils() {}
}
