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

import com.dremio.exec.calcite.logical.FlattenCrel;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

/** Rule that converts a {@link Uncollect} to a Dremio "uncollect" (flatten) operation. */
public final class UncollectToFlattenConverter extends StatelessRelShuttleImpl {
  private static final UncollectToFlattenConverter INSTANCE = new UncollectToFlattenConverter();

  private UncollectToFlattenConverter() {}

  public static RelNode convert(RelNode input) {
    return input.accept(INSTANCE);
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof Uncollect) {
      return visitUncollect((Uncollect) other);
    }

    return super.visit(other);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    if (!(project.getInput() instanceof Uncollect)) {
      return super.visit(project);
    }

    Uncollect uncollect = (Uncollect) project.getInput();
    if (project.getProjects().size() != 1) {
      return super.visit(project);
    }

    RexNode exp = project.getProjects().get(0);
    if (!(exp instanceof RexInputRef)) {
      return super.visit(project);
    }

    String alias = project.getRowType().getFieldList().get(0).getName();
    Uncollect rewrittenUncollect =
        Uncollect.create(
            uncollect.getTraitSet(),
            uncollect.getInput(),
            uncollect.withOrdinality,
            ImmutableList.of(alias));

    return visitUncollect(rewrittenUncollect);
  }

  public RelNode visitUncollect(Uncollect uncollect) {
    RelNode rewrittenInput = uncollect.getInput().accept(this);
    RelNode flattenRelNode =
        FlattenCrel.create(
            rewrittenInput,
            ImmutableList.of(
                rewrittenInput.getCluster().getRexBuilder().makeInputRef(rewrittenInput, 0)),
            ImmutableList.of(uncollect.getRowType().getFieldList().get(0).getName()),
            0);

    if (uncollect.withOrdinality) {
      // We need to enrich the flatten result with the row number
      RexBuilder rexBuilder = flattenRelNode.getCluster().getRexBuilder();

      Window.RexWinAggCall rexWinAggCall =
          new Window.RexWinAggCall(
              SqlStdOperatorTable.ROW_NUMBER,
              rexBuilder
                  .getTypeFactory()
                  .createTypeWithNullability(
                      rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false),
              ImmutableList.of(),
              0,
              false);

      RelDataTypeFactory.FieldInfoBuilder windowRelDataTypeBuilder =
          rexBuilder.getTypeFactory().builder();
      for (RelDataTypeField field : flattenRelNode.getRowType().getFieldList()) {
        windowRelDataTypeBuilder.add(field);
      }

      RelDataType windowRelDataType =
          windowRelDataTypeBuilder.add("ORDINALITY", rexWinAggCall.getType()).build();

      Window.Group group =
          new Window.Group(
              ImmutableBitSet.of(),
              true,
              RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
              RexWindowBound.create(SqlWindow.createCurrentRow(SqlParserPos.ZERO), null),
              RelCollations.of(),
              ImmutableList.of(rexWinAggCall));

      flattenRelNode =
          new LogicalWindow(
              flattenRelNode.getCluster(),
              flattenRelNode.getTraitSet(),
              flattenRelNode,
              ImmutableList.of(),
              windowRelDataType,
              ImmutableList.of(group));
    }

    return flattenRelNode;
  }
}
