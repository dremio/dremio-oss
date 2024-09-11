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
package com.dremio.exec.planner.sql;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.RelDataTypeEqualityComparer;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * If we have a query plan like:
 *
 * <p>LogicalProject(DEPTNO=[$0], names=[$SCALAR_QUERY($cor0, { LogicalAggregate(group=[{}],
 * EXPR$0=[ARRAY_AGG($0)]) LogicalProject(ENAME=[$1]) LogicalFilter(condition=[AND(=($6,
 * $cor0.DEPTNO), =($2, 'SALESMAN'))]) LogicalSort(sort0=[$1], dir0=[ASC])
 * LogicalProject(EMPNO=[$0], ..., COMM=[$7]) ScanCrel(table=[cp.scott."EMP.json"],
 * columns=[`EMPNO`, ..., `COMM`], splits=[1]) })]) LogicalAggregate(group=[{0}])
 * LogicalProject(DEPTNO=[$6]) ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, ..., `COMM`],
 * splits=[1])
 *
 * <p>Then we need to correct the reldatatype of cor0.DEPTNO, since the correlated variable is just
 * a single column instead of the whole table
 */
public class RexCorrelVariableSchemaFixer extends StatelessRelShuttleImpl {
  private static final RexCorrelVariableSchemaFixer INSTANCE = new RexCorrelVariableSchemaFixer();

  private RexCorrelVariableSchemaFixer() {}

  public static RelNode fixSchema(RelNode relNode) {
    return relNode.accept(INSTANCE);
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    RelNode left = correlate.getLeft();
    RelNode right = correlate.getRight();

    // Correct the correlate if needed
    RelDataType correlateVariableSchema = left.getRowType();
    CorrelateVariableFixer rexFixer =
        new CorrelateVariableFixer(correlate.getCorrelationId(), correlateVariableSchema);
    RelShuttle relFixer = new RexShuttleRelShuttle(rexFixer);
    right = right.accept(relFixer);

    if (rexFixer.getCorrelateVariableReplaced().isEmpty()) {
      return super.visit(correlate);
    }

    // Calculate the new required columns
    RexCorrelVariable replacedCorrelateVariable = rexFixer.getCorrelateVariableReplaced().get();
    RelDataType oldCorrelateVariableSchema = replacedCorrelateVariable.getType();
    ImmutableBitSet originalRequiredColumns = correlate.getRequiredColumns();
    Set<Integer> newRequiredColumns = new HashSet<>();
    for (int originalRequiredColumn : originalRequiredColumns.asList()) {
      String fieldName =
          oldCorrelateVariableSchema.getFieldList().get(originalRequiredColumn).getName();
      Optional<RelDataTypeField> optionalNewField =
          correlateVariableSchema.getFieldList().stream()
              .filter(field -> field.getName().equals(fieldName))
              .findFirst();
      assert optionalNewField.isPresent();
      if (optionalNewField.isEmpty()) {
        // We have a bug, so just go with the original query plan to avoid a regression.
        return super.visit(correlate);
      }

      RelDataTypeField newField = optionalNewField.get();
      newRequiredColumns.add(newField.getIndex());
    }

    ImmutableBitSet newRequiredColumnsBitSet = ImmutableBitSet.of(newRequiredColumns);
    Correlate newCorrelate =
        correlate.copy(
            correlate.getTraitSet(),
            left,
            right,
            correlate.getCorrelationId(),
            newRequiredColumnsBitSet,
            correlate.getJoinType());
    // Need to recurse for nested correlates
    return super.visit(newCorrelate);
  }

  private static final class CorrelateVariableFixer extends RexShuttle {
    private final CorrelationId targetCorrelationId;
    private final RelDataType newDataType;
    private Optional<RexCorrelVariable> correlateVariableReplaced;

    public CorrelateVariableFixer(CorrelationId targetCorrelationId, RelDataType newDataType) {
      this.targetCorrelationId = targetCorrelationId;
      this.newDataType = newDataType;
      this.correlateVariableReplaced = Optional.empty();
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess rexFieldAccess) {
      if (!(rexFieldAccess.getReferenceExpr() instanceof RexCorrelVariable)) {
        return rexFieldAccess;
      }

      RexCorrelVariable rexCorrelVariable = (RexCorrelVariable) rexFieldAccess.getReferenceExpr();
      if (!rexCorrelVariable.id.equals(targetCorrelationId)) {
        return rexFieldAccess;
      }

      if (RelDataTypeEqualityComparer.areEqual(rexCorrelVariable.getType(), newDataType)) {
        return rexFieldAccess;
      }

      correlateVariableReplaced = Optional.of(rexCorrelVariable);

      RexBuilder rexBuilder = new RexBuilder(JavaTypeFactoryImpl.INSTANCE);
      RexNode correl = rexBuilder.makeCorrel(newDataType, rexCorrelVariable.id);
      return rexBuilder.makeFieldAccess(correl, rexFieldAccess.getField().getName(), true);
    }

    public Optional<RexCorrelVariable> getCorrelateVariableReplaced() {
      return correlateVariableReplaced;
    }
  }
}
