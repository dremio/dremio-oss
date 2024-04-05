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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Rewrite numeric values in LogicalValues to have full fidelity. Depending on whether there are
 * nulls we will either serialize the value and add a cast or do a UNION + Project + Values
 *
 * <p>(details in the helper methods)
 */
public class ValuesRewriteShuttle extends StatelessRelShuttleImpl {

  private final RelBuilder relBuilder;

  private ValuesRewriteShuttle(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public static RelNode rewrite(RelNode relNode) {
    RelBuilder relBuilder =
        (RelBuilder) DremioRelFactories.CALCITE_LOGICAL_BUILDER.create(relNode.getCluster(), null);
    ValuesRewriteShuttle shuttle = new ValuesRewriteShuttle(relBuilder);
    return relNode.accept(shuttle);
  }

  @Override
  public RelNode visit(RelNode other) {
    if (!(other instanceof Collect)) {
      return super.visit(other);
    }

    // We want to minimize the number of tests we break, so we are only handling all numerics for
    // the array subquery tests.
    Collect collect = (Collect) other;
    if (!(collect.getInput() instanceof LogicalValues)) {
      return super.visit(other);
    }

    LogicalValues logicalValues = (LogicalValues) collect.getInput();
    RelNode rewrittenValues = rewrite(logicalValues, true);

    return collect.copy(collect.getTraitSet(), rewrittenValues);
  }

  @Override
  public RelNode visit(LogicalValues values) {
    return rewrite(values, false);
  }

  private RelNode rewrite(LogicalValues values, boolean allNumerics) {
    if (values.getTuples().isEmpty()) {
      return values;
    }

    Set<RelDataTypeField> numericColumns =
        values.getRowType().getFieldList().stream()
            .filter(
                field -> {
                  SqlTypeName sqlTypeName = field.getType().getSqlTypeName();
                  return allNumerics
                      ? SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)
                      : sqlTypeName == SqlTypeName.DECIMAL;
                })
            .collect(Collectors.toSet());

    if (numericColumns.isEmpty()) {
      return values;
    }

    if (!allNumerics) {
      // To match the semantics of the previous values rewrite shuttle
      // we need to rewrite all the numerics if at least one was a decimal
      return rewrite(values, true);
    }

    return rewriteUsingCastOnSerializedValues(relBuilder, numericColumns, values);
  }

  /**
   * The Values Operator has a bug where it doesn't honor the SQL Type System. It turns out that it
   * uses a JSONReader and only honors the JSON types. This means that if we have a bunch of
   * numbers, then they all get casted to Doubles.
   *
   * <p>The solution is to Rewrite:
   *
   * <p>Values((a, b, c))
   *
   * <p>(where a and c are non-nullable numerics)
   *
   * <p>To: Project (CAST($0 AS NUMERIC), $1, CAST($2 AS NUMERIC)) Values((a.toString(), b,
   * c.toString()))
   *
   * <p>This is a temp solution until the Values operator gets fixed.
   */
  private static RelNode rewriteUsingCastOnSerializedValues(
      RelBuilder relBuilder, Set<RelDataTypeField> numericColumns, LogicalValues values) {
    // We need serialize all the specified columns
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    Set<Integer> columnsToInterop =
        numericColumns.stream().map(RelDataTypeField::getIndex).collect(Collectors.toSet());
    List<List<RexLiteral>> serializedValuesTuples =
        values.getTuples().stream()
            .map(
                row -> {
                  List<RexLiteral> serializedLiterals = new ArrayList<>();
                  for (int i = 0; i < row.size(); i++) {
                    RexLiteral column = row.get(i);
                    if (!columnsToInterop.contains(i)) {
                      serializedLiterals.add(column);
                    } else {
                      RexLiteral serializedColumn;
                      if (column.isNull()) {
                        serializedColumn =
                            rexBuilder.makeNullLiteral(
                                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));
                      } else {
                        serializedColumn =
                            rexBuilder.makeLiteral(RexLiteral.value(column).toString());
                      }

                      serializedLiterals.add(serializedColumn);
                    }
                  }

                  return serializedLiterals;
                })
            .collect(Collectors.toList());

    // Adjust the Reldatatype for the now serialized columns
    RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = rexBuilder.getTypeFactory().builder();
    RelDataType stringType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    RelDataType nullableStringType =
        rexBuilder.getTypeFactory().createTypeWithNullability(stringType, true);
    for (int i = 0; i < values.getRowType().getFieldCount(); i++) {
      RelDataTypeField relDataTypeField = values.getRowType().getFieldList().get(i);
      if (!columnsToInterop.contains(i)) {
        fieldInfoBuilder.add(relDataTypeField);
      } else {
        if (relDataTypeField.getType().isNullable()) {
          fieldInfoBuilder.add(relDataTypeField.getName(), nullableStringType);
        } else {
          fieldInfoBuilder.add(relDataTypeField.getName(), stringType);
        }
      }
    }

    RelDataType serializedRelDataType = fieldInfoBuilder.build();
    RelNode serializedValues =
        relBuilder.values(serializedValuesTuples, serializedRelDataType).build();

    // Add the needed casting rex node
    List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < serializedValues.getRowType().getFieldCount(); i++) {
      RexNode project = rexBuilder.makeInputRef(serializedValues, i);
      if (columnsToInterop.contains(i)) {
        RelDataType relDataType = values.getRowType().getFieldList().get(i).getType();
        RexNode castCall = rexBuilder.makeCast(relDataType, project);
        if (relDataType.isNullable()) {
          RexNode isNull = rexBuilder.makeCall(IS_NULL, project);
          RexNode nullLiteral = rexBuilder.makeNullLiteral(relDataType);
          castCall = rexBuilder.makeCall(CASE, isNull, nullLiteral, castCall);
        }

        project = castCall;
      }

      projects.add(project);
    }

    return relBuilder
        .push(serializedValues)
        .project(projects, values.getRowType().getFieldNames())
        .build();
  }
}
