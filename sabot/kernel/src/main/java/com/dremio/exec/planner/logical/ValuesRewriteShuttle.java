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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.google.common.collect.ImmutableList;

/**
 * Rewrite decimal in LogicalValues as expressions in LogicalProject
 * If there is any decimal value, pull all the tuples into separate projects.
 * If there are more than 1 values, do a union of all those projects.
 * <pre>
 * For queries like:
 * select * from (values(1.23)), we will have:
 *
 * LogicalProject(EXPR$0=[$0])
 *    LogicalValues(tuples=[[{ 1.23 }]])
 *
 * Which should be re-written as:
 *
 * LogicalProject(EXPR$0=[$0])
 *    LogicalProject(EXPR$0=[1.23])
 *        LogicalValues(tuples=[[{ 0 }]])
 * </pre>
 */
public class ValuesRewriteShuttle extends StatelessRelShuttleImpl {

  @Override
  public RelNode visit(LogicalValues values) {
    boolean foundDecimal =
      values.getTuples().stream()
        .allMatch(tuple -> tuple.stream() // All the tuples must have decimal type.
          .anyMatch(rexNode -> rexNode.getType().getSqlTypeName() == SqlTypeName.DECIMAL));

    if (foundDecimal) {
      List<RelNode> projects = getProjects(values);
      LogicalProject project;
      if (projects.size() == 1) {
        project = (LogicalProject) projects.get(0);
      } else {
        // If there are more than one tuples, create a union here.
        LogicalUnion union = LogicalUnion.create(projects, true);

        List<RexInputRef> inputRefs = new ArrayList<>();
        for (RelDataTypeField field : union.getRowType().getFieldList()) {
          inputRefs.add(new RexInputRef(field.getIndex(), field.getType()));
        }
        project = LogicalProject.create(union, ImmutableList.of(), inputRefs, union.getRowType());
      }

      return project;
    }
    return values;
  }

  private List<RelNode> getProjects(LogicalValues values) {
    List<RelNode> projects = new ArrayList<>();
    LogicalValues oneRow = LogicalValues.createOneRow(values.getCluster());
    for (ImmutableList<RexLiteral> tuple : values.getTuples()) {
      LogicalProject valuesProject = LogicalProject.create(
        oneRow,
        ImmutableList.of(),
        tuple,
        values.getRowType());
      projects.add(valuesProject);
    }

    return projects;
  }
}
