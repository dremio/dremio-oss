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
package com.dremio.exec.planner.serializer.logical;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PLogicalProject;
import com.google.common.collect.ImmutableList;

/**
 * Serde for LogicalProject
 */
public final class LogicalProjectSerde implements RelNodeSerde<LogicalProject, PLogicalProject> {
  @Override
  public PLogicalProject serialize(LogicalProject project, RelToProto s) {
    return PLogicalProject.newBuilder()
        .setInput(s.toProto(project.getInput()))
        .addAllName(project.getRowType().getFieldNames())
        .addAllExpr(project.getProjects().stream().map(s::toProto).collect(Collectors.toList()))
        .build();
  }

  @Override
  public LogicalProject deserialize(PLogicalProject node, RelFromProto s) {
    RelNode input = s.toRel(node.getInput());
    List<RexNode> projects = node
      .getExprList()
      .stream()
      .map(expr -> s.toRex(expr, input.getRowType()))
      .collect(Collectors.toList());
    return LogicalProject.create(input, ImmutableList.of(), projects, node.getNameList());
  }
}
