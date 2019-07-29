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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.PrelUtil.ProjectPushInfo;
import com.google.common.collect.Lists;

public class PushProjectIntoScanRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new PushProjectIntoScanRule();

  private PushProjectIntoScanRule() {
    super(RelOptHelper.some(LogicalProject.class, RelOptHelper.any(ScanCrel.class)), "PushProjectIntoScanRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project proj = call.rel(0);
    final ScanCrel scan = call.rel(1);

    ProjectPushInfo columnInfo = PrelUtil.getColumns(scan.getRowType(), proj.getProjects());

    // get TableBase, either wrapped in RelOptTable, or TranslatableTable. TableBase table = scan.getTable().unwrap(TableBase.class);
    if (columnInfo == null || columnInfo.isStarQuery()) {
      return;
    }

    ScanCrel newScan = scan.cloneWithProject(columnInfo.columns);

    List<RexNode> newProjects = Lists.newArrayList();
    for (RexNode n : proj.getChildExps()) {
      newProjects.add(n.accept(columnInfo.getInputRewriter()));
    }

    final RelBuilder relBuilder = relBuilderFactory.create(proj.getCluster(), null);
    relBuilder.push(newScan);
    relBuilder.project(newProjects, proj.getRowType().getFieldNames());
    final RelNode newProj = relBuilder.build();

    if (newProj instanceof Project
        && ProjectRemoveRule.isTrivial((Project) newProj)
        && newScan.getRowType().getFullTypeString().equals(newProj.getRowType().getFullTypeString())) {
        call.transformTo(newScan);
    } else {
      if(newScan.getProjectedColumns().equals(scan.getProjectedColumns())) {
        // no point in doing a pushdown that doesn't change anything.
        return;
      }

      call.transformTo(newProj);
    }
  }

}
