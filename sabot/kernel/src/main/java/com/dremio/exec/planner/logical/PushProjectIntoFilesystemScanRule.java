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

import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.PrelUtil.ProjectPushInfo;
import com.dremio.exec.store.dfs.FilesystemScanDrel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

/** This Rule Pushes a project past filesystem scan while maintaining partition column info */
public class PushProjectIntoFilesystemScanRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new PushProjectIntoFilesystemScanRule(true);
  public static final RelOptRule PUSH_ONLY_FIELD_ACCESS_INSTANCE =
      new PushProjectIntoFilesystemScanRule(false);
  private final boolean pushItemOperator;

  private PushProjectIntoFilesystemScanRule(boolean pushItemOperator) {
    super(
        RelOptHelper.some(ProjectRel.class, RelOptHelper.any(FilesystemScanDrel.class)),
        DremioRelFactories.LOGICAL_BUILDER,
        "PushProjectIntoFilesystemScanRule");
    this.pushItemOperator = pushItemOperator;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project proj = call.rel(0);
    final FilesystemScanDrel scan = call.rel(1);
    List<RexNode> projects =
        Stream.concat(
                proj.getProjects().stream(),
                PushProjectForFlattenIntoScanRule.getPartitionColumns(
                    scan, call.builder().getRexBuilder())
                    .stream())
            .collect(ImmutableList.toImmutableList());

    ProjectPushInfo columnInfo = PrelUtil.getColumns(scan.getRowType(), projects, pushItemOperator);

    // get TableBase, either wrapped in RelOptTable, or TranslatableTable. TableBase table =
    // scan.getTable().unwrap(TableBase.class);
    if (columnInfo == null || columnInfo.isStarQuery()) {
      return;
    }

    FilesystemScanDrel newScan = scan.cloneWithProject(columnInfo.columns, true);

    // if the scan is the same as this one (no change in projections), no need to push down.
    if (newScan.getProjectedColumns().equals(scan.getProjectedColumns())) {
      return;
    }

    List<RexNode> newProjects = Lists.newArrayList();
    for (RexNode n : proj.getProjects()) {
      newProjects.add(n.accept(columnInfo.getInputRewriter()));
    }

    final RelBuilder relBuilder = relBuilderFactory.create(proj.getCluster(), null);
    relBuilder.push(newScan);
    relBuilder.project(newProjects, proj.getRowType().getFieldNames());
    final RelNode newProj = relBuilder.build();

    if (newProj instanceof Project
        && ProjectRemoveRule.isTrivial((Project) newProj)
        && newScan
            .getRowType()
            .getFullTypeString()
            .equals(newProj.getRowType().getFullTypeString())) {
      call.transformTo(newScan);
    } else {
      if (newScan.getProjectedColumns().equals(scan.getProjectedColumns())) {
        // no point in doing a pushdown that doesn't change anything.
        return;
      }
      call.transformTo(newProj);
    }
  }
}
