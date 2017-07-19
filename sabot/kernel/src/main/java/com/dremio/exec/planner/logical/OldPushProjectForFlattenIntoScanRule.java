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
package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.PrelUtil.ProjectPushInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
@Deprecated
public class OldPushProjectForFlattenIntoScanRule extends RelOptRule{
  public static final RelOptRule INSTANCE = new OldPushProjectForFlattenIntoScanRule();

  private OldPushProjectForFlattenIntoScanRule() {
    super(RelOptHelper.some(ProjectForFlattenRel.class, RelOptHelper.any(ScanRel.class)), "PushProjectForFlattenIntoScanRule");
  }


  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanRel scan = call.rel(1);

    if (scan.isProjectForFlattenPushedDown()) {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectForFlattenRel proj = call.rel(0);
    final ScanRel scan = call.rel(1);

    try {
      ProjectPushInfo columnInfoItemsExprs = PrelUtil.getColumns(scan.getRowType(), proj.getItemExprs());
      ProjectPushInfo columnInfoProjExprs = PrelUtil.getColumns(scan.getRowType(), proj.getProjExprs());


      if (columnInfoItemsExprs == null || columnInfoItemsExprs.isStarQuery() || !scan.getGroupScan().canPushdownProjects(columnInfoItemsExprs.columns)) {
        return;
      }

      final ScanRel newScan =
              new ScanRel(scan.getCluster(),
                      scan.getTable(),
                      scan.getTraitSet().plus(Rel.LOGICAL),
                      columnInfoProjExprs.createNewRowType(proj.getInput().getCluster().getTypeFactory()),
                      scan.getGroupScan().clone(columnInfoItemsExprs.columns),
                      scan.getLayoutInfo(),
                      false,
                      true,
                      scan.getRowCountDiscount());


      List<RexNode> newProjects = Lists.newArrayList();
      for (RexNode n : proj.getProjExprs()) {
        newProjects.add(n.accept(columnInfoItemsExprs.getInputRewriter()));
      }

      final ProjectRel newProj =
              new ProjectRel(proj.getCluster(),
                      proj.getTraitSet().plus(Rel.LOGICAL),
                      newScan,
                      newProjects,
                      proj.getRowType());

      if (ProjectRemoveRule.isTrivial(newProj)) {
        call.transformTo(newScan);
      } else {
        call.transformTo(newProj);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
