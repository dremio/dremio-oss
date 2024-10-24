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

import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.PrelUtil.ProjectPushInfo;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilterableScan;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

public class PushProjectForFlattenIntoScanRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new PushProjectForFlattenIntoScanRule(true);
  public static final RelOptRule PUSH_ONLY_FIELD_ACCESS_INSTANCE =
      new PushProjectForFlattenIntoScanRule(false);
  private final boolean pushItemOperator;

  private PushProjectForFlattenIntoScanRule(boolean pushItemOperator) {
    super(
        RelOptHelper.some(ProjectForFlattenRel.class, RelOptHelper.any(ScanRelBase.class)),
        "NewPushProjectForFlattenIntoScanRule");
    this.pushItemOperator = pushItemOperator;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectForFlattenRel proj = call.rel(0);
    final ScanRelBase scan = call.rel(1);

    try {
      List<RexNode> projects =
          Stream.concat(
                  proj.getStructuredColumnExprs().stream(),
                  getPartitionColumns(scan, call.builder().getRexBuilder()).stream())
              .collect(ImmutableList.toImmutableList());
      final ProjectPushInfo columnInfoItemsExprs =
          PrelUtil.getColumns(scan.getRowType(), projects, pushItemOperator);
      if (columnInfoItemsExprs == null || columnInfoItemsExprs.isStarQuery()) {
        return;
      }

      ScanRelBase newScan;
      if (scan instanceof FilterableScan) {
        newScan =
            (ScanRelBase)
                ((FilterableScan) scan).cloneWithProject(columnInfoItemsExprs.columns, true);
      } else {
        newScan = scan.cloneWithProject(columnInfoItemsExprs.columns);
      }

      // if the scan is the same as this one (no change in projections), no need to push down.
      if (newScan.getProjectedColumns().equals(scan.getProjectedColumns())) {
        return;
      }

      List<RexNode> newProjects = Lists.newArrayList();
      for (RexNode n : proj.getProjExprs()) {
        newProjects.add(n.accept(columnInfoItemsExprs.getInputRewriter()));
      }

      final ProjectRel newProj =
          ProjectRel.create(
              proj.getCluster(),
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

  public static List<RexNode> getPartitionColumns(ScanRelBase scan, RexBuilder rexBuilder) {
    if (scan instanceof FilterableScan) {
      FilterableScan filterableScan = (FilterableScan) scan;
      TableMetadata tableMetadata = filterableScan.getTableMetadata();
      if (tableMetadata.getReadDefinition() != null
          && tableMetadata.getReadDefinition().getPartitionColumnsList() != null) {
        final List<String> partitionColumns =
            tableMetadata.getReadDefinition().getPartitionColumnsList();
        final List<String> fieldNames = filterableScan.getRowType().getFieldNames();
        final Map<String, Integer> fieldMap =
            IntStream.range(0, fieldNames.size())
                .boxed()
                .collect(Collectors.toMap(fieldNames::get, i -> i));
        return partitionColumns.stream()
            .filter(fieldMap::containsKey)
            .map(f -> rexBuilder.makeInputRef(filterableScan, fieldMap.get(f)))
            .collect(ImmutableList.toImmutableList());
      }
    }
    return ImmutableList.of();
  }
}
