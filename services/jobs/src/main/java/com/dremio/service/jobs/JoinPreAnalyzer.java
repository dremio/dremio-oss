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
package com.dremio.service.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;

import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.exec.planner.common.ContainerRel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.explain.PrelSequencer.OpId;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.service.Pointer;
import com.dremio.service.job.proto.JoinCondition;
import com.dremio.service.job.proto.JoinTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * Save all information required for Join analysis from the final prel.
 */
public final class JoinPreAnalyzer extends BasePrelVisitor<Prel, Map<Prel, OpId>, RuntimeException> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinPreAnalyzer.class);

  private Map<SubstitutionUtils.VersionedPath, Integer> tablesMap;
  private List<JoinTable> joinTables;
  private List<JoinPreAnalysisInfo> joinInfos;

  private JoinPreAnalyzer() {
    this.joinInfos = new ArrayList<>();
  }

  public static JoinPreAnalyzer prepare(Prel root) {
    JoinPreAnalyzer preAnalyzer = new JoinPreAnalyzer();
    try {
      // Collect the list of scanned tables.
      preAnalyzer.tablesMap = TableScanCollector.collectTableScans(root);

      // Construct join tables from the scan list.
      preAnalyzer.joinTables =
        preAnalyzer.tablesMap.entrySet()
          .stream()
          .map(entry ->
            new JoinTable()
              .setTableId(entry.getValue())
              .setTableSchemaPathList(entry.getKey().left)
              .setVersionContext(entry.getKey().right != null ? entry.getKey().right.serialize() : null))
          .collect(Collectors.toList());

      // Collect join info.
      Map<Prel, OpId> fullMap = PrelSequencer.getIdMap(root);
      root.accept(preAnalyzer, fullMap);

      return preAnalyzer;
    } catch (Exception e) {
      logger.debug("Caught exception while preparing for join analysis", e);
      return null;
    }
  }

  public List<JoinPreAnalysisInfo> getJoinInfos() {
    return joinInfos;
  }

  public List<JoinTable> getJoinTables() {
    return joinTables;
  }

  private void visitChildren(Prel prel, Map<Prel, OpId> fullMap) {
    for (Prel child : prel){
      child.accept(this, fullMap);
    }
  }

  // Skip all other Prel
  @Override
  public Prel visitPrel(Prel prel, Map<Prel, OpId> fullMap) throws RuntimeException {
    visitChildren(prel, fullMap);
    return prel;
  }

  // save relevant information from join prel for later analysis.
  @Override
  public Prel visitJoin(JoinPrel prel, Map<Prel, OpId> fullMap) throws RuntimeException {
    visitChildren(prel, fullMap);

    OpId opId = fullMap.get(prel);
    JoinRelType joinType = prel.getJoinType();

    final RelMetadataQuery relMetadataQuery = prel.getCluster().getMetadataQuery();
    final JoinInfo joinInfo = prel.analyzeCondition();
    boolean swapped = (prel instanceof HashJoinPrel) && ((HashJoinPrel) prel).isSwapped();
    List<JoinCondition> joinConditions = null;
    try {
      joinConditions =
        Pair.zip(joinInfo.leftKeys, joinInfo.rightKeys)
        .stream()
        .map(pair -> {
          final RelColumnOrigin leftColumnOrigin = Iterables
            .getOnlyElement(relMetadataQuery.getColumnOrigins(prel.getLeft(), pair.left));
          final RelColumnOrigin rightColumnOrigin = Iterables.getOnlyElement(relMetadataQuery.getColumnOrigins(prel.getRight(), pair.right));
          final RelOptTable leftTable = leftColumnOrigin.getOriginTable();
          final RelOptTable rightTable = rightColumnOrigin.getOriginTable();

          int leftOrdinal = leftColumnOrigin.getOriginColumnOrdinal();
          int rightOrdinal = rightColumnOrigin.getOriginColumnOrdinal();
          return new JoinCondition()
            .setBuildSideColumn(rightTable.getRowType().getFieldList().get(rightOrdinal).getName())
            .setProbeSideColumn(leftTable.getRowType().getFieldList().get(leftOrdinal).getName())
            .setBuildSideTableId(Preconditions.checkNotNull(tablesMap.get(SubstitutionUtils.VersionedPath.of(rightTable.getQualifiedName(),
                                                                          SubstitutionUtils.getVersionContext(rightTable)))))
            .setProbeSideTableId(Preconditions.checkNotNull(tablesMap.get(SubstitutionUtils.VersionedPath.of(leftTable.getQualifiedName(),
                                                                          SubstitutionUtils.getVersionContext(leftTable)))));
        })
        .collect(Collectors.toList());
    } catch (Exception e) {
      logger.debug("Caught exception while finding join conditions", e);
    }
    joinInfos.add(new JoinPreAnalysisInfo(opId, joinType, swapped, joinConditions));
    return prel;
  }

  /**
   * Get the list of scan tables.
   */
  public static class TableScanCollector extends RoutingShuttle {
    private final Pointer<Integer> counter = new Pointer<>(0);
    private Map<SubstitutionUtils.VersionedPath, Integer> tables = new HashMap<>();

    public static Map<SubstitutionUtils.VersionedPath, Integer> collectTableScans(Prel root) {
      TableScanCollector collector = new TableScanCollector();
      collector.visit(root);
      return collector.tables;
    }

    @Override
    public RelNode visit(TableScan scan) {
      List<String> table = scan.getTable().getQualifiedName();
      tables.put(SubstitutionUtils.VersionedPath.of(table), counter.value++);
      return scan;
    }

    @Override
    public RelNode visit(RelNode other) {
      if ((other instanceof ContainerRel)) {
        ((ContainerRel) other).getSubTree().accept(this);
      }
      if ((other instanceof TableFunctionPrel)) {
        TableFunctionPrel tableFunctionPrel = ((TableFunctionPrel) other);
        if (tableFunctionPrel.getTable() != null) {
          List<String> table = tableFunctionPrel.getTable().getQualifiedName();
          TableVersionContext versionContext = tableFunctionPrel.getTableMetadata().getVersionContext();
          tables.put(SubstitutionUtils.VersionedPath.of(table, versionContext), counter.value++);
        }
      }
      return super.visit(other);
    }
  }

  /**
   * Join info saved from the prel.
   */
  public static class JoinPreAnalysisInfo {
    private final OpId opId;
    private final JoinRelType joinType;
    private final boolean swapped;
    private final List<JoinCondition> joinConditions;

    public JoinPreAnalysisInfo(OpId opId, JoinRelType joinType, boolean swapped, List<JoinCondition> joinConditions) {
      this.opId = opId;
      this.joinType = joinType;
      this.swapped = swapped;
      this.joinConditions = joinConditions;
    }

    public OpId getOpId() {
      return opId;
    }

    public JoinRelType getJoinType() {
      return joinType;
    }

    public boolean getSwapped() {
      return swapped;
    }

    public List<JoinCondition> getJoinConditions() {
      return joinConditions;
    }
  }
}
