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
package com.dremio.exec.planner.physical.visitor;

import static com.dremio.exec.planner.cost.DremioRelMdUtil.getNameFromColumnOrigin;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.cost.DremioRelMdUtil;
import com.dremio.exec.planner.physical.AggregatePrel;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.proto.UserBitShared;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;

/** Class for scrapping the physical Plan */
public class QueryProfileProcessor {

  public static Map<String, UserBitShared.RelNodeInfo> process(Prel prel) throws RuntimeException {
    PrettyPlanVisitor relPrettyInformationExtractorVisitor =
        new PrettyPlanVisitor(PrelSequencer.getIdMap(prel));
    prel.accept(relPrettyInformationExtractorVisitor, null);
    return relPrettyInformationExtractorVisitor.getRelNodeInfoMap();
  }

  /**
   * PrelVisitor which scraps a plan and converts its salient features to be serialized and stored
   * in {@link com.dremio.exec.proto.UserBitShared.QueryProfile}
   */
  public static class PrettyPlanVisitor extends BasePrelVisitor<Void, Object, RuntimeException> {
    private final Map<Prel, PrelSequencer.OpId> opIdMap;

    private final Map<String, UserBitShared.RelNodeInfo> relNodeInfoMap;

    public PrettyPlanVisitor(Map<Prel, PrelSequencer.OpId> opIdMap) {
      super();
      this.opIdMap = opIdMap;
      relNodeInfoMap = new HashMap<>();
    }

    public Map<String, UserBitShared.RelNodeInfo> getRelNodeInfoMap() {
      return this.relNodeInfoMap;
    }

    @Override
    public Void visitPrel(Prel prel, Object value) throws RuntimeException {
      for (Prel child : prel) {
        child.accept(this, null);
      }
      return null;
    }

    @Override
    public Void visitAggregate(AggregatePrel aggregatePrel, Object unused) throws RuntimeException {
      visitPrel(aggregatePrel, unused);
      PrelSequencer.OpId opId = opIdMap.get(aggregatePrel);
      relNodeInfoMap.put(
          opId.toString(),
          UserBitShared.RelNodeInfo.newBuilder()
              .setAggregateInfo(
                  UserBitShared.AggregateInfo.newBuilder()
                      .addAllGroupingKey(groupSetToStringList(aggregatePrel))
                      .addAllAggExpr(aggCallsToStringList(aggregatePrel))
                      .build())
              .build());
      return null;
    }

    @Override
    public Void visitJoin(JoinPrel joinPrel, Object unused) throws RuntimeException {
      visitPrel(joinPrel, unused);
      RexNode prettyJoinCondition = getPrettyColumnLineage(joinPrel, joinPrel.getCondition());
      RexNode prettyExtraCondition = getPrettyColumnLineage(joinPrel, joinPrel.getExtraCondition());

      // Default Values if failed to scrape from expression Lineage
      if (prettyJoinCondition == null) {
        prettyJoinCondition = joinPrel.getCondition();
      }
      if (prettyExtraCondition == null) {
        prettyExtraCondition = joinPrel.getExtraCondition();
      }
      PrelSequencer.OpId opId = opIdMap.get(joinPrel);
      relNodeInfoMap.put(
          opId.toString(),
          UserBitShared.RelNodeInfo.newBuilder()
              .setJoinInfo(
                  UserBitShared.JoinInfo.newBuilder()
                      .setJoinFilterString(String.valueOf(prettyJoinCondition))
                      .setExtraJoinFilterString(String.valueOf(prettyExtraCondition))
                      .build())
              .build());
      return null;
    }

    @Override
    public Void visitFilter(FilterPrel filterPrel, Object unused) throws RuntimeException {
      visitPrel(filterPrel, unused);
      RexNode prettyFilterCondition = getPrettyColumnLineage(filterPrel, filterPrel.getCondition());
      // Default Values if failed to scrape from expression Lineage
      if (prettyFilterCondition == null) {
        prettyFilterCondition = filterPrel.getCondition();
      }
      PrelSequencer.OpId opId = opIdMap.get(filterPrel);
      relNodeInfoMap.put(
          opId.toString(),
          UserBitShared.RelNodeInfo.newBuilder()
              .setFilterInfo(
                  UserBitShared.FilterInfo.newBuilder()
                      .setFilterString(prettyFilterCondition.toString())
                      .build())
              .build());
      return null;
    }

    @Override
    public Void visitTableFunction(TableFunctionPrel prel, Object unused) throws RuntimeException {
      visitPrel(prel, unused);
      if (prel.getTableFunctionConfig().getType()
          != TableFunctionConfig.FunctionType.DATA_FILE_SCAN) {
        return null;
      }
      PrelSequencer.OpId opId = opIdMap.get(prel);
      relNodeInfoMap.put(
          opId.toString(),
          UserBitShared.RelNodeInfo.newBuilder()
              .setScanInfo(
                  UserBitShared.ScanInfo.newBuilder()
                      .setTableName(PathUtils.constructFullPath(prel.getTable().getQualifiedName()))
                      .build())
              .build());
      return null;
    }

    @Override
    public Void visitLeaf(LeafPrel prel, Object value) throws RuntimeException {
      if (prel instanceof ScanPrelBase) {
        // Add info about all the Scans
        PrelSequencer.OpId opId = opIdMap.get(prel);
        relNodeInfoMap.put(
            opId.toString(),
            UserBitShared.RelNodeInfo.newBuilder()
                .setScanInfo(
                    UserBitShared.ScanInfo.newBuilder()
                        .setTableName(
                            PathUtils.constructFullPath(prel.getTable().getQualifiedName()))
                        .build())
                .build());
      }
      return null;
    }

    public static List<UserBitShared.AggExpressionInfo> aggCallsToStringList(
        AggregatePrel aggregatePrel) throws RuntimeException {
      return aggregatePrel.getAggCallList().stream()
          .map(
              f -> {
                List<String> args = new ArrayList<>();
                for (int i = 0; i < f.getArgList().size(); i++) {
                  int idx = f.getArgList().get(i);
                  RelColumnOrigin relColumnOrigin = getColumnOrigin(aggregatePrel.getInput(), idx);
                  // Fallback to stringValue of referenced column name of the input
                  args.add(
                      relColumnOrigin != null
                          ? getNameFromColumnOrigin(relColumnOrigin)
                          : aggregatePrel
                              .getInput()
                              .getRowType()
                              .getFieldList()
                              .get(idx)
                              .getName());
                }
                return UserBitShared.AggExpressionInfo.newBuilder()
                    .setAggregation(f.getAggregation().toString())
                    .addAllArgs(args)
                    .build();
              })
          .collect(Collectors.toList());
    }

    public static List<String> groupSetToStringList(AggregatePrel input) throws RuntimeException {
      ImmutableBitSet groupSet = input.getGroupSet();
      if (groupSet == null) {
        throw new RuntimeException(String.format("Origin not found for column"));
      }
      List<String> groupingList = new ArrayList<>();
      for (int group : BitSets.toIter(groupSet)) {
        RelColumnOrigin columnOrigin = getColumnOrigin(input, group);
        // Fallback to stringValue of groupingKey
        groupingList.add(
            (columnOrigin != null) ? getNameFromColumnOrigin(columnOrigin) : String.valueOf(group));
      }
      return groupingList;
    }

    private static RelColumnOrigin getColumnOrigin(RelNode input, int idx) {
      Set<RelColumnOrigin> originSet =
          input.getCluster().getMetadataQuery().getColumnOrigins(input, idx);
      if (originSet == null || originSet.size() == 0) {
        return null;
      }
      // ToDo: currently we just return first Column Origin
      return (RelColumnOrigin) originSet.toArray()[0];
    }

    private RexNode getPrettyColumnLineage(RelNode rel, RexNode expr) {
      if (expr == null) {
        return null;
      }
      RelMetadataQuery relMetadataQuery = rel.getCluster().getMetadataQuery();
      Set<RexNode> rexNodeSet = relMetadataQuery.getExpressionLineage(rel, expr);
      if (rexNodeSet == null) {
        return null;
      }

      // ToDo: currently we just return first column lineage
      RexNode prettyExpr = (RexNode) rexNodeSet.toArray()[0];
      return DremioRelMdUtil.swapTableInputReferences(prettyExpr);
    }
  }
}
