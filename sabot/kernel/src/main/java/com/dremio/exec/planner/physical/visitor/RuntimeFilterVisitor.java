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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.AggPrelBase;
import com.dremio.exec.planner.physical.BroadcastExchangePrel;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.physical.SelectionVectorRemoverPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.UnionPrel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.filter.RuntimeFilterEntry;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.collect.ImmutableList;

/**
 * This visitor does two major things:
 * 1) check with HashJoinPrel should use runtime filter
 * 2) build plan time RuntimeFilterInfo for HashJoinPrel
 */
public class RuntimeFilterVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RuntimeFilterVisitor.class);
  private Map<Prel, PrelSequencer.OpId> prelOpIdMap;

  private RuntimeFilterVisitor(Map<Prel, PrelSequencer.OpId> prelOpIdMap) {
    this.prelOpIdMap = prelOpIdMap;
  }

  public static Prel addRuntimeFilterToHashJoin(Prel prel) {
    RuntimeFilterVisitor instance = new RuntimeFilterVisitor(PrelSequencer.getIdMap(prel));
    return prel.accept(instance, null);
  }

  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = new ArrayList<>();
    for (Prel child : prel) {
      child = child.accept(this, value);
      children.add(child);
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitJoin(JoinPrel prel, Void value) throws RuntimeException {
    if (prel instanceof HashJoinPrel) {
      HashJoinPrel hashJoinPrel = (HashJoinPrel) prel;
      //Generate possible RuntimeFilterInfo to the HashJoinPrel
      // identify the corresponding probe side ScanPrel.
      RuntimeFilterInfo runtimeFilterInfo = generateRuntimeFilterInfo(hashJoinPrel);
      if (runtimeFilterInfo != null) {
        hashJoinPrel.setRuntimeFilterInfo(runtimeFilterInfo);
      }
    }
    return visitPrel(prel, value);
  }

  /**
   * Generate a possible RuntimeFilter of a HashJoinPrel
   *
   * @param hashJoinPrel
   * @return null or a partial information RuntimeFilterInfo
   */
  private RuntimeFilterInfo generateRuntimeFilterInfo (HashJoinPrel hashJoinPrel) throws RuntimeException {
    List<RuntimeFilterEntry> partitionColumns = new ArrayList<>();
    List<RuntimeFilterEntry> nonPartitionColumns = new ArrayList<>();

    JoinRelType joinRelType = hashJoinPrel.getJoinType();
    JoinInfo joinInfo = hashJoinPrel.analyzeCondition();
    if (!joinInfo.isEqui()) {
      return null;
    }

    if ((joinRelType == JoinRelType.LEFT && !hashJoinPrel.isSwapped())
        || (joinRelType == JoinRelType.RIGHT && hashJoinPrel.isSwapped())
        || joinRelType == JoinRelType.FULL) {
      return null;
    }

    final RelNode currentProbe;
    final RelNode currentBuild;
    final List<Integer> probeKeys;
    final List<Integer> buildKeys;

    //identify probe sie and build side prel tree
    if (hashJoinPrel.isSwapped()) {
      currentProbe = hashJoinPrel.getRight();
      currentBuild = hashJoinPrel.getLeft();
      probeKeys = hashJoinPrel.getRightKeys();
      buildKeys = hashJoinPrel.getLeftKeys();
    } else {
      currentProbe = hashJoinPrel.getLeft();
      currentBuild = hashJoinPrel.getRight();
      probeKeys = hashJoinPrel.getLeftKeys();
      buildKeys = hashJoinPrel.getRightKeys();
    }

    // find exchange node from build side
    ExchangePrel buildExchangePrel = findExchangePrel(currentBuild);
    ExchangePrel probeExchangePrel = findExchangePrel(currentProbe);
    if(buildExchangePrel == null && probeExchangePrel == null) {
      //does not support single fragment mode, that is the right build side can
      //only be BroadcastExchangePrel or HashToRandomExchangePrel
      return null;
    }
    List<String> rightFields = currentBuild.getRowType().getFieldNames();

    for (Pair<Integer,Integer> keyPair : Pair.zip(probeKeys, buildKeys)) {
      Integer probeKey = keyPair.left;
      Integer buildKey = keyPair.right;
      String buildFieldName = rightFields.get(buildKey);

      //This also avoids the left field of the join condition with a function call.
      if (!(currentProbe instanceof Prel)) {
        return null;
      }
      List<ColumnOriginScan> columnOrigins = ((Prel) currentProbe).accept(new FindScanVisitor(), probeKey);
      columnOrigins.stream()
        .filter(Objects::nonNull)
        .forEach(columnOrigin -> {
          Prel scanPrel = columnOrigin.getScan();
          String leftFieldName = columnOrigin.getField();
          PrelSequencer.OpId opId = prelOpIdMap.get(scanPrel);
          int probeScanMajorFragmentId = opId.getFragmentId();
          int probeScanOperatorId = opId.getAsSingleInt();
          RuntimeFilterEntry runtimeFilterEntry = new RuntimeFilterEntry(leftFieldName, buildFieldName, probeScanMajorFragmentId, probeScanOperatorId);
          if (isPartitionColumn(scanPrel, leftFieldName)) {
            partitionColumns.add(runtimeFilterEntry);
          } else {
            nonPartitionColumns.add(runtimeFilterEntry);
          }
      });
    }
    if(!partitionColumns.isEmpty() || !nonPartitionColumns.isEmpty()) {
      return new RuntimeFilterInfo.Builder()
        .nonPartitionJoinColumns(nonPartitionColumns)
        .partitionJoinColumns(partitionColumns)
        .isBroadcastJoin((buildExchangePrel instanceof BroadcastExchangePrel))
        .build();
    } else {
      return null;
    }

  }

  private static class ColumnOriginScan extends Pair<Prel,String> {

    /**
     * Creates a Pair.
     *
     * @param left  left value
     * @param right right value
     */
    public ColumnOriginScan(Prel left, String right) {
      super(left, right);
    }

    static ColumnOriginScan of(Prel scan, String field) {
      return new ColumnOriginScan(scan, field);
    }

    Prel getScan() {
      return getKey();
    }

    String getField() {
      return getValue();
    }
  }

  private static class FindScanVisitor extends BasePrelVisitor<List<ColumnOriginScan>,Integer,RuntimeException> {
    @Override
    public List<ColumnOriginScan> visitPrel(Prel prel, Integer idx) {
      if (prel instanceof FilterPrel || prel instanceof SelectionVectorRemoverPrel) {
        if (prel.getInput(0) instanceof Prel) {
          return ((Prel) prel.getInput(0)).accept(this, idx);
        }
      }
      if (prel instanceof AggPrelBase) {
        return visitAggregate(((AggPrelBase) prel), idx);
      }
      if (prel instanceof UnionPrel) {
        return visitUnion(((UnionPrel) prel), idx);
      }
      return ImmutableList.of();
    }

    private List<ColumnOriginScan> visitAggregate(AggPrelBase agg, Integer idx) {
      if (idx < agg.getGroupCount()) {
        if (!(agg.getInput() instanceof Prel)) {
          return ImmutableList.of();
        }
        Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION,
          agg.getInput().getRowType().getFieldCount(), agg.getRowType().getFieldCount());

        int i = 0;
        for (int j : agg.getGroupSet()) {
          m.set(j, i++);
        }
        return ((Prel) agg.getInput()).accept(this, m.getTarget(idx));
      }
      return ImmutableList.of();
    }

    private List<ColumnOriginScan> visitUnion(UnionPrel union, Integer idx) {
      ImmutableList.Builder<ColumnOriginScan> builder = ImmutableList.builder();
      for (Prel child : union) {
        builder.addAll(child.accept(this, idx));
      }
      return builder.build();
    }

    @Override
    public List<ColumnOriginScan> visitProject(ProjectPrel prel, Integer idx) {
      if (!(prel.getChildExps().get(idx) instanceof RexInputRef)) {
        return ImmutableList.of();
      }
      RexInputRef ref = (RexInputRef) prel.getChildExps().get(idx);
      if (prel.getInput(0) instanceof Prel) {
        return ((Prel) prel.getInput(0)).accept(this, ref.getIndex());
      }
      return ImmutableList.of();
    }

    @Override
    public List<ColumnOriginScan> visitExchange(ExchangePrel exchange, Integer idx) {
      if (exchange.getInput() instanceof Prel) {
        return ((Prel) exchange.getInput()).accept(this, idx);
      }
      return ImmutableList.of();
    }

    @Override
    public List<ColumnOriginScan> visitLeaf(LeafPrel prel, Integer idx) {
      if (prel instanceof ScanPrelBase) {
        return ImmutableList.of(ColumnOriginScan.of((ScanPrelBase) prel, prel.getRowType().getFieldNames().get(idx)));
      }
      return ImmutableList.of();
    }

    @Override
    public List<ColumnOriginScan> visitJoin(JoinPrel join, Integer outputIndex) {
      int idx;
      if (join.getProjectedFields() == null) {
        idx = outputIndex;
      } else {
        idx = join.getProjectedFields().asList().get(outputIndex);
      }
      if (idx < join.getLeft().getRowType().getFieldCount()) {
        if (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.LEFT) {
          if (!(join.getLeft() instanceof Prel)) {
            return ImmutableList.of();
          }
          Prel left = (Prel) join.getLeft();
          return left.accept(this, idx);
        }
      } else {
        if (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.RIGHT) {
          int newIdx = idx - join.getLeft().getRowType().getFieldCount();
          if (!(join.getRight() instanceof Prel)) {
            return ImmutableList.of();
          }
          Prel right = (Prel) join.getRight();
          return right.accept(this, newIdx);
        }
      }
      return ImmutableList.of();
    }

    @Override
    public List<ColumnOriginScan> visitTableFunction(TableFunctionPrel prel, Integer idx) {
      if (prel.isDataScan()) {
        return ImmutableList.of(ColumnOriginScan.of(prel, prel.getRowType().getFieldNames().get(idx))); // End here. Operators below this point are possibly dealing with manifests.
      }
      return ImmutableList.of();
    }
  }

  private ExchangePrel findExchangePrel(RelNode rightRelNode) {
    if (rightRelNode instanceof ExchangePrel) {
      return (ExchangePrel) rightRelNode;
    }
    if (rightRelNode instanceof ScanPrelBase) {
      return null;
    } else {
      List<RelNode> relNodes = rightRelNode.getInputs();
      if (relNodes.size() == 1) {
        RelNode leftNode = relNodes.get(0);
        return findExchangePrel(leftNode);
      } else {
        return null;
      }
    }
  }

  private boolean isPartitionColumn(Prel scanPrel, String fieldName) {
    Preconditions.checkArgument(scanPrel instanceof ScanRelBase || scanPrel instanceof TableFunctionPrel,
            "Incorrect data scan prel {}", scanPrel.getClass().getName());
    TableMetadata tableMetadata = (scanPrel instanceof ScanRelBase) ? ((ScanRelBase) scanPrel).getTableMetadata() : ((TableFunctionPrel) scanPrel).getTableMetadata();
    ReadDefinition readDefinition = tableMetadata.getDatasetConfig().getReadDefinition();
    if (readDefinition.getPartitionColumnsList() == null) {
      return false;
    }
    return !readDefinition.getPartitionColumnsList().isEmpty() && readDefinition.getPartitionColumnsList().contains(fieldName);
  }

}
