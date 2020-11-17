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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.exec.planner.physical.BroadcastExchangePrel;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.dremio.exec.planner.physical.TopNPrel;
import com.dremio.exec.planner.physical.WindowPrel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.filter.RuntimeFilterEntry;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;

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
    boolean allowRuntimeFilter = (joinInfo.isEqui()) && (joinRelType == JoinRelType.INNER || joinRelType == JoinRelType.RIGHT);
    if (!allowRuntimeFilter) {
      return null;
    }

    final RelNode currentLeft;
    final RelNode currentRight;
    final List<Integer> leftKeys;
    final List<Integer> rightKeys;
    final RelMetadataQuery relMetadataQuery = hashJoinPrel.getCluster().getMetadataQuery();

    //identify probe sie and build side prel tree
    if (hashJoinPrel.isSwapped()) {
      currentLeft = hashJoinPrel.getRight();
      currentRight = hashJoinPrel.getLeft();
      leftKeys = hashJoinPrel.getRightKeys();
      rightKeys = hashJoinPrel.getLeftKeys();
    } else {
      currentLeft = hashJoinPrel.getLeft();
      currentRight = hashJoinPrel.getRight();
      leftKeys = hashJoinPrel.getLeftKeys();
      rightKeys = hashJoinPrel.getRightKeys();
    }

    // find exchange node from build side
    ExchangePrel rightExchangePrel = findExchangePrel(currentRight);
    ExchangePrel leftExchangePrel = findExchangePrel(currentLeft);
    if(rightExchangePrel == null && leftExchangePrel == null) {
      //does not support single fragment mode, that is the right build side can
      //only be BroadcastExchangePrel or HashToRandomExchangePrel
      return null;
    }
    List<String> leftFields = currentLeft.getRowType().getFieldNames();
    List<String> rightFields = currentRight.getRowType().getFieldNames();

    int keyIndex = 0;
    for (Integer leftKey : leftKeys) {
      String leftFieldName = leftFields.get(leftKey);
      Integer rightKey = rightKeys.get(keyIndex++);
      String rightFieldName = rightFields.get(rightKey);

      //avoid runtime filter if the join column is not original column
      final RelColumnOrigin leftColumnOrigin = relMetadataQuery.getColumnOrigin(currentLeft, leftKey);
      if (null == leftColumnOrigin) {
        //this includes column is derived
        continue;
      }

      //This also avoids the left field of the join condition with a function call.
      ScanPrelBase scanPrel = findLeftScanPrel(leftFieldName, currentLeft);
      if (scanPrel != null) {
        //if contains block node on path from join to scan, we may not push down runtime filter
        if(hasBlockNode((Prel) currentLeft, scanPrel)) {
          return null;
        }
        PrelSequencer.OpId opId = prelOpIdMap.get(scanPrel);
        int probeScanMajorFragmentId = opId.getFragmentId();
        int probeScanOperatorId = opId.getAsSingleInt();
        RuntimeFilterEntry runtimeFilterEntry = new RuntimeFilterEntry(leftFieldName, rightFieldName, probeScanMajorFragmentId, probeScanOperatorId);
        if (isPartitionColumn(scanPrel, leftFieldName)) {
          partitionColumns.add(runtimeFilterEntry);
        } else {
          nonPartitionColumns.add(runtimeFilterEntry);
        }
      } else {
        return null;
      }
    }
    if(!partitionColumns.isEmpty() || !nonPartitionColumns.isEmpty()) {
      return new RuntimeFilterInfo.Builder()
        .nonPartitionJoinColumns(nonPartitionColumns)
        .partitionJoinColumns(partitionColumns)
        .isBroadcastJoin((rightExchangePrel instanceof BroadcastExchangePrel))
        .build();
    } else {
      return null;
    }

  }

  /**
   * Find a join condition's left input source scan Prel. If we can't find a target scan Prel then this
   * RuntimeFilter can not pushed down to a probe side scan Prel.
   *
   * @param fieldName   left join condition field Name
   * @param leftRelNode left RelNode of a BiRel or the SingleRel
   * @return a left scan Prel which contains the left join condition name or null
   */
  private ScanPrelBase findLeftScanPrel(String fieldName, RelNode leftRelNode) {
    if (leftRelNode instanceof ScanPrelBase) {
      RelDataType scanRowType = leftRelNode.getRowType();
      RelDataTypeField field = scanRowType.getField(fieldName, true, true);
      if (field != null) {
        //found
        return (ScanPrelBase) leftRelNode;
      } else {
        return null;
      }
    }  else if (leftRelNode == null) {
      return null;
    } else {
      List<RelNode> relNodes = leftRelNode.getInputs();
      RelNode scanNode;
      for (RelNode node: relNodes) {
        scanNode = findLeftScanPrel(fieldName, node);
        if (scanNode != null) {
          return (ScanPrelBase) scanNode;
        }
      }
      return null;
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

  private boolean isPartitionColumn(ScanPrelBase scanPrel, String fieldName) {
    ReadDefinition readDefinition = scanPrel.getTableMetadata().getDatasetConfig().getReadDefinition();
    if (readDefinition.getPartitionColumnsList() == null) {
      return false;
    }
    return readDefinition.getPartitionColumnsList().isEmpty()? false:readDefinition.getPartitionColumnsList().contains(fieldName);
  }

  private boolean hasBlockNode(Prel startNode, Prel endNode) {
    BlockNodeVisitor blockNodeVisitor = new BlockNodeVisitor();
    startNode.accept(blockNodeVisitor, endNode);
    return blockNodeVisitor.isEncounteredBlockNode();
  }

  private static class BlockNodeVisitor extends BasePrelVisitor<Void, Prel, RuntimeException> {

    private boolean encounteredBlockNode;

    @Override
    public Void visitPrel(Prel prel, Prel endValue) throws RuntimeException {
      if (prel == endValue) {
        return null;
      }

      Prel currentPrel = prel;
      if (currentPrel == null) {
        return null;
      }

      if (currentPrel instanceof WindowPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof StreamAggPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof HashAggPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof SortPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof TopNPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof HashJoinPrel && ((HashJoinPrel) currentPrel).getJoinType() != JoinRelType.INNER) {
        encounteredBlockNode = true;
        return null;
      }

      for (Prel subPrel : currentPrel) {
        visitPrel(subPrel, endValue);
      }
      return null;
    }

    public boolean isEncounteredBlockNode() {
      return encounteredBlockNode;
    }
  }
}
