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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.physical.AggregatePrel;
import com.dremio.exec.planner.physical.BridgeExchangePrel;
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
import com.dremio.exec.planner.physical.filter.RuntimeFilterId;
import com.dremio.exec.planner.physical.filter.RuntimeFilteredRel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.collect.ImmutableList;

/**
 * This visitor does two major things:
 * 1) check with HashJoinPrel should use runtime filter
 * 2) build plan time RuntimeFilterInfo for HashJoinPrel
 */
public class RuntimeFilterDecorator {

  public static Prel addRuntimeFilterToHashJoin(Prel prel, boolean nonParitionRuntimeFiltersEnabled) {
    JoinVisitor instance = new JoinVisitor(nonParitionRuntimeFiltersEnabled);
    return prel.accept(instance, null);
  }

  private static class JoinVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

    private boolean nonParitionRuntimeFiltersEnabled;

    private JoinVisitor(boolean nonParitionRuntimeFiltersEnabled) {
      this.nonParitionRuntimeFiltersEnabled = nonParitionRuntimeFiltersEnabled;
    }

    @Override
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
        decorateHashJoinPrel(hashJoinPrel);
      }
      return visitPrel(prel, value);
    }

    /**
     * Generate a possible RuntimeFilter of a HashJoinPrel
     *
     * @param hashJoinPrel
     * @return null or a partial information RuntimeFilterInfo
     */
    private void decorateHashJoinPrel(HashJoinPrel hashJoinPrel) throws RuntimeException {
      JoinRelType joinRelType = hashJoinPrel.getJoinType();
      JoinInfo joinInfo = hashJoinPrel.analyzeCondition();
      if (!joinInfo.isEqui()
          || (joinRelType == JoinRelType.LEFT && !hashJoinPrel.isSwapped())
          || (joinRelType == JoinRelType.RIGHT && hashJoinPrel.isSwapped())
          || joinRelType == JoinRelType.FULL) {
        return;
      }

      // Probe side and build side are terminologies for hash joins where the build side is
      // transformed into map(the smaller side) and the probe side queries the map (the larger
      // side).
      final RelNode probeSideRel;
      final RelNode buildSideRel;
      final List<Integer> probeKeys;
      final List<Integer> buildKeys;

      //identify probe sie and build side prel tree
      if (hashJoinPrel.isSwapped()) {
        probeSideRel = hashJoinPrel.getRight();
        buildSideRel = hashJoinPrel.getLeft();
        probeKeys = hashJoinPrel.getRightKeys();
        buildKeys = hashJoinPrel.getLeftKeys();
      } else {
        probeSideRel = hashJoinPrel.getLeft();
        buildSideRel = hashJoinPrel.getRight();
        probeKeys = hashJoinPrel.getLeftKeys();
        buildKeys = hashJoinPrel.getRightKeys();
      }

      boolean shouldAddNonPartitionRF = hasFilter(buildSideRel);

      // find exchange node from build side
      ExchangePrel buildExchangePrel = findExchangePrel(buildSideRel);
      ExchangePrel probeExchangePrel = findExchangePrel(probeSideRel);
      if(buildExchangePrel == null && probeExchangePrel == null) {
        //does not support single fragment mode, that is the right build side can
        //only be BroadcastExchangePrel or HashToRandomExchangePrel
        return;
      } else if (!(probeSideRel instanceof Prel)) {
        //This also avoids the left field of the join condition with a function call.
        return;
      }
      List<String> rightFields = buildSideRel.getRowType().getFieldNames();

      RuntimeFilterId id = RuntimeFilterId.createRuntimeFilterId(buildSideRel,
          buildExchangePrel instanceof BroadcastExchangePrel);
      boolean found = false;

      for (Pair<Integer,Integer> keyPair : Pair.zip(probeKeys, buildKeys)) {
        Integer probeKey = keyPair.left;
        Integer buildKey = keyPair.right;
        String buildFieldName = rightFields.get(buildKey);
        List<ColumnOriginScan> probeSideColumnOriginList =
          ((Prel) probeSideRel).accept(new FindScanVisitor(), probeKey);
        for (ColumnOriginScan probeSideColumnOrigin : probeSideColumnOriginList) {
          RuntimeFilteredRel scanPrel = probeSideColumnOrigin.filteredRel;
          String probeFieldName = probeSideColumnOrigin.fieldName;
          RuntimeFilteredRel.ColumnType columnType = isPartitionColumn(scanPrel, probeFieldName)
            ? RuntimeFilteredRel.ColumnType.PARTITION
            : RuntimeFilteredRel.ColumnType.RANDOM;
          if (columnType == RuntimeFilteredRel.ColumnType.PARTITION
            || (nonParitionRuntimeFiltersEnabled && shouldAddNonPartitionRF)) {
            scanPrel.addRuntimeFilter(
              new RuntimeFilteredRel.Info(id, columnType, probeFieldName, buildFieldName));
            found = true;
          }
        }
      }
      if (found) {
        hashJoinPrel.setRuntimeFilterId(id);
      }
    }

  }

  private static boolean hasFilter(RelNode rel) {
    if (rel instanceof FilterPrel) {
      return true;
    }
    if (rel instanceof TableFunctionPrel) {
      if (((TableFunctionPrel) rel).hasFilter()) {
        return true;
      }
    }
    for (RelNode child : rel.getInputs()) {
      if (hasFilter(child)) {
        return true;
      }
    }
    return false;
  }

  private static class ColumnOriginScan {
    final RuntimeFilteredRel filteredRel;
    final String fieldName;

    public ColumnOriginScan(RuntimeFilteredRel filteredRel, String fieldName) {
      this.filteredRel = filteredRel;
      this.fieldName = fieldName;
    }
  }

  private static class FindScanVisitor extends BasePrelVisitor<List<ColumnOriginScan>,Integer,RuntimeException> {

    @Override
    public List<ColumnOriginScan> visitPrel(Prel prel, Integer idx) {
      if (prel instanceof SelectionVectorRemoverPrel) {
        return visitRel(prel.getInput(0), idx);
      } else {
        return ImmutableList.of();
      }
    }

    @Override
    public List<ColumnOriginScan> visitFilter(FilterPrel filter, Integer idx) {
      return visitRel(filter.getInput(0), idx);
    }

    @Override
    public List<ColumnOriginScan> visitAggregate(AggregatePrel agg, Integer idx) {
      if (idx < agg.getGroupCount()) {
        return visitRel(agg.getInput(), agg.getGroupSet().asList().get(idx));
      } else {
        return ImmutableList.of();
      }
    }

    @Override
    public List<ColumnOriginScan> visitUnion(UnionPrel union, Integer idx) {
      ImmutableList.Builder<ColumnOriginScan> builder = ImmutableList.builder();
      for (Prel child : union) {
        builder.addAll(child.accept(this, idx));
      }
      return builder.build();
    }

    @Override
    public List<ColumnOriginScan> visitProject(ProjectPrel prel, Integer idx) {
      RexNode rex = prel.getProjects().get(idx);
      if (rex instanceof RexInputRef) {
        return visitRel(prel.getInput(), ((RexInputRef)rex).getIndex());
      } else {
        return ImmutableList.of();
      }
    }

    @Override
    public List<ColumnOriginScan> visitExchange(ExchangePrel exchange, Integer idx) {
      if (exchange instanceof BridgeExchangePrel) {
        return ImmutableList.of();
      }
      return visitRel(exchange.getInput(), idx);
    }

    @Override
    public List<ColumnOriginScan> visitLeaf(LeafPrel prel, Integer idx) {
      if (prel instanceof RuntimeFilteredRel) {
        return ImmutableList.of(
          new ColumnOriginScan((RuntimeFilteredRel) prel,
              prel.getRowType().getFieldNames().get(idx)));
      } else {
        return ImmutableList.of();
      }
    }

    @Override
    public List<ColumnOriginScan> visitJoin(JoinPrel join, Integer outputIndex) {
      if (outputIndex < join.getLeft().getRowType().getFieldCount()) {
        if (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.LEFT) {
          return visitRel(join.getLeft(), outputIndex);
        }
      } else {
        if (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.RIGHT) {
          int newIdx = outputIndex - join.getLeft().getRowType().getFieldCount();
          return visitRel(join.getRight(), newIdx);
        }
      }
      return ImmutableList.of();
    }

    @Override
    public List<ColumnOriginScan> visitTableFunction(TableFunctionPrel prel, Integer idx) {
      if (prel.isDataScan()) {
        // End here. Operators below this point are possibly dealing with manifests.
        return ImmutableList.of(new ColumnOriginScan(prel,
          prel.getRowType().getFieldNames().get(idx)));
      } else {
        return ImmutableList.of();
      }
    }

    private List<ColumnOriginScan> visitRel(RelNode relNode, Integer idx) {
      if (relNode instanceof Prel) {
        return ((Prel) relNode).accept(this, idx);
      } else {
        return ImmutableList.of();
      }
    }
  }

  private static ExchangePrel findExchangePrel(RelNode relNode) {
    if (relNode instanceof ExchangePrel) {
      return (ExchangePrel) relNode;
    } else if (relNode instanceof ScanPrelBase) {
      return null;
    } else if(1 == relNode.getInputs().size()) {
      return findExchangePrel(relNode.getInput(0));
    } else {
      return null;
    }
  }

  private static boolean isPartitionColumn(RuntimeFilteredRel filteredRel, String fieldName) {
    TableMetadata tableMetadata = filteredRel.getTableMetadata();
    ReadDefinition readDefinition = tableMetadata.getDatasetConfig().getReadDefinition();
    if (readDefinition.getPartitionColumnsList() == null) {
      return false;
    } else {
      return !readDefinition.getPartitionColumnsList().isEmpty()
        && readDefinition.getPartitionColumnsList().contains(fieldName);
    }
  }

}
