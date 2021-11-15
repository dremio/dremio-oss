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
package com.dremio.exec.planner.acceleration;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener.SelfFlatteningRel;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Represents a location where the query was expanded from a
 * from a default raw reflection of a VDS.
 */
public class ExpansionNode extends SingleRel implements CopyToCluster, SelfFlatteningRel {

  private final NamespaceKey path;
  private final boolean contextSensitive;

  protected ExpansionNode(NamespaceKey path, RelDataType rowType, RelOptCluster cluster, RelTraitSet traits, RelNode input, boolean contextSensitive) {
    super(cluster, traits, input);
    this.path = path;
    this.contextSensitive = contextSensitive;
    this.rowType = rowType;
  }

  public static RelNode wrap(NamespaceKey path, RelNode node, RelDataType rowType, boolean contextSensitive, boolean isDefault) {
    if (isDefault) {
      return new DefaultExpansionNode(path, rowType, node.getCluster(), node.getTraitSet(), node, contextSensitive);
    } else {
      return new ExpansionNode(path, rowType, node.getCluster(), node.getTraitSet(), node, contextSensitive);
    }
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    return new ExpansionNode(path, rowType, copier.getCluster(), copier.copyOf(getTraitSet()), getInput().accept(copier), contextSensitive);
  }

  @Override
  public void flattenRel(RelStructuredTypeFlattener flattener) {
    flattener.rewriteGeneric(this);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .item("path", path.toUnescapedString())
      .itemIf("contextSensitive", contextSensitive, contextSensitive);
  }

  public boolean isContextSensitive() {
    return contextSensitive;
  }

  public boolean isDefault() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ExpansionNode(path, rowType, this.getCluster(), traitSet, inputs.get(0), contextSensitive);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return input.estimateRowCount(mq);
  }

  public NamespaceKey getPath() {
    return path;
  }

  public static RelNode removeFromTree(RelNode tree) {
    return tree.accept(new RelShuttleImpl() {

      @Override
      public RelNode visit(RelNode other) {
        if(other instanceof ExpansionNode) {
          return ((ExpansionNode) super.visit(other)).getInput();
        }
        return super.visit(other);
      }});
  }

  public static RelNode removeParentExpansionNodes(NamespaceKey pathFilter, RelNode node) {
    RelNode rel = node.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if(other instanceof ExpansionNode) {
          ExpansionNode e = (ExpansionNode) other;
          if(e.getPath().equals(pathFilter)) {
            return e.copy(e.getTraitSet(), e.getInputs());
          } else {
            RelNode input = e.getInput().accept(this);
            if (input == e.getInput()) {
              return e;
            } else {
              return input;
            }
          }
        }
        return super.visit(other);
      }
    });
    return rel;
  }

  /**
   * Count ExpansionNodes at each depth for the given rel node and collect the results in a map.
   * First count the top level ENs, then the first level nested ENs, and so on. The count is done
   * on the level of ExpansionNode and not on the RelNode.
   *
   * @param relNode          Input rel node
   * @param nodeCountAtDepth A map to keep track of node count at each depth.
   *                         Key = Depth, Value = Expansion nodes at that depth
   * @param depth            Current depth
   */
  public static void countExpansionNodes(RelNode relNode, Map<Integer, Integer> nodeCountAtDepth, Pointer<Integer> depth) {
    if (relNode == null) {
      return;
    }

    if (relNode instanceof RelSubset) {
      RelSubset subset = (RelSubset) relNode;
      countExpansionNodes(subset.getBest(), nodeCountAtDepth, depth);
      return;
    }

    if (relNode instanceof ExpansionNode) {
      // There is an ExpansionNode at this depth
      nodeCountAtDepth.put(depth.value, nodeCountAtDepth.getOrDefault(depth.value, 0) + 1);
      depth.value++;
    }

    for (RelNode node : relNode.getInputs()) {
      countExpansionNodes(node, nodeCountAtDepth, depth);
    }

    if (relNode instanceof ExpansionNode) {
      depth.value--;
    }
  }

  public static RelNode removeAllButRoot(RelNode tree) {

    return tree.accept(new RelShuttleImpl() {

      private boolean alreadyFound = false;

      @Override
      public RelNode visit(RelNode other) {
        if(other instanceof ExpansionNode) {
          if(alreadyFound) {
            return ((ExpansionNode) other).getInput();
          } else {
            alreadyFound = true;
          }
        }
        return super.visit(other);
      }});
  }

  public static List<ExpansionNode> findNodes(RelNode node, Predicate<ExpansionNode> predicate){
    return StatelessRelShuttleImpl.find(node, r -> r instanceof ExpansionNode)
        .map(r -> ((ExpansionNode) r))
        .filter(predicate)
        .collect(Collectors.toList());
  }

  /**
   * Determine whether there are expansion nodes in the provided tree that are marked as context sensitive.
   * @param node Tree to search.
   * @return True if the tree contains at least one ExpansionNode that is context sensitive.
   */
  public static boolean isContextSensitive(RelNode node) {
    return !findNodes(node, t -> t.isContextSensitive()).isEmpty();
  }
}
