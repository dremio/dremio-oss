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

import java.util.ArrayList;
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

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Represents a location where the query was expanded from a
 * from a default raw reflection of a VDS.
 */
public class ExpansionNode extends SingleRel implements CopyToCluster, SelfFlatteningRel {

  private final NamespaceKey path;
  private final boolean contextSensitive;
  private final TableVersionContext versionContext;

  protected ExpansionNode(NamespaceKey path, RelDataType rowType, RelOptCluster cluster, RelTraitSet traits, RelNode input,
                          boolean contextSensitive, TableVersionContext versionContext) {
    super(cluster, traits, input);
    this.path = path;
    this.contextSensitive = contextSensitive;
    this.rowType = rowType;
    this.versionContext = versionContext;
  }

  public static RelNode wrap(NamespaceKey path, RelNode node, RelDataType rowType, boolean contextSensitive,
                             boolean isDefault, TableVersionContext versionContext) {
    if (isDefault) {
      return new DefaultExpansionNode(path, rowType, node.getCluster(), node.getTraitSet(), node, contextSensitive, versionContext);
    } else {
      return new ExpansionNode(path, rowType, node.getCluster(), node.getTraitSet(), node, contextSensitive, versionContext);
    }
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    return new ExpansionNode(path, rowType, copier.getCluster(), copier.copyOf(getTraitSet()), getInput().accept(copier),
      contextSensitive, versionContext);
  }

  @Override
  public void flattenRel(RelStructuredTypeFlattener flattener) {
    flattener.rewriteGeneric(this);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .item("path", path.toUnescapedString())
      .itemIf("contextSensitive", contextSensitive, contextSensitive)
      .itemIf("version", versionContext, versionContext != null);
  }

  public boolean isContextSensitive() {
    return contextSensitive;
  }

  public boolean isDefault() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ExpansionNode(path, rowType, this.getCluster(), traitSet, inputs.get(0), contextSensitive, versionContext);
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

  public TableVersionContext getVersionContext() { return versionContext; }

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

  public static RelNode removeParentExpansionNodes(SubstitutionUtils.VersionedPath pathFilter, RelNode node) {
    RelNode rel = node.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if(other instanceof ExpansionNode) {
          ExpansionNode e = (ExpansionNode) other;
          SubstitutionUtils.VersionedPath path = SubstitutionUtils.VersionedPath.of(e);
          if(path.equals(pathFilter)) {
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
   * Collect ExpansionNodes at each depth for the given rel node and collect the results in a map.
   * Collection is done on the level of ExpansionNode and not on the RelNode.
   *
   * @param relNode          Input rel node
   * @param expansionsByDepth A map to keep track of nodes at each depth.
   *                         Key = Depth, Value = Expansion nodes at that depth
   * @param depth            Current depth
   */
  public static void collectExpansionsByDepth(RelNode relNode, Map<Integer, List<ExpansionNode>> expansionsByDepth, Pointer<Integer> depth) {
    if (relNode == null) {
      return;
    }

    if (relNode instanceof RelSubset) {
      RelSubset subset = (RelSubset) relNode;
      collectExpansionsByDepth(subset.getBest(), expansionsByDepth, depth);
      return;
    }

    if (relNode instanceof ExpansionNode) {
      ExpansionNode expansionNode = (ExpansionNode)relNode;
      // There is an ExpansionNode at this depth
      List<ExpansionNode> expansionNodes = expansionsByDepth.getOrDefault(depth.value, new ArrayList<>());
      expansionNodes.add(expansionNode);
      expansionsByDepth.put(depth.value, expansionNodes);
      depth.value++;
    }

    for (RelNode node : relNode.getInputs()) {
      collectExpansionsByDepth(node, expansionsByDepth, depth);
    }

    if (relNode instanceof ExpansionNode) {
      depth.value--;
    }
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
