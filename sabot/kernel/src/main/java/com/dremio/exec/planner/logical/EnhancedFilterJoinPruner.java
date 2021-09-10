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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Pruner used in {@link EnhancedFilterJoinRule} to avoid pushing down predicates that have already
 * been pushed, as well as prune child nodes that are superset of others.
 */
public final class EnhancedFilterJoinPruner {

  private EnhancedFilterJoinPruner() {}

  /**
   * Prune if there is superset relationship among child nodes, e.x.:
   *  (a and b and c) or (a and b) -> a and b
   *  (a or b or c) and (a or b) -> a or b
   * @param rexNode RexNode to prune, should be canonicalized
   * @param toLeaf Whether to prune recursively to leaf nodes
   * @param rexBuilder RexBuilder
   * @return Pruned RexNode
   */
  public static RexNode pruneSuperset(RexNode rexNode, boolean toLeaf, RexBuilder rexBuilder) {
    switch (rexNode.getKind()) {
      case AND:
      case OR: {
        SqlKind childSqlKind = rexNode.getKind() == SqlKind.AND ? SqlKind.OR : SqlKind.AND;

        // Get child nodes
        List<RexNode> childNodes;
        if (toLeaf) {
          childNodes = MoreRelOptUtil.conDisjunctions(rexNode)
            .stream()
            .map(childNode -> pruneSuperset(childNode, toLeaf, rexBuilder))
            .collect(Collectors.toList());
        } else {
          childNodes = MoreRelOptUtil.conDisjunctions(rexNode);
        }

        // Deduplicate
        Set<String> childNodesStringSet = Sets.newHashSet();
        List<RexNode> childNodesDeduplicated = Lists.newArrayList();
        for (RexNode childNode: childNodes) {
          String childNodeString = childNode.toString();
          if (!childNodesStringSet.contains(childNodeString)) {
            childNodesDeduplicated.add(childNode);
            childNodesStringSet.add(childNodeString);
          }
        }

        // Prune superset
        List<RexNode> childNodesPruned = childNodesDeduplicated
          .stream()
          .filter(childNode -> {
            for (RexNode nodeToCompare : childNodesDeduplicated) {
              if (childNode == nodeToCompare) {
                continue;
              }
              if (isSuperset(childNode, nodeToCompare, childSqlKind)) {
                return false;
              }
            }
            return true;
          })
          .collect(Collectors.toList());

        return MoreRelOptUtil.composeConDisjunction(rexBuilder, childNodesPruned, false,
          rexNode.getKind());
      }
      default:
        return rexNode;
    }
  }

  /**
   * Prune pushdown predicates so that already pushed predicates are removed.
   * @param predicates RexNode to prune, should be canonicalized
   * @param mq MetadataQuery
   * @param relNode RelNode that {@code predicates} are pushed to
   * @param rexBuilder RexBuilder
   * @return Pruned pushdown predicates
   */
  public static RexNode prunePushdown(RexNode predicates, RelMetadataQuery mq,
    RelNode relNode, RexBuilder rexBuilder) {
    RelOptPredicateList pulledUpPredicates = mq.getPulledUpPredicates(relNode);
    Set<String> pushedFiltersString = pulledUpPredicates.pulledUpPredicates
      .stream()
      .map(RexNode::toString)
      .collect(Collectors.toSet());

    List<RexNode> prunedPredicates = Lists.newArrayList();
    for (RexNode conjunct: RelOptUtil.conjunctions(predicates)) {
      if (!pushedFiltersString.contains(conjunct.toString())) {
        prunedPredicates.add(conjunct);
      }
    }
    return RexUtil.composeConjunction(rexBuilder, prunedPredicates, false);
  }

  /**
   * Check whether the child nodes of e1 is a superset of those of e2.
   * @param e1 Candidate RexNode, should be canonicalized
   * @param e2 Candidate RexNode, should be canonicalized
   * @param sqlKind The format of e1, e2, and common child nodes, should be AND/OR
   * @return Whether e1 is a superset of e2
   */
  private static boolean isSuperset(RexNode e1, RexNode e2, SqlKind sqlKind) {
    if (!(sqlKind == SqlKind.AND || sqlKind == SqlKind.OR)) {
      return false;
    }

    Set<String> childNodesString1;
    if (e1.getKind() == sqlKind) {
      childNodesString1 = MoreRelOptUtil.conDisjunctions(e1)
        .stream()
        .map(RexNode::toString)
        .collect(Collectors.toSet());
    } else {
      childNodesString1 = Sets.newHashSet(e1.toString());
    }

    Set<String> childNodesString2;
    if (e2.getKind() == sqlKind) {
      childNodesString2 = MoreRelOptUtil.conDisjunctions(e2)
        .stream()
        .map(RexNode::toString)
        .collect(Collectors.toSet());
    } else {
      childNodesString2 = Sets.newHashSet(e2.toString());
    }

    return childNodesString1.containsAll(childNodesString2);
  }
}
