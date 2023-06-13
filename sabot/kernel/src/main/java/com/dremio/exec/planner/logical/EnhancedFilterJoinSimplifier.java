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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Simplifier used in {@link EnhancedFilterJoinRule} to simplify the filter after a part of it gets
 * pushed down.
 */
public final class EnhancedFilterJoinSimplifier {

  private EnhancedFilterJoinSimplifier() {}

  /**
   * In conjunction, we may simplify if one child is entirely pushed.
   * In the root level, we can remove child nodes which are entirely pushed
   * In non-root levels, whether we can remove those entirely pushed child nodes depends on ORs on
   *  the top, so we handle in {@link #simplifyDisjunction(RexBuilder, List, List, boolean)}
   */
  public static RexNode simplifyConjunction(RexBuilder rexBuilder, List<RexNode> originalFilters,
    List<Pair<RexNode, RexNode>> extractions, boolean rootLevel) {
    List<RexNode> remainingFilters = Lists.newArrayList();
    for (int i = 0; i < originalFilters.size(); ++i) {
      RexNode originalFilter = originalFilters.get(i);
      Pair<RexNode, RexNode> extraction = extractions.get(i);
      if (!(rootLevel && RexUtil.eq(originalFilter, extraction.getKey()))) {
        remainingFilters.add(extraction.getValue());
      }
    }
    return RexUtil.composeConjunction(rexBuilder, remainingFilters, false);
  }

  /**
   * In disjunction, if some children share common extractions, we can remove them from each child
   * E.x.  (a and b) or (a and c)  -> push: a  -> remaining: b or c
   * If in the root level all children are entirely pushed, then we can return {@code true}.
   */
  public static RexNode simplifyDisjunction(RexBuilder rexBuilder, List<RexNode> originalFilters,
    List<Pair<RexNode, RexNode>> extractions, boolean rootLevel) {
    // Find out all entirely pushed child nodes
    List<Pair<RexNode, RexNode>> entirelyPushedExtractions = Lists.newArrayList();
    List<Pair<RexNode, RexNode>> nonEntirelyPushedExtractions = Lists.newArrayList();
    for (int i = 0; i < originalFilters.size(); ++i) {
      RexNode originalFilter = originalFilters.get(i);
      Pair<RexNode, RexNode> extraction = extractions.get(i);
      if (RexUtil.eq(originalFilter, extraction.getKey())) {
        entirelyPushedExtractions.add(extraction);
      } else {
        nonEntirelyPushedExtractions.add(extraction);
      }
    }

    // If all extractions are entirely pushed, then return the disjunction of their remaining filters
    if (nonEntirelyPushedExtractions.isEmpty()) {
      if (rootLevel) {
        return rexBuilder.makeLiteral(true);
      } else {
        List<RexNode> remainingFilters = entirelyPushedExtractions
          .stream()
          .map(Pair::getValue)
          .collect(Collectors.toList());
        return RexUtil.composeDisjunction(rexBuilder, remainingFilters, false);
      }
    }

    // Find common extraction in non-entirely pushed child nodes
    RexNode commonExtractedFilter = null;
    for (Pair<RexNode, RexNode> extraction: nonEntirelyPushedExtractions) {
      RexNode extractedFilter = extraction.getKey();
      if (commonExtractedFilter == null) {
        commonExtractedFilter = extractedFilter;
      } else {
        commonExtractedFilter = getCommonChildNodes(commonExtractedFilter, extractedFilter,
          SqlKind.AND, rexBuilder);
      }
    }

    // If no common extraction, return the original filter
    if (commonExtractedFilter == null || commonExtractedFilter.isAlwaysTrue()) {
      return RexUtil.composeDisjunction(rexBuilder, originalFilters, false);
    } else {
      // Else, return disjunction of simplified non-entirely pushed child nodes (need to supply
      // non-common part) and entirely pushed child nodes. We need to preserve the order of child nodes
      // in the disjunction because we have checks whether a child gets entirely pushed elsewhere

      // Record the order of extractions
      Map<RexNode, Integer> extractionIndex = Maps.newHashMap();
      for (int i = 0; i < extractions.size(); ++i) {
        extractionIndex.put(extractions.get(i).getKey(), i);
      }

      // Variables to collect simplified filters
      List<RexNode> simplifiedFilters = Lists.newArrayList();
      Map<RexNode, RexNode> simplifiedToExtractionMap = Maps.newHashMap();

      // Simplify non-entirely pushed filters by common part and supply non-common part
      for (Pair<RexNode, RexNode> extraction: nonEntirelyPushedExtractions) {
        RexNode extractedFilter = extraction.getKey();
        RexNode remainingFilter = extraction.getValue();
        // Simplify non-entirely pushed filters by common part
        RexNode simplifiedFilter = removeChildNodes(remainingFilter, commonExtractedFilter,
          SqlKind.AND, rexBuilder);
        // Supply non-common part
        RexNode nonCommonSupply = removeChildNodes(extractedFilter, commonExtractedFilter,
          SqlKind.AND, rexBuilder);
        RexNode simplifiedFilterSupplied = RexUtil.composeConjunction(rexBuilder,
          Lists.newArrayList(simplifiedFilter, nonCommonSupply), false);
        simplifiedFilters.add(simplifiedFilterSupplied);
        simplifiedToExtractionMap.put(simplifiedFilterSupplied, extraction.getKey());
      }

      // Add entirely pushed filters
      for (Pair<RexNode, RexNode> extraction: entirelyPushedExtractions) {
        simplifiedFilters.add(extraction.getValue());
        simplifiedToExtractionMap.put(extraction.getValue(), extraction.getKey());
      }

      // Sort the simplified filters to the original order
      simplifiedFilters.sort(Comparator.comparingInt(f ->
        extractionIndex.get(simplifiedToExtractionMap.get(f))));
      return RexUtil.composeDisjunction(rexBuilder, simplifiedFilters, false);
    }
  }

  /**
   * Collect common child nodes from AND/OR, in the form of AND/OR itself
   * @param e1 Candidate RexNode
   * @param e2 Candidate RexNode
   * @param sqlKind The format of e1, e2, and common child nodes, should be AND/OR
   * @param rexBuilder RexBuilder
   * @return Common child nodes in the sqlKind form
   */
  private static RexNode getCommonChildNodes(RexNode e1, RexNode e2, SqlKind sqlKind,
    RexBuilder rexBuilder){
    if (!(sqlKind == SqlKind.AND || sqlKind == SqlKind.OR)) {
      return rexBuilder.makeLiteral(true);
    }

    Map<String, RexNode> childNodeMap1 = Maps.newHashMap();
    if (e1.getKind() == sqlKind) {
      MoreRelOptUtil.conDisjunctions(e1).forEach(rexNode ->
        childNodeMap1.put(rexNode.toString(), rexNode));
    } else {
      childNodeMap1.put(e1.toString(), e1);
    }

    Map<String, RexNode> childNodeMap2 = Maps.newHashMap();
    if (e2.getKind() == sqlKind) {
      MoreRelOptUtil.conDisjunctions(e2).forEach(rexNode ->
        childNodeMap2.put(rexNode.toString(), rexNode));
    } else {
      childNodeMap2.put(e2.toString(), e2);
    }

    List<RexNode> commonChildren = Lists.newArrayList();
    for (Map.Entry<String, RexNode> child : childNodeMap1.entrySet()) {
      if (childNodeMap2.containsKey(child.getKey())) {
        commonChildren.add(child.getValue());
        childNodeMap2.remove(child.getKey());
      }
    }

    return MoreRelOptUtil.composeConDisjunction(rexBuilder, commonChildren, false, sqlKind);
  }

  /**
   * Remove a subset of child nodes which belong to a parent node
   * E.x. parentNode = (a and b and c), nodeToRemove = (a and b), result = c
   * @param parentNode Node to remove child nodes from
   * @param nodeToRemove Child nodes to remove
   * @param sqlKind The format of the parent node, should be AND/OR
   * @param rexBuilder RexBuilder
   * @return The parent node after child nodes removed
   */
  private static RexNode removeChildNodes(RexNode parentNode, RexNode nodeToRemove, SqlKind sqlKind,
    RexBuilder rexBuilder) {
    if (!(sqlKind == SqlKind.AND || sqlKind == SqlKind.OR)) {
      return parentNode;
    }

    List<RexNode> childNodes;
    if (parentNode.getKind() == sqlKind) {
      childNodes = MoreRelOptUtil.conDisjunctions(parentNode);
    } else {
      childNodes = Lists.newArrayList(parentNode);
    }

    List<String> childNodesToRemoveString;
    if (nodeToRemove.getKind() == sqlKind) {
      childNodesToRemoveString = MoreRelOptUtil.conDisjunctions(nodeToRemove)
        .stream()
        .map(RexNode::toString)
        .collect(Collectors.toList());
    } else {
      childNodesToRemoveString = Lists.newArrayList(nodeToRemove.toString());
    }

    childNodes.removeIf(childNode -> childNodesToRemoveString.contains(childNode.toString()));
    return MoreRelOptUtil.composeConDisjunction(rexBuilder, childNodes, false, sqlKind);
  }
}
