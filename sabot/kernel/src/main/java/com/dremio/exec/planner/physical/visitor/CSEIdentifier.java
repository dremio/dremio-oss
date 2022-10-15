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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.DelegatingMetadataRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.DremioRelMetadataCache;
import com.dremio.exec.planner.physical.AggregatePrel;
import com.dremio.exec.planner.physical.BridgeExchangePrel;
import com.dremio.exec.planner.physical.BridgeReaderPrel;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionResolver;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * This class rewrites plan after eliminating common sub-expressions
 */
public class CSEIdentifier {
  private static final Logger logger = LoggerFactory.getLogger(CSEIdentifier.class);

  public static Prel embellishAfterCommonSubExprElimination(QueryContext context, Prel prel) {
    prel.getCluster().invalidateMetadataQuery();
    Map<Long, List<Prel>> candidates = findCandidateCommonSubExpressions(context, prel);
    if(candidates.isEmpty()) {
      return prel;
    }
    logger.debug("Digest:\n{}", RelOptUtil.toString(prel));
    logger.debug("CommonSubExpressions found: {}", candidates.size());
    Map<Long, List<Prel>> filteredcandidates = removeStrictSubSets(prel, candidates);
    if(filteredcandidates.isEmpty()) {
      return prel;
    }
    logger.debug("CommonSubExpressions after filtering: {}", candidates.size());
    CostingRootNodeAndBitSet costingRootNodeAndBitSet = rewriteWithCostingNodes(context, prel, filteredcandidates);
    costingRootNodeAndBitSet.bitSet.set(0, filteredcandidates.size());
    logger.debug("Costing CommonSubExpression:\n{}", RelOptUtil.toString(costingRootNodeAndBitSet.relNode));
    List<Integer> desirablecandidateList =
      findDesirableCandidates(costingRootNodeAndBitSet);
    if(desirablecandidateList.isEmpty()) {
      return prel;
    }
    Prel result =  findOptimalTree(context, costingRootNodeAndBitSet, desirablecandidateList);
    logger.debug("Result after CTE:\n{}", RelOptUtil.toString(prel));
    return result;
  }

  /**
   * Given that we have n CommonSubExpression candidates, we have 2^n combinations of them.
   * Go through each combination, compute cost, and keep the bitset which gives the lowest cost
   * This bitset is shared among the all CostingNodes inserted into the plan.
   * If bitset is set, the costing node becomes a plan with CommonSubExpression after calling removeCosting.
   * Otherwise, original plan will be preserved under the tree.
   *
   * @param queryContext
   * @param costingRootNodeAndBitSet
   * @return physical plan with the best estimated cost
   */
  private static Prel findOptimalTree(QueryContext queryContext,
      CostingRootNodeAndBitSet costingRootNodeAndBitSet, List<Integer> candidateFeatures) {
    OptionResolver optionResolver = queryContext.getPlannerSettings().getOptions();
    RelNode relNode = costingRootNodeAndBitSet.relNode;
    BitSet featureBitSet = costingRootNodeAndBitSet.bitSet;
    int permutations = (1 << candidateFeatures.size());
    RelMetadataQuery metadataQuery = relNode.getCluster().getMetadataQuery();
    BitSet bestPlan = new BitSet();
    RelOptCost bestCost = metadataQuery.getCumulativeCost(relNode);
    long maxPermutations = Math.min(
        (int)optionResolver.getOption(PlannerSettings.MAX_CSE_PERMUTATIONS),
        permutations);
    BitSet previousPlan = new BitSet();
    for (int i = 1; i < maxPermutations; i++) {
      try {
        // We are assuming CommonSubExpression will generally be more beneficial than not
        int inverted = permutations - i;
        updateBitSet(featureBitSet, inverted, candidateFeatures);
        resetMetadata(metadataQuery, relNode, featureBitSet, previousPlan);

        RelOptCost candidateCost = metadataQuery.getCumulativeCost(relNode);

        if (candidateCost.isLe(bestCost)) {
          bestCost = candidateCost;
          bestPlan.clear();
          bestPlan.or(featureBitSet);
        }
        previousPlan.clear();
        previousPlan.or(featureBitSet);
      } catch (UserException userException) {
        if(userException.getMessage()
            .equals(DremioRelMetadataCache.MAX_METADATA_CALL_ERROR_MESSAGE)) {
          break;
        }
      }
    }
    featureBitSet.clear();
    featureBitSet.or(bestPlan);
    relNode.getCluster().invalidateMetadataQuery();
    return (Prel) removeCosting(relNode);
  }

  private static List<Integer> findDesirableCandidates(
      CostingRootNodeAndBitSet costingRootNodeAndBitSet) {
    RelNode relNode = costingRootNodeAndBitSet.relNode;
    BitSet featureBitSet = costingRootNodeAndBitSet.bitSet;
    RelMetadataQuery metadataQuery = relNode.getCluster().getMetadataQuery();
    BitSet previousPlan = new BitSet();
    //Reset the metadata
    resetMetadata(metadataQuery, relNode, featureBitSet, previousPlan);
    featureBitSet.clear();
    RelOptCost baseCost = metadataQuery.getCumulativeCost(relNode);
    List<Integer> goodcandidates = new ArrayList<>();
    for (int i = 0; i < costingRootNodeAndBitSet.numberOfFeatures; i++) {
      try {
        // We are assuming CommonSubExpression will generally be more beneficial than not
        featureBitSet.clear();
        featureBitSet.set(i);
        resetMetadata(metadataQuery, relNode, featureBitSet, previousPlan);

        RelOptCost candidateCost = metadataQuery.getCumulativeCost(relNode);

        if (candidateCost.isLe(baseCost)) {
          goodcandidates.add(i);
        }
        previousPlan.clear();
        previousPlan.or(featureBitSet);
      } catch (UserException userException) {
        if(userException.getMessage()
          .equals(DremioRelMetadataCache.MAX_METADATA_CALL_ERROR_MESSAGE)) {
          break;
        }
      }
    }
    featureBitSet.clear();
    return goodcandidates;
  }

  private static void updateBitSet(BitSet bitSet, int value, List<Integer> candidateFeatures) {
    for (int i = 0; i < candidateFeatures.size(); i++) {

      bitSet.set(candidateFeatures.get(i), isFeatureSet(value, i));
    }
  }

  private static boolean isFeatureSet(int value, int index) {
    return 1 == ((value >> index) & 1);
  }

  private static RelNode removeCosting(RelNode relNode) {
    if (relNode instanceof CostingNode) {
      CostingNode costingNode = (CostingNode) relNode;
      logger.debug("Costing Node: {}", costingNode.bitSet.get(costingNode.index));
      return removeCosting(((CostingNode) relNode).currentRel());
    } else {
      return relNode.copy(relNode.getTraitSet(),
        relNode.getInputs().stream()
          .map(CSEIdentifier::removeCosting)
          .collect(ImmutableList.toImmutableList()));
    }
  }

  /**
   * Remove candidates strictly contained by another CommonSubExpression candidate
   * @param root
   * @param candidates
   * @return
   */
  private static Map<Long, List<Prel>> removeStrictSubSets(Prel root,
      Map<Long, List<Prel>> candidates) {

    List<Long> nonSubsets = root.accept(new StrictSubsetRemover(candidates), ImmutableSet.of(0));
    Set<Long> nonStrictSubsetSet = ImmutableSet.copyOf(nonSubsets);

    return candidates.entrySet().stream()
      .filter(e -> nonStrictSubsetSet.contains(e.getKey()))
      .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static CostingRootNodeAndBitSet rewriteWithCostingNodes(QueryContext context, Prel prel, Map<Long, List<Prel>> digestToCommonSubExpressions) {
    RelMetadataQuery relMetadataQuery = prel.getCluster().getMetadataQuery();
    BitSet bitSet = new BitSet();
    RelNode relNode = prel.accept(new RewritePrelVisitor<Void>() {
      final Map<Long, UuidAndIndex> digestToUuidAndIndex = new HashMap<>();

      @Override
      public RelNode visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
        Long prelDigest = MoreRelOptUtil.longHashCode(prel);

        if (!digestToCommonSubExpressions.containsKey(prelDigest)) {
          return super.visitPrel(prel, value);
        } else if (digestToUuidAndIndex.containsKey(prelDigest)) {
          UuidAndIndex uuidAndIndex = digestToUuidAndIndex.get(prelDigest);
          Prel child = ((Prel) prel.getInput());

          // for the duplicate sub-trees,
          // - add a bridge reader below the top-level exchange
          // - delete the rest of the child sub-tree

          // compute schema for the BridgeReader
          BatchSchema schema = lookupSchema(child);
          Prel readerChild = new BridgeReaderPrel(child.getCluster(),
            child.getTraitSet(),
            child.getRowType(),
            relMetadataQuery.getRowCount(child),
            schema,
            Long.toHexString(uuidAndIndex.uuid));
          RelNode rewrittenOffNode = ((Prel)prel.getInput()).accept(this, value);
          CostingNode costingNode = new CostingNode(bitSet, uuidAndIndex.index,
            rewrittenOffNode, readerChild);
          return prel.copy(prel.getTraitSet(), ImmutableList.of(costingNode));
        } else {
          RelNode child = ((Prel) prel.getInput()).accept(this, value);
          UuidAndIndex uuidAndIndex =
            new UuidAndIndex(
              prelDigest,
              digestToUuidAndIndex.size());
          digestToUuidAndIndex.put(prelDigest, uuidAndIndex);

          // add bridge exchange between the top-level exchange and it's receiver.
          Prel newPrel = new BridgeExchangePrel(child.getCluster(), child.getTraitSet(),
            child,
            Long.toHexString(uuidAndIndex.uuid));
          CostingNode costingNode = new CostingNode(bitSet,uuidAndIndex.index,
            child, newPrel);
          return prel.copy(prel.getTraitSet(), ImmutableList.of(costingNode));
        }
      }

      private BatchSchema lookupSchema(Prel prel) {
        // This is need to get row type for execution, we do not have calcite to arrow row type
        // conversion.
        try {
          PhysicalPlanCreator planCreator =
            new PhysicalPlanCreator(context, PrelSequencer.getIdMap(prel));
          return prel.getPhysicalOperator(planCreator).getProps().getSchema();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }, null);

    return new CostingRootNodeAndBitSet(relNode, bitSet, digestToCommonSubExpressions.size());
  }

  public static Map<Long, List<Prel>> findCandidateCommonSubExpressions(QueryContext queryContext, Prel p) {
    final DeriveExchangeDigestVisitor deriveExchangeDigest = new DeriveExchangeDigestVisitor(queryContext.getOptions());
    p.accept(deriveExchangeDigest, null);
    return deriveExchangeDigest.getDigestToPrels().entrySet().stream()
      .filter( e -> 1 < e.getValue().size())
      .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static class CostingNode extends AbstractRelNode implements DelegatingMetadataRel {
    private final BitSet bitSet;
    private final int index;
    private final RelNode offNode;
    private final RelNode onNode;

    public CostingNode(BitSet bitSet,
        int index,
        RelNode offNode,
        RelNode onNode) {
      super(onNode.getCluster(), onNode.getTraitSet());
      this.bitSet = bitSet;
      this.index = index;
      this.offNode = offNode;
      this.onNode = onNode;
    }

    @Override
    public List<RelNode> getInputs() {
      return ImmutableList.of(currentRel());
    }

    @Override
    public RelNode getMetadataDelegateRel() {
      return currentRel();
    }

    @Override
    public String getDigest() {
      return "CostingNode(" + currentRel().getDigest() + ')';
    }

    @Override
    public void explain(RelWriter pw) {
      pw.input("input", currentRel())
        .itemIf("alternate", true, bitSet.get(index))
        .done(this);
    }

    @Override
    protected RelDataType deriveRowType() {
      return currentRel().getRowType();
    }

    private RelNode currentRel() {
      if(bitSet.get(index)) {
        return onNode;
      } else {
        return offNode;
      }
    }
  }

  private static class UuidAndIndex {
    final long uuid;
    final int index;

    public UuidAndIndex(long uuid, int index) {
      this.uuid = uuid;
      this.index = index;
    }
  }

  /**
   * The root node for costing along with a big set
   */
  private static class CostingRootNodeAndBitSet {
    final RelNode relNode;
    final BitSet bitSet;
    final int numberOfFeatures;

    public CostingRootNodeAndBitSet(RelNode relNode, BitSet bitSet, int numberOfFeatures) {
      this.relNode = relNode;
      this.bitSet = bitSet;
      this.numberOfFeatures = numberOfFeatures;
    }
  }

  private static class DeriveExchangeDigestVisitor extends BasePrelVisitor<Boolean, Void, RuntimeException> {
    private final Map<Long, List<Prel>> digestToPrels = new LinkedHashMap<>();
    private final boolean enableHeuristicFilter;
    private final boolean requireHeuristicAgg;
    private final boolean requireHeuristicJoin;
    private final boolean requireHeuristicFilter;

    public DeriveExchangeDigestVisitor(OptionResolver optionResolver) {
      this.enableHeuristicFilter = optionResolver.getOption(PlannerSettings.ENABLE_CSE_HEURISTIC_FILTER);
      this.requireHeuristicAgg = optionResolver.getOption(PlannerSettings.ENABLE_CSE_HEURISTIC_REQUIRE_AGGREGATE);
      this.requireHeuristicJoin = optionResolver.getOption(PlannerSettings.ENABLE_CSE_HEURISTIC_REQUIRE_JOIN);
      this.requireHeuristicFilter = optionResolver.getOption(PlannerSettings.ENABLE_CSE_HEURISTIC_REQUIRE_FILTER);
    }

    public Map<Long, List<Prel>> getDigestToPrels() {
      return digestToPrels;
    }

    @Override
    public Boolean visitPrel(Prel prel, Void notUsed) throws RuntimeException {
      boolean value = !enableHeuristicFilter;
      for(Prel sub: prel) {
        value |= sub.accept(this, notUsed);
      }
      return value;
    }

    @Override
    public Boolean visitExchange(ExchangePrel prel, Void notUsed) throws RuntimeException {
      if (prel instanceof BridgeExchangePrel) {
        return false;
      }

      if (visitPrel(prel, notUsed)) {
        digestToPrels.computeIfAbsent(MoreRelOptUtil.longHashCode(prel), digest -> new ArrayList<>()).add(prel);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Boolean visitAggregate(AggregatePrel prel, Void notUsed) throws RuntimeException {
      return visitPrel(prel, notUsed) || requireHeuristicAgg;
    }

    @Override public Boolean visitJoin(JoinPrel prel, Void notUsed) throws RuntimeException {
      return visitPrel(prel, notUsed) || requireHeuristicJoin;
    }

    @Override public Boolean visitFilter(FilterPrel prel, Void notUsed) throws RuntimeException {
      return visitPrel(prel, notUsed)  || requireHeuristicFilter;
    }
  }


  private static class StrictSubsetRemover
    extends BasePrelVisitor<List<Long>, Set<Integer>, RuntimeException> {
    private final Map<Long, List<Prel>> candidates;

    public StrictSubsetRemover(Map<Long, List<Prel>> candidates) {
      this.candidates = candidates;
    }

    @Override
    public List<Long> visitPrel(Prel prel, Set<Integer> seen) throws RuntimeException {
      long prelDigest = MoreRelOptUtil.longHashCode(prel);
      int numCandidatesOnThisPrel = candidates.getOrDefault(prelDigest, ImmutableList.of()).size();
      if(seen.contains(numCandidatesOnThisPrel)) {
        // This candidate is strictly contained by another candidate.
        // Do not add this candidate and keep finding.
        return recurse(prel, seen).build();
      } else {
        // This candidate is not strictly contained by another candidate.
        // Add this candidate and keep finding.
        return recurse(prel, appendTo(seen, numCandidatesOnThisPrel))
          .add(prelDigest)
          .build();
      }
    }

    private ImmutableList.Builder<Long> recurse(Prel prel, Set<Integer> seen) {
      ImmutableList.Builder<Long> foundNonDuplicates = ImmutableList.builder();
      for (Prel sub: prel) {
        foundNonDuplicates.addAll(sub.accept(this, seen));
      }
      return foundNonDuplicates;
    }

    private Set<Integer> appendTo(Set<Integer> values, int value) {
      return ImmutableSet.<Integer>builder()
        .addAll(values)
        .add(value)
        .build();
    }
  }

  private static boolean resetMetadata(RelMetadataQuery query,
      RelNode relNode, BitSet current, BitSet previous) {
    boolean found;
    if(relNode instanceof CostingNode) {
      //If this costing node has been toggled then the metadata needs to be reset for it and all
      //nodes above it
      CostingNode costingNode = (CostingNode) relNode;
      found = Boolean.logicalXor(
        current.get(costingNode.index), previous.get(costingNode.index));
    } else {
      found = false;
    }
    for (RelNode sub : relNode.getInputs()) {
      found |= resetMetadata(query, sub, current, previous);
    }
    if(found) {
      query.clearCache(relNode);
    }
    return found;
  }

}
