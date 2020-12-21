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
package com.dremio.exec.planner;

import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.ACCELERATOR_STORAGEPLUGIN_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Does a bottom up build of a map that maps the reflection id to the best plan that uses the reflection
 */
public class CheapestPlanWithReflectionVisitor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CheapestPlanWithReflectionVisitor.class);

  private final RelNode root;
  private final VolcanoPlanner planner;

  private Map<RelNode,Result> map = new HashMap<>();

  private Set<RelNode> currentlyVisiting = new HashSet<>();

  private static final Map<String,RelCostPair> EMPTY_MAP = ImmutableMap.of();
  private static final Result EMPTY_RESULT = new Result(null, null, EMPTY_MAP);

  private boolean tooDeepFailure = false;
  private int MAX_DEPTH = PlannerSettings.MAX_RECURSION_STACK_DEPTH;

  public CheapestPlanWithReflectionVisitor(VolcanoPlanner planner) {
    this.root = planner.getRoot();
    this.planner = planner;
  }

  public Map<String,RelNode> getBestPlansWithReflections() {
    Result r = visit(root, 0);

    ImmutableMap.Builder<String,RelNode> builder = ImmutableMap.builder();

    if (tooDeepFailure) {
      return builder.build();
    }

    for (Entry<String,RelCostPair> e : r.bestWithReflectionMap.entrySet()) {
      builder.put(e.getKey(), convertRelSubsets(e.getValue().rel));
    }
    return builder.build();
  }

  private RelNode convertRelSubsets(RelNode root) {
    return root.accept(new RoutingShuttle() {
      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof RelSubset) {
          return visit(((RelSubset) other).getBest());
        }
        return super.visit(other);
      }
    });
  }

  private Result visit(final RelNode p, int depth) {
    if (depth > MAX_DEPTH) {
      tooDeepFailure = true;
      logger.debug("Traversal is too deep, aborting");
      return null;
    }
    if (tooDeepFailure) {
      return null;
    }
    Result result = map.get(p);
    if (result != null) {
      return result;
    }
    if (p instanceof RelSubset) {
      RelSubset subset = (RelSubset) p;
      RelNode best = subset.getBest();

      if (best == null) {
        result = EMPTY_RESULT;
        map.put(p, result);
        return result;
      }

      RelOptCost bestCost = planner.getCost(subset, null);

      Map<String,RelCostPair> bestWithReflectionMap = new HashMap<>();

      for (RelNode rel : subset.getRelList()) {
        // this check is to avoid infinite loops, since there are cycles in the graph
        // if rel is an AbstractConverter, cost is inf anyway; plus, it'll throw ClassCastException
        if (currentlyVisiting.contains(rel) || rel instanceof AbstractConverter) {
          continue;
        }
        currentlyVisiting.add(rel);
        Result r;
        try {
          r = visit(rel, depth + 1);
        } finally {
          currentlyVisiting.remove(rel);
        }
        for (String reflection : r.bestWithReflectionMap.keySet()) {
          RelCostPair currentBestCostForReflection = bestWithReflectionMap.get(reflection);
          if (currentBestCostForReflection == null || r.bestWithReflectionMap.get(reflection).cost.isLt(currentBestCostForReflection.getValue())) {
            bestWithReflectionMap.put(reflection, r.bestWithReflectionMap.get(reflection));
          }
        }
      }

      result = new Result(best, bestCost, bestWithReflectionMap);
      map.put(p, result);
      return result;
    }

    if (p.getInputs().size() == 0) {
      RelOptCost cost = p.getCluster().getMetadataQuery().getNonCumulativeCost(p);

      if (p instanceof TableScan && p.getTable().getQualifiedName().get(0).equals(ACCELERATOR_STORAGEPLUGIN_NAME)) {
        TableScan tableScan = (TableScan) p;
        String reflection = tableScan.getTable().getQualifiedName().get(1);
        result = new Result(tableScan, cost, ImmutableMap.of(reflection, RelCostPair.of(p, cost)));
        map.put(p, result);
        return result;
      } else {
        result = new Result(p, cost, EMPTY_MAP);
        map.put(p, result);
        return result;
      }
    }

    List<RelNode> oldInputs = p.getInputs();

    List<Result> results = new ArrayList<>();
    for (int i = 0; i < oldInputs.size(); i++) {
      RelNode oldInput = oldInputs.get(i);
      currentlyVisiting.add(oldInput);
      try {
        results.add(visit(oldInput, depth + 1));
      } finally {
        currentlyVisiting.remove(oldInput);
      }
    }

    if (Iterables.any(results, new Predicate<Result>() {
      @Override
      public boolean apply(@Nullable Result r) {
        return r.best == null;
      }
    })) {
      result = EMPTY_RESULT;
      map.put(p, result);
      return result;
    }

    RelNode best = p.copy(p.getTraitSet(), Lists.transform(results, new Function<Result, RelNode>() {
      @Override
      public RelNode apply(Result r) {
        return r.best;
      }
    }));

    RelOptCost bestCost = null;
    for (Result r : results) {
      if (bestCost == null) {
        bestCost = r.bestCost;
      } else {
        bestCost = bestCost.plus(r.bestCost);
      }
    }

    Set<String> reflections = FluentIterable.from(results)
      .transformAndConcat(new Function<Result, Iterable<String>>() {
        @Override
        public Iterable<String> apply(Result result) {
          return result.bestWithReflectionMap.keySet();
        }
      })
      .toSet();

    Map<String,RelCostPair> bestWithReflectionMap = new HashMap<>();

    for (final String reflection : reflections) {
      int bestAlternative = -1;
      RelOptCost bestCostDiff = null;
      // for RelNode with multiple inputs, if more than one uses the reflection but have worst cost, we only want to
      // choose the input that has the smallest cost penalty, since the ultimate goal is to find the cheapest plan that
      // uses the reflection
      for (Ord<Result> r : Ord.zip(results)) {
        if (r.e.bestWithReflectionMap.get(reflection) != null) {
          if (bestAlternative == -1) {
            bestAlternative = r.i;
            bestCostDiff = r.e.costDiff(reflection);
          } else {
            if (r.e.costDiff(reflection).isLt(bestCostDiff)) {
              bestAlternative = r.i;
              bestCostDiff = r.e.costDiff(reflection);
            }
          }
        }
      }

      if (bestAlternative > -1) {
        final int alternateIndex = bestAlternative;
        RelNode bestWithReflection = p.copy(p.getTraitSet(), Lists.transform(Ord.zip(results), new Function<Ord<Result>, RelNode>() {
          @Override
          public RelNode apply(Ord<Result> r) {
            RelCostPair pair = r.e.bestWithReflectionMap.get(reflection);
            if (r.i == alternateIndex || (pair != null && pair.cost.isLt(r.e.bestCost))) {
              return pair.rel;
            } else {
              return r.e.best;
            }
          }
        }));
        RelOptCost relCost = p.getCluster().getMetadataQuery().getNonCumulativeCost(p);

        bestWithReflectionMap.put(reflection, RelCostPair.of(bestWithReflection, bestCost.plus(relCost)));
      }
    }

    result = new Result(best, bestCost, bestWithReflectionMap);

    map.put(p, result);
    return result;
  }

  /**
   * This class holds the best plan and the best cost for each reflection at every point in the graph
   */
  private static class Result {
    private final RelNode best;
    private final RelOptCost bestCost;

    private final Map<String,RelCostPair> bestWithReflectionMap;

    private Result(RelNode best, RelOptCost bestCost, Map<String,RelCostPair> bestWithReflectionMap) {
      this.best = best;
      this.bestCost = bestCost;
      this.bestWithReflectionMap = bestWithReflectionMap;
    }

    private RelOptCost costDiff(String reflection) {
      RelOptCost costWithReflection = bestWithReflectionMap.get(reflection).cost;
      if (costWithReflection == null) {
        return null;
      }
      return costWithReflection.minus(bestCost);
    }
  }

  private static class RelCostPair extends Pair<RelNode,RelOptCost> {
    public RelNode rel;
    public RelOptCost cost;

    private RelCostPair(RelNode rel, RelOptCost cost) {
      super(rel, cost);
      this.rel = rel;
      this.cost = cost;
    }

    private static RelCostPair of(RelNode rel, RelOptCost cost) {
      return new RelCostPair(rel, cost);
    }
  }
}
