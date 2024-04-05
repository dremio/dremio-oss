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

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared.PlannerPhaseRulesStats;
import com.dremio.options.OptionResolver;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for handling configuration, instrumentation and logging each execution of the {@link
 * DremioHepPlanner}.
 */
public class HepPlannerRunner {
  public static final Logger LOGGER = LoggerFactory.getLogger(HepPlannerRunner.class);
  private final PlannerSettings plannerSettings;
  private final OptionResolver optionResolver;
  private final ConstExecutor constExecutor;
  private final RelOptCostFactory relOptCostFactory;
  private final PlannerStatsReporter plannerStatsReporter;

  public HepPlannerRunner(
      PlannerSettings plannerSettings,
      OptionResolver optionResolver,
      ConstExecutor constExecutor,
      RelOptCostFactory relOptCostFactory,
      PlannerStatsReporter plannerStatsReporter) {
    this.plannerSettings = plannerSettings;
    this.optionResolver = optionResolver;
    this.constExecutor = constExecutor;
    this.relOptCostFactory = relOptCostFactory;
    this.plannerStatsReporter = plannerStatsReporter;
  }

  public RelNode transform(RelNode relNode, HepPlannerRunnerConfig hepPlannerRunnerConfig) {
    Stopwatch stopwatch = Stopwatch.createStarted();

    int matchLimit = (int) optionResolver.getOption(PlannerSettings.HEP_PLANNER_MATCH_LIMIT);
    boolean verbose = optionResolver.getOption(PlannerSettings.VERBOSE_PROFILE);

    final HepProgramBuilder hepPgmBldr = new HepProgramBuilder();

    hepPgmBldr.addMatchLimit(matchLimit);

    MatchCountListener matchCountListener =
        new MatchCountListener(
            MoreRelOptUtil.countRelNodes(relNode),
            Iterables.size(hepPlannerRunnerConfig.getRuleSet()),
            matchLimit,
            Thread.currentThread().getName());

    hepPgmBldr.addMatchOrder(hepPlannerRunnerConfig.getHepMatchOrder());
    hepPgmBldr.addRuleCollection(Lists.newArrayList(hepPlannerRunnerConfig.getRuleSet()));

    final DremioHepPlanner hepPlanner =
        new DremioHepPlanner(
            hepPgmBldr.build(),
            plannerSettings,
            relOptCostFactory,
            hepPlannerRunnerConfig.getPlannerPhase(), // planner_phase
            matchCountListener);
    hepPlanner.setExecutor(constExecutor);

    // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
    RelOptCluster cluster = relNode.getCluster();
    cluster.invalidateMetadataQuery();

    // Begin planning
    hepPlanner.setRoot(relNode);

    RelNode transformedRelNode = hepPlanner.findBestExp();

    List<PlannerPhaseRulesStats> rulesBreakdownStats = matchCountListener.getRulesBreakdownStats();
    long millisTaken = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    if (verbose) {
      LOGGER.debug("Phase: {}", hepPlannerRunnerConfig.plannerPhase);
      LOGGER.debug("Time taken by the phase (calculated by the stopwatch): {} ms", millisTaken);
      LOGGER.debug(matchCountListener.toString());
    }

    plannerStatsReporter.report(
        hepPlannerRunnerConfig, millisTaken, relNode, transformedRelNode, rulesBreakdownStats);
    return transformedRelNode;
  }

  public static class HepPlannerRunnerConfig {
    private HepMatchOrder hepMatchOrder = HepMatchOrder.ARBITRARY;
    private PlannerPhase plannerPhase = null;
    private RuleSet ruleSet = null;

    public HepPlannerRunnerConfig getHepMatchOrder(HepMatchOrder hepMatchOrder) {
      this.hepMatchOrder = hepMatchOrder;
      return this;
    }

    public HepMatchOrder getHepMatchOrder() {
      return hepMatchOrder;
    }

    public PlannerPhase getPlannerPhase() {
      return plannerPhase;
    }

    public HepPlannerRunnerConfig setPlannerPhase(PlannerPhase plannerPhase) {
      this.plannerPhase = plannerPhase;
      return this;
    }

    public RuleSet getRuleSet() {
      return ruleSet;
    }

    public HepPlannerRunnerConfig setRuleSet(RuleSet ruleSet) {
      this.ruleSet = ruleSet;
      return this;
    }
  }

  @FunctionalInterface
  public interface PlannerStatsReporter {
    void report(
        HepPlannerRunnerConfig config,
        long millisTaken,
        RelNode input,
        RelNode output,
        List<PlannerPhaseRulesStats> rulesBreakdownStats);
  }
}
