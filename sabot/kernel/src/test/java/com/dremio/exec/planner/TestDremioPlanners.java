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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.test.DremioTest;

/**
 * Tests to check common features of Dremio planners
 */
public class TestDremioPlanners {
  private static class NoneRel extends AbstractRelNode {
    public NoneRel(RelOptCluster cluster) {
      super(cluster, cluster.traitSetOf(Convention.NONE));
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    @Override
    protected RelDataType deriveRowType() {
      final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
      return new RelDataTypeFactory.Builder(getCluster().getTypeFactory())
          .add("none", typeFactory.createJavaType(Void.TYPE))
          .build();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).item("id", id);
    }
  }

  private static class LoopRule extends RelOptRule {
    public LoopRule() {
      super(operand(NoneRel.class, none()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      // Wait a bit to avoid looping too many times
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      call.transformTo(new NoneRel(call.getRelList().get(0).getCluster()));
    }
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  public PlannerSettings getSettings(long timeoutMillis, int maxNodes) {
    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOptionValidatorListing()).thenReturn(mock(OptionValidatorListing.class));

    OptionValue plannerTimeout = OptionValue.createLong(OptionType.QUERY, "planner.timeout_per_phase_ms", timeoutMillis);
    OptionValue plannerMaxNodes = OptionValue.createLong(OptionType.QUERY, "planner.max_nodes_per_plan", maxNodes);

    OptionList optionList = new OptionList();
    optionList.add(plannerTimeout);
    optionList.add(plannerMaxNodes);

    when(optionManager.getOption("planner.timeout_per_phase_ms")).thenReturn(plannerTimeout);
    when(optionManager.getOption("planner.max_nodes_per_plan")).thenReturn(plannerMaxNodes);
    when(optionManager.getNonDefaultOptions()).thenReturn(optionList);

    return new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionManager, null);
  }

  @Test
  public void testHepPlannerCancelFlag() {

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(new LoopRule());
    DremioHepPlanner planner = new DremioHepPlanner(builder.build(), getSettings(100, 25_000), new DremioCost.Factory(), PlannerPhase.LOGICAL, new MatchCountListener(0, 0, 0));

    checkCancelFlag(planner);
  }


  @Test
  public void checkThrowOnMaxNodes() throws Exception {

    DremioVolcanoPlanner planner = DremioVolcanoPlanner.of(new DremioCost.Factory(), getSettings(60_000, 10), a -> {}, null);
    planner.setPlannerPhase(PlannerPhase.LOGICAL);
    planner.addRule(new LoopRule());
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(SqlTypeFactoryImpl.INSTANCE));
    RelNode root = new NoneRel(cluster);
    expectedException.expectMessage("Job was cancelled because the query went beyond system capacity during query planning.");
    planner.setRoot(root);
    planner.findBestExp();
  }

  @Test
  public void testVolcanoPlannerCancelFlag() {

    DremioVolcanoPlanner planner = DremioVolcanoPlanner.of(new DremioCost.Factory(), getSettings(100, 25_000), a -> {}, null);
    planner.setPlannerPhase(PlannerPhase.LOGICAL);
    planner.addRule(new LoopRule());

    checkCancelFlag(planner);
  }

  private void checkCancelFlag(RelOptPlanner planner) {
    expectedException.expect(UserException.class);
    expectedException.expectMessage("Query was cancelled because planning time exceeded");
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(SqlTypeFactoryImpl.INSTANCE));
    RelNode root = new NoneRel(cluster);
    planner.setRoot(root);
    planner.findBestExp();
  }

}
