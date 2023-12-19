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

import static com.dremio.exec.planner.TestDremioPlanners.NoneRel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableSet;

public class TestDremioVolcanoPlanner {

  /**
   * Verify that we can de-reference planning objects in DremioVolcanoPlanner so that they can be garbage collected.
   */
  @Test
  public void testDispose() {
    SubstitutionProvider provider = mock(SubstitutionProvider.class);
    when(provider.findSubstitutions(any())).thenCallRealMethod();
    when(provider.getMatchedReflections()).thenReturn(ImmutableSet.of("m1"));
    DremioVolcanoPlanner planner = DremioVolcanoPlanner.of(new DremioCost.Factory(), TestDremioPlanners.getSettings(1_000, 100), provider, null);

    // Logical planning with reflections
    planner.setPlannerPhase(PlannerPhase.LOGICAL);
    planner.addRule(new LogicalRelRule());
    planner.addRule(new PhysicalRelRule());
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(SqlTypeFactoryImpl.INSTANCE));
    RelNode noneRel = new NoneRel(cluster);
    RelNode convertedRel = planner.changeTraits(noneRel, cluster.traitSetOf(Rel.LOGICAL));
    planner.setRoot(convertedRel);
    RelNode result = planner.findBestExp();
    assertEquals(Rel.LOGICAL, result.getConvention());
    assertEquals(1, planner.getMatchedReflections().size());
    assertTrue(planner.getMatchedReflections().contains("m1"));

    // Physical planning
    planner.setPlannerPhase(PlannerPhase.PHYSICAL);
    convertedRel = planner.changeTraits(result, cluster.traitSetOf(Prel.PHYSICAL));
    planner.setRoot(convertedRel);
    result = planner.findBestExp();
    assertEquals(Prel.PHYSICAL, result.getConvention());
    // Substitution doesn't happen for physical planning so we just get back same matches from logical planning
    assertEquals(1, planner.getMatchedReflections().size());
    assertTrue(planner.getMatchedReflections().contains("m1"));

    // Dispose the DremioVolcanoPlanner and verify we have cleaned stuff out
    planner.dispose();
    assertNull(planner.getExecutor());
    assertNull(planner.getOriginalRoot());
    assertNotEquals(planner.getRoot(), convertedRel); // Replaced with another dummy rel
    assertTrue(planner.getRules().isEmpty());
    assertEquals(0, planner.getMatchedReflections().size());
  }

  private static class LogicalRelRule extends RelOptRule {
    LogicalRelRule() {
      super(operand(NoneRel.class, any()));
    }

    @Override
    public Convention getOutConvention() {
      return Rel.LOGICAL;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      NoneRel noneRel = call.rel(0);
      call.transformTo(new LogicalRel(noneRel.getCluster()));
    }
  }

  private static class LogicalRel extends AbstractRelNode implements Rel {
    public LogicalRel(RelOptCluster cluster) {
      super(cluster, cluster.traitSetOf(Rel.LOGICAL));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
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

  private static class PhysicalRelRule extends RelOptRule {
    PhysicalRelRule() {
      super(operand(LogicalRel.class, any()));
    }

    @Override
    public Convention getOutConvention() {
      return Prel.PHYSICAL;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalRel logicalRel = call.rel(0);
      call.transformTo(new PhysicalRel(logicalRel.getCluster()));
    }
  }

  private static class PhysicalRel extends AbstractRelNode implements Prel {
    public PhysicalRel(RelOptCluster cluster) {
      super(cluster, cluster.traitSetOf(Prel.PHYSICAL));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
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

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      return null;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
      return null;
    }

    @Override
    public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
      return new BatchSchema.SelectionVectorMode[0];
    }

    @Override
    public BatchSchema.SelectionVectorMode getEncoding() {
      return null;
    }

    @Override
    public boolean needsFinalColumnReordering() {
      return false;
    }

    @NotNull
    @Override
    public Iterator<Prel> iterator() {
      return null;
    }
  }



}
