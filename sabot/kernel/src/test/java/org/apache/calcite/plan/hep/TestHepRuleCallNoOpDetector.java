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
package org.apache.calcite.plan.hep;

import static com.dremio.test.dsl.RexDsl.eq;
import static com.dremio.test.dsl.RexDsl.intInput;
import static com.dremio.test.dsl.RexDsl.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.when;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import java.util.List;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestHepRuleCallNoOpDetector {

  private @Mock HepRuleCall call;

  private static RelBuilder relBuilder;
  private static RelNode rel0;
  private static RelNode rel1;

  @BeforeAll
  public static void init() {
    RexBuilder rexBuilder = new RexBuilder(JavaTypeFactoryImpl.INSTANCE);
    HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
    RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);
    relBuilder = RelBuilder.proto(Contexts.of()).create(cluster, null);
    rel0 =
        relBuilder
            .values(new String[] {"c1", "c2", "c3"}, 0, 0, 0)
            .filter(eq(intInput(0), literal(0)))
            .project(intInput(0), intInput(1))
            .build();
    rel1 =
        relBuilder
            .values(new String[] {"c1", "c2", "c3"}, 0, 0, 0)
            .project(intInput(0), intInput(1))
            .filter(eq(intInput(0), literal(0)))
            .build();
  }

  @Test
  public void testTransformationWithSameReference() {
    RelNode sameRef = rel0;
    when(call.rel(Mockito.eq(0))).thenReturn(rel0);
    when(call.getResults()).thenReturn(List.of(sameRef));
    when(call.getRule()).thenReturn(NoOpRule.Config.DEFAULT.toRule());
    assertThatCode(() -> HepRuleCallNoOpDetector.hasNoOpTransformations(call))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Attempting to transform to same object reference");
  }

  @Test
  public void testTransformationWithEquivalentRelation() {
    RelNode equivalentTransformation =
        rel0.copy(relBuilder.getCluster().traitSet(), rel0.getInputs());
    when(call.rel(Mockito.eq(0))).thenReturn(rel0);
    when(call.getResults()).thenReturn(List.of(equivalentTransformation));
    when(call.getRule()).thenReturn(NoOpRule.Config.DEFAULT.toRule());
    assertThatCode(() -> HepRuleCallNoOpDetector.hasNoOpTransformations(call))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Attempting to transform to equivalent relational expression");
  }

  @Test
  public void testTransformationWithDifferentRelation() {
    RelNode newTransformation = rel1;
    when(call.rel(Mockito.eq(0))).thenReturn(rel0);
    when(call.getResults()).thenReturn(List.of(newTransformation));
    when(call.getRule()).thenReturn(NoOpRule.Config.DEFAULT.toRule());
    assertThat(HepRuleCallNoOpDetector.hasNoOpTransformations(call)).isFalse();
  }

  private static class NoOpRule extends RelRule<NoOpRule.Config> {

    public NoOpRule(Config config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {}

    interface Config extends RelRule.Config {
      Config DEFAULT =
          RelRule.Config.EMPTY
              .withOperandSupplier(b -> b.operand(RelNode.class).anyInputs())
              .as(Config.class);

      @Override
      default RelOptRule toRule() {
        return new NoOpRule(this);
      }
    }
  }
}
