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

import static com.dremio.test.dsl.RexDsl.and;
import static com.dremio.test.dsl.RexDsl.eq;
import static com.dremio.test.dsl.RexDsl.intInput;
import static com.dremio.test.dsl.RexDsl.literal;
import static com.dremio.test.dsl.RexDsl.or;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Test;

/** Test for {@link EnhancedFilterJoinPruner}. */
public class TestEnhancedFilterJoinPruner {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);

  @Test
  public void testPruneSupersetInAndSubsetHasMultiChild() {
    testPruneSuperset(
        and(
            or(
                eq(intInput(0), literal(10)),
                eq(intInput(1), literal(20)),
                eq(intInput(2), literal(30))),
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        true,
        "OR(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInOrSubsetHasMultiChild() {
    testPruneSuperset(
        or(
            and(
                eq(intInput(0), literal(10)),
                eq(intInput(1), literal(20)),
                eq(intInput(2), literal(30))),
            and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        true,
        "AND(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInAndSubsetHasSingleChild() {
    testPruneSuperset(
        and(
            or(
                eq(intInput(0), literal(10)),
                eq(intInput(1), literal(20)),
                eq(intInput(2), literal(30))),
            eq(intInput(0), literal(10))),
        true,
        "=($0, 10)");
  }

  @Test
  public void testPruneSupersetInOrSubsetHasSingleChild() {
    testPruneSuperset(
        or(
            and(
                eq(intInput(0), literal(10)),
                eq(intInput(1), literal(20)),
                eq(intInput(2), literal(30))),
            eq(intInput(0), literal(10))),
        true,
        "=($0, 10)");
  }

  @Test
  public void testPruneSupersetInAndSubsetSameAsSuperset() {
    testPruneSuperset(
        and(
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        true,
        "OR(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInOrSubsetSameAsSuperset() {
    testPruneSuperset(
        or(
            and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        true,
        "AND(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetMultiSupersetRelationship() {
    testPruneSuperset(
        and(
            or(
                eq(intInput(0), literal(10)),
                eq(intInput(1), literal(20)),
                eq(intInput(1), literal(30))),
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            eq(intInput(0), literal(10))),
        true,
        "=($0, 10)");
  }

  @Test
  public void testPruneSupersetToLeaf() {
    testPruneSuperset(
        and(
            or(
                eq(intInput(0), literal(10)),
                and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
            or(
                eq(intInput(0), literal(10)),
                eq(intInput(1), literal(20)),
                eq(intInput(1), literal(30))),
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        true,
        "=($0, 10)");
  }

  @Test
  public void testPruneSupersetNotToLeaf() {
    testPruneSuperset(
        and(
            or(
                eq(intInput(0), literal(10)),
                and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
            or(
                eq(intInput(0), literal(10)),
                eq(intInput(1), literal(20)),
                eq(intInput(1), literal(30))),
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        false,
        "AND(OR(=($0, 10), AND(=($0, 10), =($1, 20))), OR(=($0, 10), =($1, 20)))");
  }

  private void testPruneSuperset(RexNode rexNode, boolean toLeaf, String expectedNodeString) {
    RexNode nodePruned = EnhancedFilterJoinPruner.pruneSuperset(rexBuilder, rexNode, toLeaf);
    Assert.assertEquals(expectedNodeString, nodePruned.toString());
  }
}
