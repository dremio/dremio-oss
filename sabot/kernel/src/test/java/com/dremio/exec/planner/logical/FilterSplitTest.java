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
import static com.dremio.test.dsl.RexDsl.lt;
import static com.dremio.test.dsl.RexDsl.or;
import static com.dremio.test.dsl.RexDsl.varcharInput;
import static com.dremio.test.scaffolding.ScaffoldingRel.TYPE_FACTORY;

import java.util.BitSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.logical.partition.FindPartitionConditions;
import com.dremio.test.GoldenFileTestBuilder;

public class FilterSplitTest {

  private static final RexBuilder REX_BUILDER = new DremioRexBuilder(TYPE_FACTORY);
  @Test
  public void goldenTest() {
    new GoldenFileTestBuilder<>(this::transform, rex -> GoldenFileTestBuilder.MultiLineString.create(rex.toString()))
      .add("simpleCompound: (a < 1 AND dir0 in (2,3))",
        and(
          lt(c(0), literal(1)),
          or(
            eq(c(1), literal(2)),
            eq(c(1), literal(3)))))
      .add("badFunc (dir0 || 1)", fn(cs(0), cs(1)))
      .add("twoLevelDir: (dir0 = 1 and dir1 = 2) OR (dir0 = 3 and dir1 = 4)",
        or(
          and(
            eq(c(1), literal(1)),
            eq(c(2), literal(2))),
          and(
            eq(c(1), literal(3)),
            eq(c(2), literal(4)))))
      .add("badOr: (dir0 = 1 and dir1 = 2) OR (a < 5)",
        or(
          and(
            eq(c(1), literal(1)),
            eq(c(2), literal(2))),
          lt(c(0), literal(5))))
      .add("disjunctiveNormalForm (a, dir0) IN ((0, 1), (2, 3))",
        or(
          and(
            eq(c(0), literal(0)),
            eq(c(1), literal( 1))),
          and(
            eq(c(0), literal(2)),
            eq(c(1), literal(3)))))
      .add("Large DNF (a, dir0) IN (....)",
        or(
          IntStream.range(0, 100)
            .mapToObj(i ->
              and(eq(c(0), literal(i)), eq(c(1), literal(i))))
            .collect(Collectors.toList())))
      .runTests();
  }

  public String transform (RexNode rexNode){
    BitSet bs = new BitSet();
    bs.set(1);
    bs.set(2);
    FindPartitionConditions c = new FindPartitionConditions(bs, REX_BUILDER);
    c.analyze(rexNode);
    RexNode partNode = c.getFinalCondition();
    if(partNode != null) {
      return partNode.toString();
    } else {
      return null;
    }
  }

  private static RexNode fn(RexNode...nodes){
    return REX_BUILDER.makeCall(SqlStdOperatorTable.CONCAT, nodes);
  }

  private static RexNode c(int index){
    return intInput(index);
  }

  private RexNode cs(int index){
    return varcharInput(index);
  }
}
