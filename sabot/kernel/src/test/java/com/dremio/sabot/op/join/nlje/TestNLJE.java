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
package com.dremio.sabot.op.join.nlje;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.join.BaseTestJoin;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;

/**
 * Test the enhanced Nested Loop Join
 */
@RunWith(Parameterized.class)
public class TestNLJE extends BaseTestJoin {

  @Parameters(name = "useVectorInput = {0}")
  public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {{true}, {false}});
  }

  private final boolean useVectorRangeInput;

  public TestNLJE(boolean useVectorRangeInput) {
    this.useVectorRangeInput = useVectorRangeInput;
  }

  @Test
  public void nljSmallBatch() throws Exception {
    final Table expected = t(
        th("r_name", "r_regionKey"),
        tr("AFRICA", 0L),
        tr("AMERICA", 0L),
        tr("ASIA", 0L),
        tr("AFRICA", 1L),
        tr("AMERICA", 1L),
        tr("ASIA", 1L),
        tr("AFRICA", 2L),
        tr("AMERICA", 2L),
        tr("ASIA", 2L),

        tr("EUROPE", 0L),
        tr("MIDDLE EAST", 0L),

        tr("EUROPE", 1L),
        tr("MIDDLE EAST", 1L),

        tr("EUROPE", 2L),
        tr("MIDDLE EAST", 2L),

        tr("AFRICA", 3L),
        tr("AMERICA", 3L),
        tr("ASIA", 3L),

        tr("AFRICA", 4L),
        tr("AMERICA", 4L),
        tr("ASIA", 4L),

        tr("EUROPE", 3L),
        tr("MIDDLE EAST", 3L),

        tr("EUROPE", 4L),
        tr("MIDDLE EAST", 4L)
        );

    validateDual(
        new NestedLoopJoinPOP(PROPS, null, null, JoinRelType.INNER, null, true, null),
        NLJEOperator.class,
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey"),
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_name"),
        3, expected);
  }


  @Test
  public void nljSingleBatch() throws Exception {

    final Table expected = t(
        th("r_name", "r_regionKey"),
        tr("AFRICA", 0L),
        tr("AMERICA", 0L),
        tr("ASIA", 0L),
        tr("EUROPE", 0L),
        tr("MIDDLE EAST", 0L),

        tr("AFRICA", 1L),
        tr("AMERICA", 1L),
        tr("ASIA", 1L),
        tr("EUROPE", 1L),
        tr("MIDDLE EAST", 1L),

        tr("AFRICA", 2L),
        tr("AMERICA", 2L),
        tr("ASIA", 2L),
        tr("EUROPE", 2L),
        tr("MIDDLE EAST", 2L),

        tr("AFRICA", 3L),
        tr("AMERICA", 3L),
        tr("ASIA", 3L),
        tr("EUROPE", 3L),
        tr("MIDDLE EAST", 3L),

        tr("AFRICA", 4L),
        tr("AMERICA", 4L),
        tr("ASIA", 4L),
        tr("EUROPE", 4L),
        tr("MIDDLE EAST", 4L)
        );

    validateDual(
        new NestedLoopJoinPOP(PROPS, null, null, JoinRelType.INNER, null, true, null),
        NLJEOperator.class,
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey"),
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_name"),
        100, expected);
  }

  private final FunctionCall getVectorOp() {
    if(useVectorRangeInput) {
      return new FunctionCall("all", Collections.emptyList());
    } else {
      return null;
    }
  }

  @Test
  public void nljBatchBoundary() throws Exception {

    DataRow dr = tr(1);

    int rows = 2047;

    DataRow[] t1Data = new DataRow[rows];

    Arrays.fill(t1Data, dr);

    final Table t1 = t(
      th("x"),
      t1Data
    );

    final Table t2 = t(
      th("y"),
      dr
    );

    DataRow expDr = tr(1,1);

    DataRow[] expectedData = new DataRow[rows];

    Arrays.fill(expectedData, expDr);

    final Table expected = t(
      th("y", "x"),
      expectedData
    );


    validateDual(
      new NestedLoopJoinPOP(PROPS, null, null, JoinRelType.INNER, null, true, null),
      NLJEOperator.class,
      t1.toGenerator(getTestAllocator()),
      t2.toGenerator(getTestAllocator()),
      2047, expected);
  }

  @Test
  public void noNullEquivalenceWithNullsLeft() {
    // disable since ordering is different.
    Assume.assumeFalse(true);
  }

  @Test
  public void noNullEquivalenceWithZeroKeyLeft() {
    // disable since ordering is different.
    Assume.assumeFalse(true);
  }

  @Test
  public void hugeBatch() {
    // disable ass this takes too long in unit tests (4B comparisons are required)
    Assume.assumeFalse(true);
  }

  @Test
   public void regionNationInner() {
    // disable since ordering is different.
    Assume.assumeFalse(true);
  }

  @Override
  public void runRightAndOuter() {
    Assume.assumeTrue(false);
  }


  @Override
  protected JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type) {
    BooleanOperator be = new BooleanOperator("booleanAnd", conditions.stream().map(c -> new FunctionCall("EQUALS".equals(c.getRelationship()) ? "=" : "is_not_distinct_from", Arrays.asList(
        new InputReference(0, (SchemaPath) c.getLeft()),
        new InputReference(1, (SchemaPath) c.getRight())
        ))).collect(Collectors.toList()));
    return new JoinInfo(NLJEOperator.class, new NestedLoopJoinPOP(PROPS, null, null, type, be, true, null));
  }

}
