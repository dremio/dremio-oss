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
package com.dremio.sabot.join.merge;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.config.MergeJoinPOP;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.join.BaseTestJoin.JoinInfo;
import com.dremio.sabot.op.join.merge.MergeJoinOperator;

public class TestMergeJoinRandom extends BaseTestOperator {

  private static final int DEFAULT_SMALL_BATCH = 20;

  private static final long RAND_SEED = 10000;
  private static final Random rand = new Random(RAND_SEED);

  private static final int NUM_GROUPS = 100;
  private static final int MAX_GROUP_SIZE = 30;

  private ArrayList<DataRow> rowsLeft;
  private ArrayList<DataRow> rowsRight;
  private Map<Integer, Pair<Integer, Integer>> leftRowCount;
  private Map<Integer, Pair<Integer, Integer>> rightRowCount;
  private Table left;
  private Table right;

  @Before
  public void beforeTest() {
    leftRowCount = new HashMap<>();
    rightRowCount = new HashMap<>();

    rowsLeft = new ArrayList<>();
    rowsRight = new ArrayList<>();

    for (int i = 0; i < NUM_GROUPS; i++) {
      int numOfFalse = rand.nextBoolean() ? 0 : rand.nextInt(MAX_GROUP_SIZE);
      int numOfTrue = rand.nextBoolean() ? 0 : rand.nextInt(MAX_GROUP_SIZE);

      leftRowCount.put(i, Pair.of(numOfFalse, numOfTrue));

      for (int j = 0; j < numOfFalse; j++) {
        rowsLeft.add(tr(false, i));
      }

      for (int j = 0; j < numOfTrue; j++) {
        rowsLeft.add(tr(true, i));
      }
    }

    for (int i = 0; i < NUM_GROUPS; i++) {
      int numOfFalse = rand.nextBoolean() ? 0 : rand.nextInt(MAX_GROUP_SIZE);
      int numOfTrue = rand.nextBoolean() ? 0 : rand.nextInt(MAX_GROUP_SIZE);

      rightRowCount.put(i,  Pair.of(numOfFalse, numOfTrue));

      for (int j = 0; j < numOfFalse; j++) {
        rowsRight.add(tr(false, i));
      }

      for (int j = 0; j < numOfTrue; j++) {
        rowsRight.add(tr(true, i));
      }
    }

    left = t(
      th("bool1", "name1"),
      rowsLeft.toArray(new DataRow[0])
    );

    right = t(
      th("bool2", "name2"),
      rowsRight.toArray(new DataRow[0])
    );
  }

  protected JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type) {
    return new JoinInfo(MergeJoinOperator.class, new MergeJoinPOP(PROPS, null, null, conditions, type));
  }

  private void noNullMultipleRowsData(JoinInfo info, Table expected) throws Exception {

    validateDual(
      info.operator, info.clazz,
      left.toGenerator(getTestAllocator()),
      right.toGenerator(getTestAllocator()),
      DEFAULT_BATCH, expected);
    validateDual(
      info.operator, info.clazz,
      left.toGenerator(getTestAllocator()),
      right.toGenerator(getTestAllocator()),
      DEFAULT_SMALL_BATCH, expected);
  }

  @Test
  public void noNullEquivalenceInnerSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))),
        JoinRelType.INNER);
    ArrayList<DataRow> rows = new ArrayList<>();

    for (int i = 0; i < NUM_GROUPS; i++) {
      final int leftFalse = leftRowCount.get(i).getLeft();
      final int leftTrue = leftRowCount.get(i).getRight();

      final int rightFalse = rightRowCount.get(i).getLeft();
      final int rightTrue = rightRowCount.get(i).getRight();

      for (int j = 0; j < leftFalse; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, false, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, false, i));
        }
      }

      for (int j = 0; j < leftTrue; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, true, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, true, i));
        }
      }
    }

    final Table expected = t(th("bool2", "name2", "bool1", "name1"), rows.toArray(new DataRow[0]));
    noNullMultipleRowsData(joinInfo, expected);
  }

  @Test
  public void noNullEquivalenceLeftSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))),
        JoinRelType.LEFT);
    ArrayList<DataRow> rows = new ArrayList<>();

    for (int i = 0; i < NUM_GROUPS; i++) {
      final int leftFalse = leftRowCount.get(i).getLeft();
      final int leftTrue = leftRowCount.get(i).getRight();

      final int rightFalse = rightRowCount.get(i).getLeft();
      final int rightTrue = rightRowCount.get(i).getRight();

      for (int j = 0; j < leftFalse; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, false, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, false, i));
        }

        if (rightFalse + rightTrue == 0) {
          rows.add(tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT, false, i));
        }
      }

      for (int j = 0; j < leftTrue; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, true, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, true, i));
        }

        if (rightFalse + rightTrue == 0) {
          rows.add(tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT, true, i));
        }
      }
    }

    final Table expected = t(th("bool2", "name2", "bool1", "name1"), rows.toArray(new DataRow[0]));
    noNullMultipleRowsData(joinInfo, expected);
  }

  @Test
  public void noNullEquivalenceRightSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))),
        JoinRelType.RIGHT);
    ArrayList<DataRow> rows = new ArrayList<>();

    for (int i = 0; i < NUM_GROUPS; i++) {
      final int leftFalse = leftRowCount.get(i).getLeft();
      final int leftTrue = leftRowCount.get(i).getRight();

      final int rightFalse = rightRowCount.get(i).getLeft();
      final int rightTrue = rightRowCount.get(i).getRight();

      for (int j = 0; j < leftFalse; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, false, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, false, i));
        }
      }

      for (int j = 0; j < leftTrue; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, true, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, true, i));
        }
      }

      if (leftFalse + leftTrue == 0) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT));
        }
      }
    }

    final Table expected = t(th("bool2", "name2", "bool1", "name1"), rows.toArray(new DataRow[0]));
    noNullMultipleRowsData(joinInfo, expected);
  }

  @Test
  public void noNullEquivalenceFullSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))),
        JoinRelType.FULL);
    ArrayList<DataRow> rows = new ArrayList<>();

    for (int i = 0; i < NUM_GROUPS; i++) {
      final int leftFalse = leftRowCount.get(i).getLeft();
      final int leftTrue = leftRowCount.get(i).getRight();

      final int rightFalse = rightRowCount.get(i).getLeft();
      final int rightTrue = rightRowCount.get(i).getRight();

      for (int j = 0; j < leftFalse; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, false, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, false, i));
        }

        if (rightFalse + rightTrue == 0) {
          rows.add(tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT, false, i));
        }
      }

      for (int j = 0; j < leftTrue; j++) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, true, i));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, true, i));
        }

        if (rightFalse + rightTrue == 0) {
          rows.add(tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT, true, i));
        }
      }

      if (leftFalse + leftTrue == 0) {
        for (int k = 0; k < rightFalse; k++) {
          rows.add(tr(false, i, Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT));
        }

        for (int k = 0; k < rightTrue; k++) {
          rows.add(tr(true, i, Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT));
        }
      }
    }

    final Table expected = t(th("bool2", "name2", "bool1", "name1"), rows.toArray(new DataRow[0]));
    noNullMultipleRowsData(joinInfo, expected);
  }

}
