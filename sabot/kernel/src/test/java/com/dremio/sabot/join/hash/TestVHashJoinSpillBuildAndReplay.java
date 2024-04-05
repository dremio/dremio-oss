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
package com.dremio.sabot.join.hash;

import static com.dremio.sabot.Fixtures.struct;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.toUnionCell;
import static com.dremio.sabot.Fixtures.tr;
import static com.dremio.sabot.Fixtures.tuple;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.spill.VectorizedSpillingHashJoinOperator;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Test join with build, followed by spill & then, replay.
public class TestVHashJoinSpillBuildAndReplay extends TestVHashJoinSpill {
  private final OptionManager options = testContext.getOptions();
  private final int minReserve = VectorizedSpillingHashJoinOperator.MIN_RESERVE;

  @Override
  @Before
  public void before() {
    options.setOption(
        OptionValue.createBoolean(
            OptionValue.OptionType.SYSTEM, HashJoinOperator.ENABLE_SPILL.getOptionName(), true));
    options.setOption(
        OptionValue.createLong(
            OptionValue.OptionType.SYSTEM,
            ExecConstants.TARGET_BATCH_RECORDS_MAX.getOptionName(),
            65535));
    // If this option is set, the operator starts with a DiskPartition. This forces the code-path of
    // spill write,
    // read and replay, thus testing the recursion & replay code.
    options.setOption(
        OptionValue.createString(
            OptionValue.OptionType.SYSTEM,
            HashJoinOperator.TEST_SPILL_MODE.getOptionName(),
            "buildAndReplay"));
    options.setOption(
        OptionValue.createLong(
            OptionValue.OptionType.SYSTEM, HashJoinOperator.NUM_PARTITIONS.getOptionName(), 4));
    VectorizedSpillingHashJoinOperator.MIN_RESERVE = 7 * 1024 * 1024;
  }

  @Override
  @After
  public void after() {
    options.setOption(HashJoinOperator.ENABLE_SPILL.getDefault());
    options.setOption(ExecConstants.TARGET_BATCH_RECORDS_MAX.getDefault());
    options.setOption(HashJoinOperator.TEST_SPILL_MODE.getDefault());
    options.setOption(HashJoinOperator.NUM_PARTITIONS.getDefault());
    VectorizedSpillingHashJoinOperator.MIN_RESERVE = minReserve;
  }

  @Override
  @Test
  public void manyColumns() throws Exception {
    baseManyColumns();
  }

  /**
   * Test Hash Join Spill for two tables containing following columns - integer, list of integers ,
   * list of strings
   *
   * @throws Exception
   */
  @Test
  public void testListColumns() throws Exception {

    final JoinInfo joinInfo =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("id_left"), f("id_right"))),
            JoinRelType.LEFT);

    // header of expected joined table
    final Fixtures.HeaderRow joinedHeader =
        th("id_right", "ints_right", "strings_right", "id_left", "ints_left", "strings_left");

    final int numberOfRows = 1;
    final Fixtures.DataRow[] joinedData = getDataWithListVector(numberOfRows);

    // expected joined table.
    final Fixtures.Table expected = t(joinedHeader, false, joinedData);

    // validate joined data against expected table
    validateDual(
        joinInfo.operator,
        joinInfo.clazz,
        new ListColumnsGenerator<>(getTestAllocator(), numberOfRows, 0, "left"),
        new ListColumnsGenerator<>(getTestAllocator(), numberOfRows, 0, "right"),
        50,
        expected);
  }

  @Test
  public void testUnionColumns() throws Exception {

    final JoinInfo joinInfo =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("id_left"), f("id_right"))),
            JoinRelType.LEFT);

    // header of expected joined table
    // union column header in probe table
    final Fixtures.ComplexColumnHeader leftUnionHeader =
        struct("union_left", ImmutableList.of("int", "float"));
    // union column header in build table
    final Fixtures.ComplexColumnHeader rightUnionHeader =
        struct("union_right", ImmutableList.of("int", "float"));

    final Fixtures.HeaderRow joinedHeader =
        th("id_right", rightUnionHeader, "id_left", leftUnionHeader);

    final int numberOfRows = 1;
    final Fixtures.DataRow[] joinedData = getDataWithUnionVector(numberOfRows);

    // expected joined table.
    final Fixtures.Table expected = t(joinedHeader, false, joinedData);

    // validate joined data against expected table
    validateDual(
        joinInfo.operator,
        joinInfo.clazz,
        new UnionColumnGenerator<>(getTestAllocator(), numberOfRows, 0, "left"),
        new UnionColumnGenerator<>(getTestAllocator(), numberOfRows, 0, "right"),
        50,
        expected);
  }

  /**
   * Test Hash Join Spill for tables containing struct columns
   *
   * @throws Exception
   */
  @Test
  public void testStructColumns() throws Exception {

    final JoinInfo joinInfo =
        getJoinInfo(
            Arrays.asList(new JoinCondition("EQUALS", f("id_left"), f("id_right"))),
            JoinRelType.LEFT);

    // struct column header in probe table
    final Fixtures.ComplexColumnHeader leftStructHeader =
        struct("struct_left", ImmutableList.of("child_string", "child_int"));
    // struct column header in build table
    final Fixtures.ComplexColumnHeader rightStructHeader =
        struct("struct_right", ImmutableList.of("child_string", "child_int"));

    // header of expected joined table
    Fixtures.HeaderRow joinedHeader =
        th("id_right", "int_right", rightStructHeader, "id_left", "int_left", leftStructHeader);

    final int numberOfRows = 1;
    final Fixtures.DataRow[] joinedData = getDataWithStructVector(numberOfRows);

    // expected joined table.
    final Fixtures.Table expected = t(joinedHeader, false, joinedData);

    // validate joined data against expected table
    validateDual(
        joinInfo.operator,
        joinInfo.clazz,
        new MixedColumnGenerator<>(getTestAllocator(), numberOfRows, 0, "left"),
        new MixedColumnGenerator<>(getTestAllocator(), numberOfRows, 0, "right"),
        50,
        expected);
  }

  /**
   * Creates data rows with following columns - int, int, struct{int, string}, int, int, struct{int,
   * string}
   *
   * @param numberOfRows
   * @return
   */
  protected Fixtures.DataRow[] getDataWithStructVector(final int numberOfRows) {
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[numberOfRows];
    for (int i = 0; i < numberOfRows; i++) {
      final Fixtures.Cell[] leftStructCell = tuple(Integer.toString(i), i);
      final Fixtures.Cell[] rightStructCell =
          tuple(Integer.toString(i + numberOfRows), i + numberOfRows);
      rows[i] = tr(i, i + numberOfRows, rightStructCell, i, i, leftStructCell);
    }
    return rows;
  }

  /**
   * Creates data rows with following sequence of columns - Int, Union<Int, Float>, Int, Union<Int,
   * Float>
   *
   * @param numberOfRows
   * @return
   */
  protected Fixtures.DataRow[] getDataWithUnionVector(final int numberOfRows) {
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[numberOfRows];
    final Map<ArrowType, Boolean> unionTypes = new HashMap<>();
    for (int i = 0; i < numberOfRows; i++) {
      unionTypes.clear();
      if (i % 2 == 0) {

        unionTypes.put(new ArrowType.Int(32, true), true);
        unionTypes.put(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false);

        Fixtures.UnionCell uCell = toUnionCell(i, unionTypes);
        rows[i] = tr(i, uCell, i, uCell);
      } else {
        unionTypes.put(new ArrowType.Int(32, true), false);
        unionTypes.put(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), true);
        Fixtures.UnionCell uCell = toUnionCell((float) i, unionTypes);
        rows[i] = tr(i, uCell, i, uCell);
      }
    }
    return rows;
  }

  /**
   * Generates data rows containing following columns - integer, list of integers, list of strings,
   * integer, list of integers, list of strings
   *
   * @param numberOfRows
   * @return
   */
  protected Fixtures.DataRow[] getDataWithListVector(final int numberOfRows) {

    final List<List<Integer>> intValues = getIntList(numberOfRows);
    final List<List<Text>> stringValues = getStringList(numberOfRows);
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[numberOfRows];
    for (int i = 0; i < numberOfRows; i++) {

      rows[i] =
          tr(i, intValues.get(i), stringValues.get(i), i, intValues.get(i), stringValues.get(i));
    }
    return rows;
  }

  private static List<List<Integer>> getIntList(final int size) {
    final List<List<Integer>> listOfLists = new ArrayList<>(size);
    final int listSize = 5;
    for (int i = 0; i < size; i++) {

      final List<Integer> list = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(j);
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }

  private static List<List<Text>> getStringList(int size) {
    final List<List<Text>> listOfLists = new ArrayList<>(size);
    final int listSize = 5;

    for (int i = 0; i < size; i++) {
      final List<Text> list = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(new Text(Integer.toString(j)));
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }
}
