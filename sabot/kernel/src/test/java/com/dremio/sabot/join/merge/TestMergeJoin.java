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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.config.MergeJoinPOP;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.join.BaseTestJoin.JoinInfo;
import com.dremio.sabot.join.hash.EmptyGenerator;
import com.dremio.sabot.op.join.merge.MergeJoinOperator;

public class TestMergeJoin extends BaseTestOperator {

  private static final int DEFAULT_SMALL_BATCH = 2;

  protected JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type) {
    return new JoinInfo(MergeJoinOperator.class, new MergeJoinPOP(PROPS, null, null, conditions, type));
  }

  private void nullLowSingleRowsData(JoinInfo info, Table expected) throws Exception {
    final Table left = t(
      th("bool1", "name1"),
      tr(true, Fixtures.NULL_VARCHAR),
      tr(true, "a1"),
      tr(false, "a2"),
      tr(true, "a3"),
      tr(false, "a4"),
      tr(false, "a6")
    );

    final Table right = t(
      th("bool2", "name2"),
      tr(true, Fixtures.NULL_VARCHAR),
      tr(false, "a1"),
      tr(true, "a2"),
      tr(true, "a4"),
      tr(true, "a5"),
      tr(false, "a6")
    );
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

  private void nullHighSingleRowsData(JoinInfo info, Table expected) throws Exception {
    final Table left = t(
      th("bool1", "name1"),
      tr(true, "a1"),
      tr(false, "a2"),
      tr(true, "a3"),
      tr(false, "a4"),
      tr(false, "a6"),
      tr(true, Fixtures.NULL_VARCHAR)
    );

    final Table right = t(
      th("bool2", "name2"),
      tr(false, "a1"),
      tr(true, "a2"),
      tr(true, "a4"),
      tr(true, "a5"),
      tr(false, "a6"),
      tr(true, Fixtures.NULL_VARCHAR)
    );
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

  private void noNullSingleRowsData(JoinInfo info, Table expected) throws Exception {
    final Table left = t(
      th("bool1", "name1"),
      tr(true, "a1"),
      tr(false, "a2"),
      tr(true, "a3"),
      tr(false, "a4"),
      tr(false, "a6")
    );

    final Table right = t(
      th("bool2", "name2"),
      tr(false, "a1"),
      tr(true, "a2"),
      tr(true, "a4"),
      tr(true, "a5"),
      tr(false, "a6")
    );
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

  private void noNullEmptyLeftData(JoinInfo info, Table expected) throws Exception {

    final Table right = t(
      th("bool2", "name2"),
      tr(0L, 1L),
      tr(1L, 2L),
      tr(1L, 4L),
      tr(1L, 5L),
      tr(0L, 6L)
    );
    validateDual(
      info.operator, info.clazz,
      new EmptyGenerator(getTestAllocator()),
      right.toGenerator(getTestAllocator()),
      DEFAULT_BATCH, expected);
    validateDual(
      info.operator, info.clazz,
      new EmptyGenerator(getTestAllocator()),
      right.toGenerator(getTestAllocator()),
      DEFAULT_SMALL_BATCH, expected);
  }

  private void noNullEmptyRightData(JoinInfo info, Table expected) throws Exception {
    final Table left = t(
      th("bool1", "name1"),
      tr(1L, 1L),
      tr(0L, 2L),
      tr(1L, 3L),
      tr(0L, 4L),
      tr(0L, 6L)
    );

    validateDual(
      info.operator, info.clazz,
      left.toGenerator(getTestAllocator()),
      new EmptyGenerator(getTestAllocator()),
      DEFAULT_BATCH, expected);
    validateDual(
      info.operator, info.clazz,
      left.toGenerator(getTestAllocator()),
      new EmptyGenerator(getTestAllocator()),
      DEFAULT_SMALL_BATCH, expected);
  }

  private void noNullMultipleRowsData(JoinInfo info, Table expected) throws Exception {
    final Table left = t(
      th("bool1", "name1"),
      tr(false, "a1"),
      tr(true, "a1"),
      tr(true, "a2"),
      tr(true, "a3"),
      tr(false, "a4"),
      tr(false, "a4")
    );

    final Table right = t(
      th("bool2", "name2"),
      tr(false, "a1"),
      tr(true, "a3"),
      tr(true, "a3"),
      tr(false, "a4"),
      tr(true, "a4"),
      tr(true, "a4"),
      tr(false, "a5")
    );
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
  public void nullEquivalenceInnerSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))), JoinRelType.INNER);
    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", true, "a1"),
          tr(true, "a2", false, "a2"),
          tr(true, "a4", false, "a4"),
          tr(false, "a6", false, "a6")
        );
      nullHighSingleRowsData(joinInfo, expected);
    }
  }

  @Test
  public void nullIsNotDistinctFromInnerSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("IS_NOT_DISTINCT_FROM", f("name1"), f("name2"))), JoinRelType.INNER);
    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", true, "a1"),
          tr(true, "a2", false, "a2"),
          tr(true, "a4", false, "a4"),
          tr(false, "a6", false, "a6"),
          tr(true, Fixtures.NULL_VARCHAR, true, Fixtures.NULL_VARCHAR)
        );
      nullHighSingleRowsData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceInnerSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))), JoinRelType.INNER);
    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", true, "a1"),
          tr(true, "a2", false, "a2"),
          tr(true, "a4", false, "a4"),
          tr(false, "a6", false, "a6")
        );
      noNullSingleRowsData(joinInfo, expected);
    }

    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", false, "a1"),
          tr(false, "a1", true, "a1"),

          tr(true, "a3", true, "a3"),
          tr(true, "a3", true, "a3"),

          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4")
        );
      noNullMultipleRowsData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceInnerEmptyTable() throws Exception{
    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("value"), f("name2"))), JoinRelType.INNER);
      final Table expected = t(
          th("bool2", "name2", "key", "value"),
          true,
          tr(0L, 0L, 0L, 0L)
        );
      noNullEmptyLeftData(joinInfo, expected);
    }

    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("value"))), JoinRelType.INNER);
      final Table expected = t(
          th("key", "value", "bool1", "name1"),
          true,
          tr(0L, 0L, 0L, 0L)
        );
      noNullEmptyRightData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceLeftEmptyTable() throws Exception{
    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("value"), f("name2"))), JoinRelType.LEFT);
      final Table expected = t(
          th("bool2", "name2", "key", "value"),
          true,
          tr(0L, 0L, 0L, 0L)
        );
      noNullEmptyLeftData(joinInfo, expected);
    }

    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("value"))), JoinRelType.LEFT);
      final Table expected = t(
          th("key", "value", "bool1", "name1"),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 1L, 1L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 0L, 2L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 1L, 3L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 0L, 4L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 0L, 6L)
        );
      noNullEmptyRightData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceRightEmptyTable() throws Exception{
    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("value"), f("name2"))), JoinRelType.RIGHT);
      final Table expected = t(
          th("bool2", "name2", "key", "value"),
          tr(0L, 1L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(1L, 2L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(1L, 4L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(1L, 5L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(0L, 6L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT)
        );
      noNullEmptyLeftData(joinInfo, expected);
    }

    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("value"))), JoinRelType.RIGHT);
      final Table expected = t(
          th("key", "value", "bool1", "name1"),
          true,
          tr(0L, 0L, 0L, 0L)
        );
      noNullEmptyRightData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceFullEmptyTable() throws Exception{
    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("value"), f("name2"))), JoinRelType.FULL);
      final Table expected = t(
          th("bool2", "name2", "key", "value"),
          tr(0L, 1L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(1L, 2L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(1L, 4L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(1L, 5L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
          tr(0L, 6L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT)
        );
      noNullEmptyLeftData(joinInfo, expected);
    }

    {
      JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("value"))), JoinRelType.FULL);
      final Table expected = t(
          th("key", "value", "bool1", "name1"),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 1L, 1L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 0L, 2L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 1L, 3L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 0L, 4L),
          tr(Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT, 0L, 6L)
        );
      noNullEmptyRightData(joinInfo, expected);
    }
  }


  @Test
  public void noNullEquivalenceLeftSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))), JoinRelType.LEFT);
    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", true, "a1"),
          tr(true, "a2", false, "a2"),
          tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR, true, "a3"),
          tr(true, "a4", false, "a4"),
          tr(false, "a6", false, "a6")
        );
      noNullSingleRowsData(joinInfo, expected);
    }

    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", false, "a1"),
          tr(false, "a1", true, "a1"),

          tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR, true, "a2"),

          tr(true, "a3", true, "a3"),
          tr(true, "a3", true, "a3"),

          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4")
        );
      noNullMultipleRowsData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceRightSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))), JoinRelType.RIGHT);
    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", true, "a1"),
          tr(true, "a2", false, "a2"),
          tr(true, "a4", false, "a4"),
          tr(true, "a5", Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR),
          tr(false, "a6", false, "a6")
        );
      noNullSingleRowsData(joinInfo, expected);
    }

    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", false, "a1"),
          tr(false, "a1", true, "a1"),

          tr(true, "a3", true, "a3"),
          tr(true, "a3", true, "a3"),

          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),

          tr(false, "a5", Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR)
        );
      noNullMultipleRowsData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceFullSingleCondition() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("name1"), f("name2"))), JoinRelType.FULL);
    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", true, "a1"),
          tr(true, "a2", false, "a2"),
          tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR, true, "a3"),
          tr(true, "a4", false, "a4"),
          tr(true, "a5", Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR),
          tr(false, "a6", false, "a6")
        );
      noNullSingleRowsData(joinInfo, expected);
    }

    {
      final Table expected = t(
          th("bool2", "name2", "bool1", "name1"),
          tr(false, "a1", false, "a1"),
          tr(false, "a1", true, "a1"),

          tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR, true, "a2"),

          tr(true, "a3", true, "a3"),
          tr(true, "a3", true, "a3"),

          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(false, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),
          tr(true, "a4", false, "a4"),

          tr(false, "a5", Fixtures.NULL_BOOLEAN, Fixtures.NULL_VARCHAR)
        );
      noNullMultipleRowsData(joinInfo, expected);
    }
  }

  @Test
  public void noNullEquivalenceInnerMultipleConditions() throws Exception{
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(
        new JoinCondition("EQUALS", f("name1"), f("name2")),
        new JoinCondition("EQUALS", f("bool1"), f("bool2"))
        ), JoinRelType.INNER);

    {
      final Table expected = t(
        th("bool2", "name2", "bool1", "name1"),
        tr(false, "a6", false, "a6")
      );
      noNullSingleRowsData(joinInfo, expected);
    }

    {
      final Table expected = t(
        th("bool2", "name2", "bool1", "name1"),
        tr(false, "a1", false, "a1"),

        tr(true, "a3", true, "a3"),
        tr(true, "a3", true, "a3"),

        tr(false, "a4", false, "a4"),
        tr(false, "a4", false, "a4")
      );
      noNullMultipleRowsData(joinInfo, expected);
    }
  }
}
