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

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.join.BaseTestJoin;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.spill.VectorizedSpillingHashJoinOperator;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestVHashJoinSpillWithMemoryLimit extends BaseTestOperator {
  private final OptionManager options = testContext.getOptions();
  private final int minReserve = VectorizedSpillingHashJoinOperator.MIN_RESERVE;

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
    VectorizedSpillingHashJoinOperator.MIN_RESERVE = 9 * 1024 * 1024;
  }

  @After
  public void after() {
    options.setOption(HashJoinOperator.ENABLE_SPILL.getDefault());
    options.setOption(ExecConstants.TARGET_BATCH_RECORDS_MAX.getDefault());
    VectorizedSpillingHashJoinOperator.MIN_RESERVE = minReserve;
  }

  @Test
  public void spillWithoutCarryOvers() throws Exception {
    BaseTestJoin.JoinInfo joinInfo =
        getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("a"), f("b"))), JoinRelType.INNER);

    final int batchSize = 64000;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[batchSize];
    for (int i = 0; i < batchSize; i++) {
      leftRows[i] = tr((long) i);
      rightRows[i] = tr((long) i);
      expectedRows[i] = tr((long) i, (long) i);
    }

    final Fixtures.Table left = t(th("a"), leftRows);
    final Fixtures.Table right = t(th("b"), rightRows);
    final Fixtures.Table expected = t(th("b", "a"), expectedRows).orderInsensitive();

    validateDual(
        joinInfo.operator,
        joinInfo.clazz,
        left.toGenerator(getTestAllocator()),
        right.toGenerator(getTestAllocator()),
        batchSize,
        expected);
  }

  @Test
  public void spillWithFixedLenCarryOvers() throws Exception {
    BaseTestJoin.JoinInfo joinInfo =
        getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("a"), f("b"))), JoinRelType.INNER);

    final int batchSize = 64000;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[batchSize];
    for (int i = 0; i < batchSize; i++) {
      leftRows[i] = tr((long) i, i + 1);
      rightRows[i] = tr((long) i, i + 11);
      expectedRows[i] = tr((long) i, i + 11, (long) i, i + 1);
    }

    final Fixtures.Table left = t(th("a", "aInt"), leftRows);
    final Fixtures.Table right = t(th("b", "bInt"), rightRows);
    final Fixtures.Table expected =
        t(th("b", "bInt", "a", "aInt"), expectedRows).orderInsensitive();

    validateDual(
        joinInfo.operator,
        joinInfo.clazz,
        left.toGenerator(getTestAllocator()),
        right.toGenerator(getTestAllocator()),
        batchSize,
        expected);
  }

  @Test
  public void spillWithCarryOvers() throws Exception {
    BaseTestJoin.JoinInfo joinInfo =
        getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("a"), f("b"))), JoinRelType.INNER);

    final int batchSize = 64000;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[batchSize];
    for (int i = 0; i < batchSize; i++) {
      leftRows[i] = tr((long) i, i + 1, Integer.toString(i + 2), BigDecimal.valueOf(i + 3));
      rightRows[i] = tr((long) i, i + 11, Integer.toString(i + 12), BigDecimal.valueOf(i + 13));
      expectedRows[i] =
          tr(
              (long) i,
              i + 11,
              Integer.toString(i + 12),
              BigDecimal.valueOf(i + 13),
              (long) i,
              i + 1,
              Integer.toString(i + 2),
              BigDecimal.valueOf(i + 3));
    }

    final Fixtures.Table left = t(th("a", "aInt", "aString", "aDecimal"), leftRows);
    final Fixtures.Table right = t(th("b", "bInt", "bString", "bDecimal"), rightRows);
    final Fixtures.Table expected =
        t(th("b", "bInt", "bString", "bDecimal", "a", "aInt", "aString", "aDecimal"), expectedRows)
            .orderInsensitive();

    validateDual(
        joinInfo.operator,
        joinInfo.clazz,
        left.toGenerator(getTestAllocator()),
        right.toGenerator(getTestAllocator()),
        batchSize,
        expected);
  }

  BaseTestJoin.JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type) {
    OpProps propsWithLimit = OpProps.prototype(0 /*reserve*/, 20 * 1024 * 1024);
    return new BaseTestJoin.JoinInfo(
        VectorizedSpillingHashJoinOperator.class,
        new HashJoinPOP(propsWithLimit, null, null, conditions, null, type, true, true, null));
  }
}
