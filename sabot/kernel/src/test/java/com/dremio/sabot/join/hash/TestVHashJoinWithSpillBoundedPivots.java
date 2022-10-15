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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.core.JoinRelType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

public class TestVHashJoinWithSpillBoundedPivots extends BaseTestOperator {
  private final OptionManager options = testContext.getOptions();

  @Before
  public void before() {
    options.setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, HashJoinOperator.ENABLE_SPILL.getOptionName(), true));
    options.setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, ExecConstants.TARGET_BATCH_RECORDS_MAX.getOptionName(), 65535));
  }

  @After
  public void after() {
    options.setOption(HashJoinOperator.ENABLE_SPILL.getDefault());
    options.setOption(ExecConstants.TARGET_BATCH_RECORDS_MAX.getDefault());
  }

  @Test
  public void largePivotsWithFixedKeys() throws Exception {
    BaseTestJoin.JoinInfo joinInfo = getJoinInfo(
      Arrays.asList(
        new JoinCondition("EQUALS", f("a0"), f("b0")),
        new JoinCondition("EQUALS", f("a1"), f("b1")),
        new JoinCondition("EQUALS", f("a2"), f("b2")),
        new JoinCondition("EQUALS", f("a3"), f("b3")),
        new JoinCondition("EQUALS", f("a4"), f("b4")),
        new JoinCondition("EQUALS", f("a5"), f("b5")),
        new JoinCondition("EQUALS", f("a6"), f("b6")),
        new JoinCondition("EQUALS", f("a7"), f("b7")),
        new JoinCondition("EQUALS", f("a8"), f("b8")),
        new JoinCondition("EQUALS", f("a9"), f("b9"))
      ),
      JoinRelType.INNER);

    // 2000 (batchSize) * 16*10 (size of 10 decimal keys) > 256K (page size)
    final int batchSize = 2000;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[batchSize];
    for (int i = 0; i < batchSize; i++) {
      leftRows[i] = tr((long)i,
        BigDecimal.valueOf(0), BigDecimal.valueOf(i), BigDecimal.valueOf(i*2), BigDecimal.valueOf(i*3),
        BigDecimal.valueOf(i*4), BigDecimal.valueOf(i*5), BigDecimal.valueOf(i*6), BigDecimal.valueOf(i*7),
        BigDecimal.valueOf(i*8), BigDecimal.valueOf(i*9));
      rightRows[i] = tr((long)(i + 11),
        BigDecimal.valueOf(0), BigDecimal.valueOf(i), BigDecimal.valueOf(i*2), BigDecimal.valueOf(i*3),
        BigDecimal.valueOf(i*4), BigDecimal.valueOf(i*5), BigDecimal.valueOf(i*6), BigDecimal.valueOf(i*7),
        BigDecimal.valueOf(i*8), BigDecimal.valueOf(i*9));
      expectedRows[i] = tr((long)i+11,
        BigDecimal.valueOf(0), BigDecimal.valueOf(i), BigDecimal.valueOf(i*2), BigDecimal.valueOf(i*3),
        BigDecimal.valueOf(i*4), BigDecimal.valueOf(i*5), BigDecimal.valueOf(i*6), BigDecimal.valueOf(i*7),
        BigDecimal.valueOf(i*8), BigDecimal.valueOf(i*9),
        (long)i,
        BigDecimal.valueOf(0), BigDecimal.valueOf(i), BigDecimal.valueOf(i*2), BigDecimal.valueOf(i*3),
        BigDecimal.valueOf(i*4), BigDecimal.valueOf(i*5), BigDecimal.valueOf(i*6), BigDecimal.valueOf(i*7),
        BigDecimal.valueOf(i*8), BigDecimal.valueOf(i*9)
        );
    }

    final Fixtures.Table left = t(th("a", "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"), leftRows);
    final Fixtures.Table right = t(th("b", "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"), rightRows);
    final Fixtures.Table expected = t(th("b", "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "a", "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"),
      expectedRows).orderInsensitive();

    validateDual(
      joinInfo.operator, joinInfo.clazz,
      left.toGenerator(getTestAllocator()),
      right.toGenerator(getTestAllocator()),
      batchSize, expected);
  }

  @Test
  public void largePivotsWithVarKey() throws Exception {
    BaseTestJoin.JoinInfo joinInfo = getJoinInfo(
      Arrays.asList(
        new JoinCondition("EQUALS", f("a"), f("b")),
        new JoinCondition("EQUALS", f("aString"), f("bString"))
      ),
      JoinRelType.INNER);

    // 1000 (batchSize) * 1000 (size of each string key) > 256K (bounded pivot size)
    final int batchSize = 1000;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[batchSize];
    for (int i = 0; i < batchSize; i++) {
      String largePaddedStr = String.format("%1000d", i);
      leftRows[i] = tr((long)i, i+1, largePaddedStr, BigDecimal.valueOf(i+3));
      rightRows[i] = tr((long)i, i+11, largePaddedStr, BigDecimal.valueOf(i+13));
      expectedRows[i] = tr((long)i, i+11, largePaddedStr, BigDecimal.valueOf(i+13),
        (long)i, i+1, largePaddedStr, BigDecimal.valueOf(i+3));
    }

    final Fixtures.Table left = t(th("a", "aInt", "aString", "aDecimal"), leftRows);
    final Fixtures.Table right = t(th("b", "bInt", "bString", "bDecimal"), rightRows);
    final Fixtures.Table expected = t(th("b", "bInt", "bString", "bDecimal", "a", "aInt", "aString", "aDecimal"), expectedRows).orderInsensitive();

    validateDual(
      joinInfo.operator, joinInfo.clazz,
      left.toGenerator(getTestAllocator()),
      right.toGenerator(getTestAllocator()),
      batchSize, expected);
  }

  BaseTestJoin.JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type) {
    OpProps propsWithLimit = OpProps.prototype();
    return new BaseTestJoin.JoinInfo(VectorizedSpillingHashJoinOperator.class, new HashJoinPOP(propsWithLimit, null, null, conditions, null, type, true, null));
  }
}
