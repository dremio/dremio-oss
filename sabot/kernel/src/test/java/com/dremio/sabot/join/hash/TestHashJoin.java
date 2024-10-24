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

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.sabot.join.BaseTestJoin;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.VectorizedHashJoinOperator;
import com.dremio.sabot.op.join.vhash.spill.VectorizedSpillingHashJoinOperator;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Ignore;
import org.junit.Test;

public class TestHashJoin extends BaseTestJoin {

  @Override
  protected JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type) {
    return new JoinInfo(
        HashJoinOperator.class,
        new HashJoinPOP(PROPS, null, null, conditions, null, type, false, null));
  }

  protected JoinInfo getSpillingJoinInfo(
      List<JoinCondition> conditions,
      LogicalExpression extraCondition,
      JoinRelType type,
      int reserve,
      int limit) {
    OpProps opProps = OpProps.prototype(reserve, limit);
    return new JoinInfo(
        VectorizedSpillingHashJoinOperator.class,
        new HashJoinPOP(opProps, null, null, conditions, extraCondition, type, true, true, null));
  }

  protected JoinInfo getNoSpillJoinInfo(
      List<JoinCondition> conditions,
      LogicalExpression extraCondition,
      JoinRelType type,
      int reserve,
      int limit) {
    OpProps opProps = OpProps.prototype(reserve, limit);
    return new JoinInfo(
        VectorizedHashJoinOperator.class,
        new HashJoinPOP(opProps, null, null, conditions, extraCondition, type, true, null));
  }

  @Ignore("DX-5845")
  @Test
  public void manyColumns() throws Exception {
    baseManyColumns();
  }
}
