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

import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.sabot.join.BaseTestJoin;
import com.dremio.sabot.op.join.vhash.VectorizedHashJoinOperator;

public class TestVHashJoin extends BaseTestJoin {

  @Override
  protected JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type, Set<Integer> buildProjected, Set<Integer> probeProjected) {
    return new JoinInfo(VectorizedHashJoinOperator.class, new HashJoinPOP(PROPS, null, null, conditions, type, true, null));
  }

  @Test
  public void manyColumns() throws Exception {
    baseManyColumns();
  }

  @Test
  public void manyColumnsDecimal() throws Exception {
    baseManyColumnsDecimal();
  }
}
