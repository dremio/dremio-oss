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
package com.dremio.sabot.project;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.physical.config.Project;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.ProjectOperator;
import com.google.common.collect.ImmutableList;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;

public class TestProject extends BaseTestOperator {

  @Test
  public void basicProjectionLifecycle() throws Exception {
    Project conf = new Project(PROPS, null, Collections.singletonList(n("c_custkey")));
    assertSingleInput(conf, ProjectOperator.class, TpchTable.CUSTOMER, 0.1, null, 4095);
  }

  @Test
  public void doubleColumnProjection() throws Exception {
    Project conf = new Project(PROPS, null, ImmutableList.of(n("c_custkey"), n("c_custkey")));
    assertSingleInput(conf, ProjectOperator.class, TpchTable.CUSTOMER, 0.1, null, 4095);
  }

  @Test
  public void projectRegions() throws Exception {
    Project conf = new Project(PROPS, null, Arrays.asList(n("r_regionkey"), n("r_name"), n("r_comment")));
    final Table expected = t(
        th("r_regionkey", "r_name", "r_comment"),
        tr(0L, "AFRICA", "cajole boldly quickly special packages. bold instructions above the final accounts haggle about the slyly express f"),
        tr(1L, "AMERICA", "ges cajole? ironic packages alo"),
        tr(2L, "ASIA", ". quickly even requests impress"),
        tr(3L, "EUROPE", "d have to snooze blithely according to the fu"),
        tr(4L, "MIDDLE EAST", "sual excuses. final asymptotes haggle blithely regular, ironic ideas. quickly unusual requests wake quickly.")
        );

    validateSingle(conf, ProjectOperator.class, TpchTable.REGION, 0.1, expected);
  }


  @Test
  public void nativeSum() throws Exception {
    Project conf = new Project(PROPS, null, Arrays.asList(n("r_regionkey + r_regionkey", "sum")));
    final Table expected = t(
        th("sum"),
        tr(0L),
        tr(2L),
        tr(4L),
        tr(6L),
        tr(8L)
        );

    validateSingle(conf, ProjectOperator.class, TpchTable.REGION, 0.1, expected);
  }

  @Test
  public void optimisationInProject() throws Exception {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    int i = 8000;
    sb.append("r_regionkey < 10 AND r_regionkey > 20");
    while (i-- > 0) {
      sb.append(" AND r_regionkey < ").append(random.nextInt());
    }

    Project conf = new Project(PROPS, null, Arrays.asList(n(sb.toString(), "A"), n("r_regionkey + r_regionkey", "B")));
    final Table expected = t(
      th("A", "B"),
      tr(false, 0L),
      tr(false, 2L),
      tr(false, 4L),
      tr(false, 6L),
      tr(false, 8L)
    );

    OperatorStats stats = validateSingle(conf, ProjectOperator.class, TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator()), expected, 4095);
    List<UserBitShared.ExpressionSplitInfo> splitInfoList = stats.getProfile(true).getDetails().getSplitInfosList();

    Assert.assertEquals(2, splitInfoList.size());
    Assert.assertFalse(splitInfoList.get(0).getOptimize());
    Assert.assertTrue(splitInfoList.get(1).getOptimize());
  }

}
