/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.junit.Test;

import com.dremio.exec.physical.config.Project;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.project.ProjectOperator;
import com.google.common.collect.ImmutableList;

import io.airlift.tpch.GenerationDefinition.TpchTable;

public class TestProject extends BaseTestOperator {

  @Test
  public void basicProjectionLifecycle() throws Exception {
    Project conf = new Project(Collections.singletonList(n("c_custkey")), null);
    assertSingleInput(conf, ProjectOperator.class, TpchTable.CUSTOMER, 0.1, null, 4095);
  }

  @Test
  public void doubleColumnProjection() throws Exception {
    Project conf = new Project(ImmutableList.of(n("c_custkey"), n("c_custkey")), null);
    assertSingleInput(conf, ProjectOperator.class, TpchTable.CUSTOMER, 0.1, null, 4095);
  }

  @Test
  public void projectRegions() throws Exception {
    Project conf = new Project(Arrays.asList(n("r_regionkey"), n("r_name"), n("r_comment")), null);
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
}
