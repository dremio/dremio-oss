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
package com.dremio.sabot;

import java.util.Collections;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.junit.Test;

import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.physical.config.Limit;
import com.dremio.exec.physical.config.Project;
import com.dremio.sabot.op.limit.LimitOperator;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.sort.external.ExternalSortOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;

public class SimpleSingleInputTests extends BaseTestOperator {

  static final TpchTable TABLE = TpchTable.CUSTOMER;
  static final int TARGET_BATCH_SIZE = 4095;
  static final double SCALE = 0.1;

  @Test
  public void project() throws Exception {
    Project conf = new Project(Collections.singletonList(straightName("rownumber")), null);
    basicTests(conf, ProjectOperator.class, TABLE, SCALE, null, TARGET_BATCH_SIZE);
  }

  @Test
  public void sort() throws Exception {
    ExternalSort sort = new ExternalSort(null, Collections.singletonList(ordering("rownumber", Direction.ASCENDING, NullDirection.FIRST)), false);
    basicTests(sort, ExternalSortOperator.class, TABLE, SCALE, null, TARGET_BATCH_SIZE);
  }

  @Test
  public void limit() throws Exception {
    Limit limit = new Limit(null, 0, 100);
    basicTests(limit, LimitOperator.class, TABLE, SCALE, 100L, TARGET_BATCH_SIZE);
  }


}
