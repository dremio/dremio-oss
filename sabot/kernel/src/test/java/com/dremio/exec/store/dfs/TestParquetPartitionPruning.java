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
package com.dremio.exec.store.dfs;

import org.apache.arrow.vector.util.DateUtility;
import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestParquetPartitionPruning extends BaseTestQuery {

  @Test
  public void testPartitionPruningOnTimestamp() throws Exception {
    String sql = "select ts from cp.`parquet/singlets.parquet` where ts = TIMESTAMP '2008-10-05 05:13:14.000'";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("ts")
      .baselineValues(DateUtility.formatTimeStampMilli.parseLocalDateTime("2008-10-05 05:13:14.000"))
      .go();
  }
}
