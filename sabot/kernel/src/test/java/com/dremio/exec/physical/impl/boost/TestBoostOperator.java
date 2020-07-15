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
package com.dremio.exec.physical.impl.boost;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;

public class TestBoostOperator extends BaseTestQuery {

  @Test
  public void testTwoMultiBatchSplits() throws Exception {
    final String testWorkingPath = TestTools.getWorkingPath();
    final String table = testWorkingPath + "/src/test/resources/iceberg/supplier";
    final String firstSplitPath = table + "/1_0_0.parquet";
    final String secondSplitPath = table + "/1_1_0.parquet";

    try (AutoCloseable c = treatScanAsBoost()) {
      testBuilder()
        .sqlQuery("select * from dfs.\"" + table + "\"") // writes arrow files to /tmp/dfs./{$table}/firstSplitPath, secondSplitPath
        .unOrdered()
        .expectsEmptyResultSet()
        .build()
        .run();
    }

    try {
      testBuilder()
        .sqlQuery("select * from dfs.\"/tmp/dfs." + firstSplitPath + "/S_ACCTBAL/1_0_0.dremarrow1" + "\"")
        .unOrdered()
        .sqlBaselineQuery("select S_ACCTBAL from dfs.\"" + firstSplitPath + "\"")
        .build()
        .run();

      testBuilder()
        .sqlQuery("select * from dfs.\"/tmp/dfs." + firstSplitPath + "/S_SUPPKEY/1_0_0.dremarrow1" + "\"")
        .unOrdered()
        .sqlBaselineQuery("select S_SUPPKEY from dfs.\"" + firstSplitPath + "\"")
        .build()
        .run();

      testBuilder()
        .sqlQuery("select * from dfs.\"/tmp/dfs." + secondSplitPath + "/S_SUPPKEY/1_1_0.dremarrow1" + "\"")
        .unOrdered()
        .sqlBaselineQuery("select S_SUPPKEY from dfs.\"" + secondSplitPath + "\"")
        .build()
        .run();

      testBuilder()
        .sqlQuery("select * from dfs.\"/tmp/dfs." + secondSplitPath + "/S_NATIONKEY/1_1_0.dremarrow1" + "\"")
        .unOrdered()
        .sqlBaselineQuery("select S_NATIONKEY from dfs.\"" + secondSplitPath + "\"")
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(new File("/tmp/dfs."));
    }
  }

}
