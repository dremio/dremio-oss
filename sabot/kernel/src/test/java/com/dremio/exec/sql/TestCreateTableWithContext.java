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
package com.dremio.exec.sql;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;

public class TestCreateTableWithContext extends PlanTestBase {
  @Before
  public void setUp() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "true");
    setSystemOption(ExecConstants.CTAS_CAN_USE_ICEBERG, "true");
  }

  @After
  public void cleanUp() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG,
      ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.CTAS_CAN_USE_ICEBERG,
      ExecConstants.CTAS_CAN_USE_ICEBERG.getDefault().getBoolVal().toString());
  }

  @Test
  public void createEmptyTableWithContext() throws Exception {
    final String useSchemaQuery = "USE  " + TEMP_SCHEMA;
    test(useSchemaQuery);

    final String newTblName = "create_test_with_context_tbl";
    try {
      final String createTableQuery = String.format("CREATE TABLE %s (col1 int)  ", newTblName);
      test(createTableQuery);
      final String selectFromCreatedTable = String.format("select count(*) as cnt from %s", newTblName);
      testBuilder()
        .sqlQuery(selectFromCreatedTable)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }
}
