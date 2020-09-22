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
package com.dremio.exec.store.iceberg;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;

public class TestPartitionMatch extends BaseTestQuery {

  @Test
  public void datePartition() throws Exception {
    final String tableName = "orders_with_date_partition";

    try (AutoCloseable c = enableIcebergTables()) {
      try {
        final String testWorkingPath = TestTools.getWorkingPath();
        final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
        final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (o_orderdate) " +
          " AS SELECT * from dfs.\"" + parquetFiles + "\" order by o_orderdate limit 20",
          TEMP_SCHEMA, tableName);

        test(ctasQuery);

        testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s where o_orderdate='1992-02-21'", TEMP_SCHEMA, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .build()
          .run();
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void decimalPartition() throws Exception {
    final String tableName = "match_decimal_partition";

    try (AutoCloseable c = enableIcebergTables()) {
      try {
        final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (DEC9) " +
            " AS SELECT cast(DEC9 as decimal(30, 10)) DEC9 from cp.\"input_simple_decimal.json\"",
          TEMP_SCHEMA, tableName);

        test(ctasQuery);

        testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s where DEC9=-123.1234000000", TEMP_SCHEMA, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .build()
          .run();
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }
}
