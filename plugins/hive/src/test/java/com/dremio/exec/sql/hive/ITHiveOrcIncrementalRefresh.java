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
package com.dremio.exec.sql.hive;

import static com.dremio.exec.sql.hive.ITHiveRefreshDatasetMetadataRefresh.EXPLAIN_PLAN;
import static com.dremio.exec.sql.hive.ITHiveRefreshDatasetMetadataRefresh.verifyIcebergExecution;

import java.util.Arrays;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Test;

/**
 * Test Incremental metadata refresh flow ORC input format
 */
public class ITHiveOrcIncrementalRefresh extends ITHiveRefreshDatasetIncrementalMetadataRefresh {
  @Override
  public String getFileFormat() {
    return "ORC";
  }

  @Test
  public void testIncrementalRefreshSchemaEvolution() throws Exception {
    final String tableName = "incrrefresh_v2_test_schema_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      dataGenerator.executeDDL("INSERT INTO " + tableName + " VALUES(1, 'a')");
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      dataGenerator.executeDDL("ALTER TABLE " + tableName + " CHANGE col1 col3 DOUBLE");
      dataGenerator.executeDDL("INSERT INTO " + tableName + " VALUES(2.0, 'b')");
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //col2 renamed to col3 and schema updated from int -> double and column id also changed
      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col2", new Types.StringType()),
        Types.NestedField.optional(2, "col3", new Types.DoubleType())));

      verifyMetadata(expectedSchema, 2);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("col2", "col3")
        .baselineValues("a", 1.0)
        .baselineValues("b", 2.0)
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }
}
