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
package com.dremio.exec.planner.sql;

import com.dremio.BaseTestQuery;
import java.io.File;
import java.util.Arrays;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class TestDMLOnMapTables extends BaseTestQuery {

  @Test
  public void testDeleteDMLOnMapTables() throws Exception {
    String tblName = "newMapTable1";
    try {

      String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s as select * from cp.\"parquet/map_data_types/mapcolumn.parquet\"",
              TEMP_SCHEMA_HADOOP, tblName);
      test(ctasQuery);

      String selectQuery =
          String.format(
              "select map_keys(col2) as col2_keys from %s.%s", TEMP_SCHEMA_HADOOP, tblName);
      testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col2_keys")
          .baselineValues(Arrays.asList(new Text("b")))
          .baselineValues(Arrays.asList(new Text("c")))
          .go();

      String deleteQuery =
          String.format("DELETE FROM %s.%s WHERE col2['b'] = 'bb'", TEMP_SCHEMA_HADOOP, tblName);
      test(deleteQuery);

      testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col2_keys")
          .baselineValues(Arrays.asList(new Text("c")))
          .go();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testInsertDMLOnMapTables() throws Exception {
    String tblName = "newMapTable2";
    try {

      String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s as select * from cp.\"parquet/map_data_types/mapcolumn.parquet\" limit 1",
              TEMP_SCHEMA_HADOOP, tblName);
      test(ctasQuery);

      String countQuery =
          String.format("select COUNT(*) as cols from %s.%s", TEMP_SCHEMA_HADOOP, tblName);
      testBuilder()
          .sqlQuery(countQuery)
          .unOrdered()
          .baselineColumns("cols")
          .baselineValues(1L)
          .go();

      String insertQuery =
          String.format(
              "INSERT INTO %s.%s select * from cp.\"parquet/map_data_types/mapcolumn.parquet\" where col2['c'] = 'cc'",
              TEMP_SCHEMA_HADOOP, tblName);
      test(insertQuery);
      testBuilder()
          .sqlQuery(countQuery)
          .unOrdered()
          .baselineColumns("cols")
          .baselineValues(2L)
          .go();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testMergeDMLOnMapTables() throws Exception {
    String tblName = "newMapTable3";
    try {

      String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s as select * from cp.\"parquet/map_data_types/mapcolumn.parquet\" limit 1",
              TEMP_SCHEMA_HADOOP, tblName);
      test(ctasQuery);

      String selectQuery = String.format("select col3 from %s.%s", TEMP_SCHEMA_HADOOP, tblName);
      testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col3")
          .baselineValues("def")
          .go();

      String mergeQuery =
          String.format(
              "merge into %s.%s t using %s.%s s\n"
                  + "on t.col1 = s.col1\n"
                  + "WHEN MATCHED THEN UPDATE SET col3 = 'aaa'\n"
                  + "WHEN NOT MATCHED THEN INSERT *;",
              TEMP_SCHEMA_HADOOP, tblName, TEMP_SCHEMA_HADOOP, tblName);
      test(mergeQuery);

      testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col3")
          .baselineValues("aaa") // value for col3 is updated
          .go();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testUpdateDMLOnMapTables() throws Exception {
    String tblName = "newMapTable4";
    try {

      String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s as select * from cp.\"parquet/map_data_types/map_string_keys.parquet\"",
              TEMP_SCHEMA_HADOOP, tblName);
      test(ctasQuery);

      String selectQuery =
          String.format(
              "select id from %s.%s where sample_map['key1'] = 1", TEMP_SCHEMA_HADOOP, tblName);

      testBuilder().sqlQuery(selectQuery).unOrdered().baselineColumns("id").baselineValues(1L).go();

      String updateQuery =
          String.format(
              "UPDATE %s.%s SET id = 2 WHERE sample_map['key1'] = 1", TEMP_SCHEMA_HADOOP, tblName);
      test(updateQuery);

      testBuilder().sqlQuery(selectQuery).unOrdered().baselineColumns("id").baselineValues(2L).go();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }
}
