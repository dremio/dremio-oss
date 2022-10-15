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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestDMLOnMapTables extends BaseTestQuery {

  @Test
  public void testDMLFailureOnMapTables() throws Exception {
    String tblName = "newMapTable";
    try {

      String ctasQuery = String.format(
        "CREATE TABLE %s.%s as select * from cp.\"parquet/map_data_types/map_string_keys.parquet\"", TEMP_SCHEMA_HADOOP, tblName);
      test(ctasQuery);

      String insertQuery = String.format("INSERT INTO %s.%s select * from cp.\"parquet/map_data_types/map_string_keys.parquet\"", TEMP_SCHEMA_HADOOP, tblName);
      assertThatThrownBy(() ->
        test(insertQuery))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("DML operations on tables with MAP columns is not yet supported.");

      String deleteQuery = String.format("DELETE FROM %s.%s WHERE col1['key1'] = 1", TEMP_SCHEMA_HADOOP, tblName);
      assertThatThrownBy(() ->
        test(deleteQuery))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("DML operations on tables with MAP columns is not yet supported.");

      String updateQuery = String.format("UPDATE %s.%s SET col1 = 2 WHERE col1['key1'] = 1", TEMP_SCHEMA_HADOOP, tblName);
      assertThatThrownBy(() ->
        test(updateQuery))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("DML operations on tables with MAP columns is not yet supported.");

      String mergeQuery = String.format("merge into %s.%s t using %s.%s s\n" +
        "on t.retailer_product_id = s.retailer_product_id\n" +
        "WHEN MATCHED THEN UPDATE SET activity_type = 'NEVER!'\n" +
        "WHEN NOT MATCHED THEN INSERT *;", TEMP_SCHEMA_HADOOP, tblName, TEMP_SCHEMA_HADOOP, tblName);
      assertThatThrownBy(() ->
        test(mergeQuery))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("DML operations on tables with MAP columns is not yet supported.");

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }
}
