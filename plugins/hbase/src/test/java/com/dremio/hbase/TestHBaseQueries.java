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
package com.dremio.hbase;

import java.util.Arrays;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

public class TestHBaseQueries extends BaseHBaseTest {

  @Test
  public void testWithEmptyFirstAndLastRegion() throws Exception {
    HBaseAdmin admin = (HBaseAdmin) HBaseTestsSuite.getAdmin();
    TableName tableName = TableName.valueOf("dremio_ut_empty_regions");

    try (Table table = HBaseTestsSuite.getConnection().getTable(tableName);) {
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor("f"));
      admin.createTable(desc, Arrays.copyOfRange(TestTableGenerator.SPLIT_KEYS, 0, 2));

      Put p = new Put("b".getBytes());
      p.addColumn("f".getBytes(), "c".getBytes(), "1".getBytes());
      table.put(p);

      setColumnWidths(new int[] {8, 15});
      runHBaseSQLVerifyCount("SELECT *\n"
          + "FROM\n"
          + "  hbase.\"" + tableName + "\" tableName\n"
          , 1);
    } finally {
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (Exception e) { } // ignore
    }

  }

  @Test
  public void testWithEmptyTable() throws Exception {
    Admin admin = HBaseTestsSuite.getAdmin();
    TableName tableName = TableName.valueOf("dremio_ut_empty_table");

    try (Table table = HBaseTestsSuite.getConnection().getTable(tableName);) {
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor("f"));
      admin.createTable(desc, Arrays.copyOfRange(TestTableGenerator.SPLIT_KEYS, 0, 2));

      setColumnWidths(new int[] {8, 15});
      runHBaseSQLVerifyCount("SELECT row_key, count(*)\n"
          + "FROM\n"
          + "  hbase.\"" + tableName + "\" tableName GROUP BY row_key\n"
          , 0);
    } finally {
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (Exception e) { } // ignore
    }
  }

  @Test
  public void testCountStar() throws Exception {
    testBuilder()
        .sqlQuery("SELECT count(*) as cnt FROM hbase." + HBaseTestsSuite.TEST_TABLE_1.getNameAsString())
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(8L)
        .go();
  }

  // DX-10110: issues when "use hbase" used multiple times
  @Test
  public void testMultiUseHbase() throws Exception {
    test("use hbase");
    test("use hbase");
    testBuilder()
      .sqlQuery("SELECT count(*) as cnt FROM " + HBaseTestsSuite.TEST_TABLE_1.getNameAsString())
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(8L)
      .go();
  }

  @Test
  public void queryNonDefaultNamespace() throws Exception {
    testBuilder()
        .sqlQuery("SELECT count(*) as cnt FROM hbase." +
            HBaseTestsSuite.TEST_TABLE_NON_DEFAULT_NAMESPACE.getNamespaceAsString() + "." +
            HBaseTestsSuite.TEST_TABLE_NON_DEFAULT_NAMESPACE.getQualifierAsString())
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(8L)
        .go();
  }

  @Test
  public void queryNonDefaultNamespace2() throws Exception {
    runHBaseSQLVerifyCount("SELECT * FROM hbase." +
            HBaseTestsSuite.TEST_TABLE_NON_DEFAULT_NAMESPACE.getNamespaceAsString() + "." +
            HBaseTestsSuite.TEST_TABLE_NON_DEFAULT_NAMESPACE.getQualifierAsString(),
        8);
  }
}
