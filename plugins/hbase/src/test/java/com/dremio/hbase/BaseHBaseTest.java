/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.store.hbase.HBaseConf;
import com.dremio.exec.store.hbase.HBaseStoragePlugin;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@RunWith(GuavaPatcherRunner.class)
public class BaseHBaseTest extends BaseTestQuery {

 /* static {
    GuavaPatcher.patch();
  }*/

  private static final String HBASE_STORAGE_PLUGIN_NAME = "hbase";

  protected static HBaseConf storagePluginConfig;
  protected static HBaseStoragePlugin storagePlugin;

  @BeforeClass
  public static void setupHBaseTestCluster() throws Exception {
    /*
     * Change the following to HBaseTestsSuite.configure(false, true)
     * if you want to test against an externally running HBase cluster.
     */
    HBaseTestsSuite.configure(true /*manageHBaseCluster*/, true /*createTables*/);
    HBaseTestsSuite.initCluster();

    final CatalogServiceImpl catalog = (CatalogServiceImpl) getSabotContext().getCatalogService();
    HBaseConf conf = new HBaseConf();
    conf.port = HBaseTestsSuite.getZookeeperPort();
    conf.zkQuorum = "localhost";
    storagePluginConfig = conf;
    SourceConfig sc = new SourceConfig();
    sc.setConfig(conf.toBytesString());
    sc.setType(conf.getType());
    sc.setName(HBASE_STORAGE_PLUGIN_NAME);
    catalog.getSystemUserCatalog().createSource(sc);
    storagePlugin = catalog.getSource(HBASE_STORAGE_PLUGIN_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    HBaseTestsSuite.tearDownCluster();
  }

  protected String getPlanText(String planFile, String tableName) throws IOException {
    return Files.toString(FileUtils.getResourceAsFile(planFile), Charsets.UTF_8)
        .replaceFirst("\"hbase\\.zookeeper\\.property\\.clientPort\".*:.*\\d+", "\"hbase.zookeeper.property.clientPort\" : " + HBaseTestsSuite.getZookeeperPort())
        .replace("[TABLE_NAME]", tableName);
  }

  protected void runHBasePhysicalVerifyCount(String planFile, String tableName, int expectedRowCount) throws Exception{
    String physicalPlan = getPlanText(planFile, tableName);
    List<QueryDataBatch> results = testPhysicalWithResults(physicalPlan);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  protected List<QueryDataBatch> runHBaseSQLlWithResults(String sql) throws Exception {
    sql = canonizeHBaseSQL(sql);
    System.out.println("Running query:\n" + sql);
    return testSqlWithResults(sql);
  }

  protected void runHBaseSQLVerifyCount(String sql, int expectedRowCount) throws Exception{
    List<QueryDataBatch> results = runHBaseSQLlWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  private void printResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount) throws SchemaChangeException {
    int rowCount = printResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }

  protected String canonizeHBaseSQL(String sql) {
    return sql.replace("[TABLE_NAME]", HBaseTestsSuite.TEST_TABLE_1.getNameAsString());
  }

}
