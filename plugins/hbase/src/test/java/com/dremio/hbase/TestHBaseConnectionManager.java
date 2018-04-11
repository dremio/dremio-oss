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

import org.junit.Test;

public class TestHBaseConnectionManager extends BaseHBaseTest {

  @Test
  public void testHBaseConnectionManager() throws Exception {
    setColumnWidth(8);
    runHBaseSQLVerifyCount("SELECT\n"
        + "row_key\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 8);

    /*
     * Simulate HBase connection close and ensure that the connection
     * will be reestablished automatically.
     *
     * DX-9766: This is actually invalid because it puts the connection in a "closing" state which is then shared when other connections are opened.
     */
    // storagePlugin.getStoragePlugin().closeCurrentConnection();

    runHBaseSQLVerifyCount("SELECT\n"
        + "row_key\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 8);

    /*
     * Simulate HBase cluster restart and ensure that running query against
     * HBase does not require Dremio cluster restart.
     */
    HBaseTestsSuite.getHBaseTestingUtility().shutdownMiniHBaseCluster();
    HBaseTestsSuite.getHBaseTestingUtility().restartHBaseCluster(1);
    runHBaseSQLVerifyCount("SELECT\n"
        + "row_key\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 8);

  }

}
