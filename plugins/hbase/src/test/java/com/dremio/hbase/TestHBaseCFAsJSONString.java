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
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.client.DremioClient;
import com.dremio.sabot.rpc.user.QueryDataBatch;

public class TestHBaseCFAsJSONString extends BaseHBaseTest {

  private static DremioClient parent_client;

  @BeforeClass
  public static void openMyClient() throws Exception {
    parent_client = client;
    client = new DremioClient(config, clusterCoordinator);
    client.setSupportComplexTypes(false);
    client.connect();
  }

  @AfterClass
  public static void closeMyClient() throws IOException {
    if (client != null) {
      client.close();
    }
    client = parent_client;
  }

  @Test
  public void testColumnFamiliesAsJSONString() throws Exception {
    test("ALTER SESSION SET \"exec.errors.verbose\" = true");
    setColumnWidths(new int[] {112, 12});
    List<QueryDataBatch> resultList = runHBaseSQLlWithResults("SELECT f, f2 FROM hbase.\"[TABLE_NAME]\" tableName LIMIT 1");
    printResult(resultList);
  }

}
