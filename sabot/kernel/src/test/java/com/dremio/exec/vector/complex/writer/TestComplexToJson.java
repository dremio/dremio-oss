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
package com.dremio.exec.vector.complex.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.UserBitShared.RecordBatchDef;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.sabot.rpc.user.QueryDataBatch;

public class TestComplexToJson extends BaseTestQuery {

  @Test
  public void test() throws Exception {
    DremioClient parent_client = client;

    List<QueryDataBatch> results;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());

    setup();
    results = testSqlWithResults("select * from dfs_root.`[WORKING_PATH]/src/test/resources/store/text/data/regions.csv`");
    checkResult(loader, results);

    setup();
    results = testSqlWithResults("select * from dfs_root.`[WORKING_PATH]/src/test/resources/store/text/data/regions.csv`");
    checkResult(loader, results);

    client = parent_client;
  }

  @Test
  public void test1() throws Exception {
    DremioClient parent_client = client;

    List<QueryDataBatch> results;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());

    setup();
    results = testSqlWithResults("select * from dfs_root.`[WORKING_PATH]/src/test/resources/store/text/data/jsonwriterbug.json`");
    checkResult(loader, results);

    setup();
    results = testSqlWithResults("select * from dfs_root.`[WORKING_PATH]/src/test/resources/store/text/data/jsonwriterbug.json`");
    checkResult(loader, results);

    client = parent_client;
  }

  private void checkResult(RecordBatchLoader loader, List<QueryDataBatch> results) {
    loader.load(results.get(0).getHeader().getDef(), results.get(0).getData());
    RecordBatchDef def = results.get(0).getHeader().getDef();
    // the entire row is returned as a single column
    assertEquals(1, def.getFieldCount());
    assertTrue(def.getField(0).getMajorType().getMode() == DataMode.OPTIONAL);
    loader.clear();
    for(QueryDataBatch result : results) {
      result.release();
    }
    client.close();
  }

  private void setup() throws Exception {
    client = new DremioClient(config, clusterCoordinator);
    client.setSupportComplexTypes(false);
    client.connect();
  }
}
