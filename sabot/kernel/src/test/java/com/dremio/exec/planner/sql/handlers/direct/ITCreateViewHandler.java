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
package com.dremio.exec.planner.sql.handlers.direct;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.BaseTestQuery;
import com.dremio.config.DremioConfig;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.test.TemporarySystemProperties;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class ITCreateViewHandler extends BaseTestQuery {

  private static final String VDS_NAME = "dfs_test.test_vds";
  private static IcebergTestTables.Table table;

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @BeforeClass
  public static void beforeClass() {
    table = IcebergTestTables.NATION.get();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    table.close();
  }

  @Before
  public void beforeTest() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @After
  public void afterTest() throws Exception {
    runSQL(String.format("DROP VDS IF EXISTS %s", VDS_NAME));
  }

  @Test
  public void testCreateViewForSimpleQuery() throws Exception {
    String sql = String.format("CREATE VDS %s AS SELECT * FROM %s", VDS_NAME, table.getTableName());
    List<QueryDataBatch> results = testSqlWithResults(sql);
    String resultString = getResultString(results, "|");
    assertThat(resultString)
        .isEqualTo("ok|summary\ntrue|View 'dfs_test.test_vds' created successfully\n");
  }
}
