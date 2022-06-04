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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.exec.work.foreman.AttemptManager;

// Inject a failure during commit, and verify that it bails out (no timeout/hang).
public class TestCommitFailure extends BaseTestQuery {

  @Test
  public void commit() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      final String tableName = "commit_failure";

      final String controls = Controls.newBuilder()
        .addException(AttemptManager.class, "commit-failure", UnsupportedOperationException.class)
        .build();

      try (AutoCloseable c = enableIcebergTables()) {
        try {
          final String testWorkingPath = TestTools.getWorkingPath();
          final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
          final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (o_orderdate) " +
              " AS SELECT * from dfs.\"" + parquetFiles + "\" limit 2",
            testSchema, tableName);

          ControlsInjectionUtil.setControls(client, controls);
          assertThatThrownBy(() -> test(ctasQuery))
            .hasMessageContaining("commit-failure");

        } finally {
          FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
        }
      }
    }
  }
}
