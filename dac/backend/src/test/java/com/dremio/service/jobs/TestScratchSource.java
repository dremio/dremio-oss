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
package com.dremio.service.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.util.Objects;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.job.proto.QueryType;
import com.dremio.test.TemporarySystemProperties;
import com.dremio.test.UserExceptionAssert;

/**
 * Test scratch source.
 */
public class TestScratchSource extends BaseTestServer {

  @Rule
  public final TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  @Test
  public void testCTASAndDropTable() throws Exception {
    // Disabling auto promotion should still work with $scratch
    properties.set("dremio.datasets.auto_promote", "false");
    // Create a table
    final SqlQuery ctas = getQueryFromSQL("CREATE TABLE \"$scratch\".\"ctas\" AS select * from (VALUES (1))");
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    final FileSystemPlugin<?> plugin = getCurrentDremioDaemon()
        .getBindingProvider()
        .lookup(CatalogService.class)
        .getSource("$scratch");

    // Make sure the table data files exist
    final File ctasTableDir = new File(plugin.getConfig().getPath().toString(), "ctas");
    assertTrue(ctasTableDir.exists());
    assertTrue(ctasTableDir.list().length >= 1);

    // Now drop the table
    SqlQuery dropTable = getQueryFromSQL("DROP TABLE \"$scratch\".\"ctas\""); // throws if not auto-promoted
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(dropTable)
        .setQueryType(QueryType.ACCELERATOR_DROP)
        .build()
    );

    // Make sure the table data directory is deleted
    assertFalse(ctasTableDir.exists());
  }

  @Test
  @Ignore("DX-34314")
  public void testIcebergCTAS() throws Exception {
    assumeFalse(isMultinode()); // when multinode, scratch plugin uses file://
    final String ctasIceberg = "CREATE TABLE \"$scratch\".\"ctasonpdfs\" as select * from (VALUES (1))";
    runWithIcebergEnabled(ctasIceberg);

    final FileSystemPlugin<?> plugin = getCurrentDremioDaemon()
      .getBindingProvider()
      .lookup(CatalogService.class)
      .getSource("$scratch");

    // Make sure the table data files exist
    final File ctasTableDir = new File(plugin.getConfig().getPath().toString(), "ctasonpdfs");
    assertTrue(ctasTableDir.exists());
    assertTrue(Objects.requireNonNull(ctasTableDir.list()).length >= 1);

    // not an iceberg table
    assertEquals(0, Objects.requireNonNull(ctasTableDir.list((dir, name) -> name.equals("metadata"))).length);

    // drop the table
    SqlQuery dropTable = getQueryFromSQL("DROP TABLE \"$scratch\".\"ctasonpdfs\"");
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(dropTable)
        .setQueryType(QueryType.ACCELERATOR_DROP)
        .build()
    );
  }

  @Test
  @Ignore("DX-34314")
  public void testIcebergCreate() {
    assumeFalse(isMultinode()); // when multinode, scratch plugin uses file://
    final String createIceberg = "CREATE TABLE \"$scratch\".\"createonpdfs\"(id int, code decimal(18, 3))";
    UserExceptionAssert.assertThatThrownBy(() -> runWithIcebergEnabled(createIceberg))
      .hasMessageContaining("Source [$scratch] does not support CREATE TABLE");
  }

  private void runWithIcebergEnabled(String createIceberg) {
    try {
      runQuery("alter system set \"dremio.iceberg.enabled\" = true");
      runQuery(createIceberg);
    } finally {
      runQuery("alter system set \"dremio.iceberg.enabled\" = false");
    }
  }

  private void runQuery(String query) {
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL(query))
        .setQueryType(QueryType.UI_RUN)
        .build()
    );
  }
}
