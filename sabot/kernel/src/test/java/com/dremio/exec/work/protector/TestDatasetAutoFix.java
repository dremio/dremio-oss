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
package com.dremio.exec.work.protector;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.io.Files;

/**
 * Make sure auto fix works.
 */
public class TestDatasetAutoFix extends BaseTestQuery {

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
    setSessionOption(ExecConstants.ENABLE_REATTEMPTS, "true");
  }

  @Test
  public void addNewColumnOnSelectStarMultiLevel() throws Exception {
    setupAndRun(
        (String tableName, File f1, File folder, TestBuilder testBuilder) -> {
          testNoResult("create view dfs_test.view2_%s as select * from dfs_test.view_%s", tableName, tableName);

          // add a second file.
          File f2 = new File(folder, "file2.json");
          Files.write("{a:2, b:3, c:4}", f2, StandardCharsets.UTF_8);
          testNoResult("alter pds dfs_test.%s refresh metadata force update", tableName);

          testBuilder().sqlQuery("select * from dfs_test.view2_%s order by a", tableName)
            .ordered()
            .baselineColumns("a", "b", "c")
            .baselineValues(1L, 2L, null)
            .baselineValues(2L, 3L, 4L)
            .go();

        });
  }

  @Test
  public void addNewColumnOnSelectStar() throws Exception {
    setupAndRun(
        (String tableName, File f1, File folder, TestBuilder testBuilder) -> {
          // add a second file.
          File f2 = new File(folder, "file2.json");
          Files.write("{a:2, b:3, c:4}", f2, StandardCharsets.UTF_8);
          testNoResult("alter pds dfs_test.%s refresh metadata force update", tableName);

          testBuilder.baselineColumns("a", "b", "c")
            .baselineValues(1L, 2L, null)
            .baselineValues(2L, 3L, 4L)
            .go();

        });
  }

  @Test
  public void reorderFieldsSameTypeNoForget() throws Exception {
    setupAndRun(
        (String tableName, File f1, File folder, TestBuilder testBuilder) -> {
          // replace file
          f1.delete();
          File f2 = new File(folder, "file2.json");
          Files.write("{b:2, a:3}", f2, StandardCharsets.UTF_8);

          testBuilder.baselineColumns("a", "b")
            .baselineValues(3L, 2L)
            .go();

        });
  }

  @Test
  public void reorderFieldsSameTypeWithForget() throws Exception {
    setupAndRun(
        (String tableName, File f1, File folder, TestBuilder testBuilder) -> {
          // replace file
          f1.delete();
          File f2 = new File(folder, "file2.json");
          Files.write("{b:2, a:3}", f2, StandardCharsets.UTF_8);

          // refresh the metadata
          testNoResult("alter pds dfs_test.%s forget metadata", tableName);

          testBuilder.baselineColumns("b", "a")
            .baselineValues(2L, 3L)
            .go();

        });
  }

  @Test
  public void failedUpdate() throws Exception {
    setupAndRun(
        (String tableName, File f1, File folder, TestBuilder testBuilder) -> {
          testNoResult("create view dfs_test.view2_%s as select a,b from dfs_test.view_%s", tableName, tableName);

          f1.delete();
          testNoResult("alter pds dfs_test.%s forget metadata", tableName);

          // add a new file with different columns
          File f2 = new File(folder, "file2.json");
          Files.write("{b:3, c:4}", f2, StandardCharsets.UTF_8);

          assertThatThrownBy(() -> testNoResult("select * from dfs_test.view2_%s order by a", tableName))
            .hasMessageContaining(
              String.format("Error while expanding view dfs_test.view2_%s. Column 'a' not found in any table. Verify the view’s SQL definition.", tableName));
        });
  }

  @Test
  public void changeFieldType() throws Exception {
    setupAndRun(
        (String tableName, File f1, File folder, TestBuilder testBuilder) -> {
          f1.delete();
          File f2 = new File(folder, "file2.json");
          Files.write("{a:1, b:\"hello\"}", f2, StandardCharsets.UTF_8);

          // refresh the metadata
          testNoResult("alter pds dfs_test.%s forget metadata", tableName);

          // make sure that that we automatically learn schema, update vds, show correct result.
          testBuilder.baselineColumns("a", "b")
            .baselineValues(1L, "hello")
            .go();
        });
  }

  /**
   * Create a new table based on the test method name with a single record of two columns, a.b and both of integer type.
   * @param op The operation that should run within this setup.
   * @throws Exception
   */
  private void setupAndRun(InbetweenOp op) throws Exception {
    final String testName = TEST_NAME.getMethodName();
    // write a table.
    File f = new File(getDfsTestTmpSchemaLocation());

    // create a directory
    File folder = new File(f, testName);
    folder.mkdirs();

    // write a file to it.
    File f1 = new File(folder, "file1.json");
    Files.write("{a:1, b:2}", f1, StandardCharsets.UTF_8);

    // query to autopromote the folder.
    testNoResult("select * from dfs_test.%s", testName);

    // create a view on autopromoted table.
    testNoResult("create view dfs_test.view_%s as select * from dfs_test.%s", testName, testName);

    op.doit(testName, f1, folder, testBuilder().sqlQuery("select * from dfs_test.%s order by a", testName).ordered());

  }

  private interface InbetweenOp {
    public void doit(String tableName, File f1, File folder, TestBuilder testBuilder) throws Exception;
  }

}
