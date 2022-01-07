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
package com.dremio.exec.store.json;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.proto.UserBitShared;

public class TestJsonFiltersNoMixedTypes extends PlanTestBase {

  @Test
  public void testEqFilterSelectSingleColumn() throws Exception {
    Path jsonDir = copyFiles("varchar_and_double");
    String query = "SELECT heading1 from dfs.\"" + jsonDir + "\" where heading1 = 'red'";
    triggerSchemaLearning(jsonDir);
    verifyRecords(query, "heading1", "red");
  }

  @Test
  public void testEqFilterEmptyResults() throws Exception {
    Path jsonDir = copyFiles("varchar_and_double");
    String query = "SELECT * from dfs.\"" + jsonDir + "\" where heading1 = 'yellow'";
    triggerSchemaLearning(jsonDir);
    verifyEmptyResult(query);
  }

  @Test
  public void testLtFilterSelectSingleColumn() throws Exception {
    Path jsonDir = copyFiles("varchar_and_double");
    String query = "SELECT heading1 from dfs.\"" + jsonDir + "\" where heading1 < 'red'";
    triggerSchemaLearning(jsonDir);
    verifyRecords(query, "heading1", "12.3", "12.4", "blue");
  }

  @Test
  public void testLtFilterEmptyResults() throws Exception {
    Path jsonDir = copyFiles("varchar_and_double");
    String query = "SELECT * from dfs.\"" + jsonDir + "\" where heading1 < '1'";
    triggerSchemaLearning(jsonDir);
    verifyEmptyResult(query);
  }

  @Test
  public void testLteFilterSelectSingleColumn() throws Exception {
    Path jsonDir = copyFiles("varchar_and_double");
    String query = "SELECT heading1 from dfs.\"" + jsonDir + "\" where heading1 <= 'red'";
    triggerSchemaLearning(jsonDir);
    verifyRecords(query, "heading1", "12.3", "12.4", "blue", "red");
  }

  @Test
  public void testLteFilterEmptyResults() throws Exception {
    Path jsonDir = copyFiles("varchar_and_double");
    String query = "SELECT * from dfs.\"" + jsonDir + "\" where heading1 <= '1'";
    triggerSchemaLearning(jsonDir);
    verifyEmptyResult(query);
  }

  @Test
  public void testGtFilterSelectSingleColumn() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT heading1 from dfs.\"" + jsonDir + "\" where heading1 > 12";
    triggerSchemaLearning(jsonDir);
    verifyRecords(query, "heading1", 12.3, 12.4, 13.0);
  }

  @Test
  public void testGtFilterEmptyResults() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT * from dfs.\"" + jsonDir + "\" where heading1 > 15";
    triggerSchemaLearning(jsonDir);
    verifyEmptyResult(query);
  }

  @Test
  public void testGteFilterSelectSingleColumn() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT heading1 from dfs.\"" + jsonDir + "\" where heading1 >= 12";
    triggerSchemaLearning(jsonDir);
    verifyRecords(query, "heading1", 12.3, 12.4, 12.0, 13.0);
  }

  @Test
  public void testGteFilterEmptyResults() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT * from dfs.\"" + jsonDir + "\" where heading1 >= 15";
    triggerSchemaLearning(jsonDir);
    verifyEmptyResult(query);
  }

  @Test
  public void testBetweenFilterSelectSingleColumn() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT heading1 from dfs.\"" + jsonDir + "\" where heading1 between 12.0 and 13.0";
    triggerSchemaLearning(jsonDir);
    verifyRecords(query, "heading1", 12.3, 12.4, 12.0, 13.0);
  }

  @Test
  public void testBetweenFilterEmptyResults() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT * from dfs.\"" + jsonDir + "\" where heading1 between 15.0 and 16.0";
    triggerSchemaLearning(jsonDir);
    verifyEmptyResult(query);
  }

  @Test
  public void testInFilterSelectSingleColumn() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT heading1 from dfs.\"" + jsonDir + "\" where heading1 in (12.0,null)";
    triggerSchemaLearning(jsonDir);
    verifyRecords(query, "heading1", 12.0);
  }

  @Test
  public void testInFilterEmptyResults() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    String query = "SELECT * from dfs.\"" + jsonDir + "\" where heading1 in (15.0,null)";
    triggerSchemaLearning(jsonDir);
    verifyEmptyResult(query);
  }

  void verifyRecords(String query, String baselineCol, Object... values) throws Exception {
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns(baselineCol);
    for (Object value : values) {
      testBuilder.baselineValues(value);
    }
    testBuilder.go();
  }

  void verifyEmptyResult(String query) throws Exception {
    testBuilder()
      .sqlQuery(query)
      .expectsEmptyResultSet()
      .go();
  }

  private Path copyFiles(String dirName) {
    Path jsonDir = createTempDirWithName(dirName).toPath();
    writeDir(Paths.get("json/schema_changes/no_mixed_types/simple/"), jsonDir, dirName);
    return jsonDir;
  }

  private void triggerSchemaLearning(Path jsonDir) {
    String query = String.format("SELECT * FROM dfs.\"%s\"", jsonDir);
    try {
      testRunAndReturn(UserBitShared.QueryType.SQL, query);
      fail("Expected UserRemoteException");
    } catch (Exception e) {
      assertThat(e.getCause(), instanceOf(UserRemoteException.class));
      assertThat(e.getCause().getMessage(), containsString("New schema found"));
    }
  }
}
