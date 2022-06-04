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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.rpc.user.QueryDataBatch;

public class TestSimpleJsonSchemaUpPromotionAndTypeCoercion extends PlanTestBase {

  @Test
  public void testUpPromotionAndTypeCoercionFromDoubleToVarchar() throws Exception {
    Path jsonDir = copyFiles("varchar_and_double");
    // Run a query triggering a schema change
    triggerSchemaLearning(jsonDir);
    // Schema should have changed to (CHARACTER VARYING,CHARACTER VARYING) now, irrespective of which file was picked first
    assertThat(runDescribeQuery(jsonDir)).contains("heading1|CHARACTER VARYING").contains("heading2|CHARACTER VARYING");
    // Run a query touching all the files and ensure that it returns the correct records
    verifyRecords(jsonDir, "heading1", "red", "12.3", "blue", "12.4");
    verifyRecords(jsonDir, "heading2", "red", "12.3", "blue", "12.4");
    verifyCountStar(jsonDir, 4);
  }

  @Test
  public void testUpPromotionAndTypeCoercionFromBigIntToVarchar() throws Exception {
    Path jsonDir = copyFiles("varchar_and_bigint");
    // Run a query triggering a schema change
    triggerSchemaLearning(jsonDir);
    // Schema should have changed to (CHARACTER VARYING,CHARACTER VARYING) now, irrespective of which file was picked first
    assertThat(runDescribeQuery(jsonDir)).contains("heading1|CHARACTER VARYING").contains("heading2|CHARACTER VARYING");
    // Run a query touching all the files and ensure that it returns the correct records
    verifyRecords(jsonDir, "heading1", "red", "12", "blue", "13");
    verifyRecords(jsonDir, "heading2", "red", "12", "blue", "13");
    verifyCountStar(jsonDir, 4);
  }

  @Test
  public void testUpPromotionAndTypeCoercionFromBigIntToDouble() throws Exception {
    Path jsonDir = copyFiles("double_and_bigint");
    // Run a query triggering a schema change
    triggerSchemaLearning(jsonDir);
    // Schema should have changed to (DOUBLE,DOUBLE) now, irrespective of which file was picked first
    assertThat(runDescribeQuery(jsonDir)).contains("heading1|DOUBLE").contains("heading2|DOUBLE");
    // Run a query touching all the files and ensure that it returns the correct records
    verifyRecords(jsonDir, "heading1", 12.3, 12.0, 12.4, 13.0);
    verifyRecords(jsonDir, "heading2", 12.3, 12.0, 12.4, 13.0);
    verifyCountStar(jsonDir, 4);
  }

  @Test
  public void testUpPromotionAndTypeCoercionInUnions() throws Exception {
    Path jsonDir = copyFiles("mixed_file");
    // Run a query triggering a schema change
    triggerSchemaLearning(jsonDir);
    // Schema should have changed to (CHARACTER VARYING,CHARACTER VARYING) now, irrespective of which file was picked first
    assertThat(runDescribeQuery(jsonDir)).contains("heading1|CHARACTER VARYING").contains("heading2|CHARACTER VARYING");
    // Run a query touching all the files and ensure that it returns the correct records
    verifyRecords(jsonDir, "heading1", "12", "red", "12.3", "12", "12", "12.3");
    verifyRecords(jsonDir, "heading2", "12.3", "12", "12.3", "12.3", "red", "12.3");
    verifyCountStar(jsonDir, 6);
  }

  @Test
  public void testUpPromotionAndTypeCoercionInLargeUnions() throws Exception {
    Path jsonDir = copyFiles("large_mixed_file");
    // Run a query touching all the files and ensure that it returns the correct records
    String query = String.format("SELECT * FROM dfs.\"%s\" where heading1 != 'hello'", jsonDir);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("heading1");
    testBuilder.baselineValues("1");
    testBuilder.go();
    verifyCountStar(jsonDir, 10201);
  }

  @Test
  public void testUpPromotionAndTypeCoercionInListsOfNull() throws Exception {
    Path jsonDir = copyFiles("list_of_null");
    // Run a query touching all the files and ensure that it returns the correct records
    String query = String.format("SELECT * FROM dfs.\"%s\"", jsonDir);
    testRunAndReturn(UserBitShared.QueryType.SQL, query);
    verifyCountStar(jsonDir, 1);
  }

  @Test
  public void testInvalidUpPromotionAndTypeCoercionInUnions() {
    Path jsonDir = copyFiles("invalid_mixed_file");
    String query = String.format("SELECT * FROM dfs.\"%s\"", jsonDir);
    assertThatExceptionOfType(Exception.class)
      .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
      .havingCause()
      .isInstanceOf(UserException.class)
      .withMessageContaining("Unable to coerce from the file's data type \"boolean\" to the column's data type \"int64\" in table")
      .withMessageContaining("invalid_mixed_file")
      .withMessageContaining(", column \"heading1\" and file")
      .withMessageContaining("mixed_file_int_bool.json");
  }

  @Test
  public void testInvalidUpPromotionAndTypeCoercionAcrossFiles() throws Exception {
    Path jsonDir = copyFiles("bigint_and_bool");
    String query = String.format("SELECT * FROM dfs.\"%s\"", jsonDir);
    assertThatExceptionOfType(Exception.class)
      .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
      .havingCause()
      .isInstanceOf(UserException.class)
      .withMessageContaining("Unable to coerce from the file's data type")
      .withMessageContaining("in table")
      .withMessageContaining("bigint_and_bool")
      .withMessageContaining(", column \"heading1\" and file");
  }

  @Test
  public void testUpPromotionAndTypeCoercionFromBooleanToVarchar() throws Exception {
    Path jsonDir = copyFiles("varchar_and_bool");
    // Run a query triggering a schema change
    triggerSchemaLearning(jsonDir);
    // Schema should have changed to (CHARACTER VARYING,CHARACTER VARYING) now, irrespective of which file was picked first
    assertThat(runDescribeQuery(jsonDir)).contains("heading1|CHARACTER VARYING").contains("heading2|CHARACTER VARYING");
    // Run a query touching all the files and ensure that it returns the correct records
    verifyRecords(jsonDir, "heading1", "red", "true", "blue", "false");
    verifyRecords(jsonDir, "heading2", "red", "true", "blue", "false");
    verifyCountStar(jsonDir, 4);
  }

  @Test
  public void testSingleColumnSelectOnBooleanAndNull() throws Exception {
    Path jsonDir = copyFiles("bool_and_null");
    // Run a query triggering a schema change
    triggerSchemaLearning(jsonDir);
    // Schema should have changed to (BOOLEAN,BOOLEAN) now, irrespective of which file was picked first
    assertThat(runDescribeQuery(jsonDir)).contains("heading1|BOOLEAN").contains("heading2|BOOLEAN");
    // Run a query touching all the files and ensure that it returns the correct records
    verifyRecords(jsonDir, "heading1", true, null);
    verifyRecords(jsonDir, "heading2", true, null);
    verifyCountStar(jsonDir, 2);
  }

  private void verifyRecords(Path jsonDir, String col, Object... values) throws Exception {
    String query = String.format("SELECT %s FROM dfs.\"%s\"", col, jsonDir);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns(col);
    for (Object value : values) {
      testBuilder.baselineValues(value);
    }
    testBuilder.go();
  }

  private void verifyCountStar(Path jsonDir, long result) throws Exception {
    String query = String.format("SELECT count(*) FROM dfs.\"%s\"", jsonDir);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(result);
    testBuilder.go();
  }

  private Path copyFiles(String dirName) {
    Path jsonDir = createTempDirWithName(dirName).toPath();
    writeDir(Paths.get("json/schema_changes/no_mixed_types/simple/"), jsonDir, dirName);
    return jsonDir;
  }

  private void triggerSchemaLearning(Path jsonDir) {
    String query = String.format("SELECT * FROM dfs.\"%s\"", jsonDir);
    assertThatExceptionOfType(Exception.class)
      .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
      .havingCause()
      .isInstanceOf(UserRemoteException.class)
      .withMessageContaining("New schema found");
  }

  private String runDescribeQuery(Path jsonDir) throws Exception {
    String query = String.format("describe dfs.\"%s\"", jsonDir);
    List<QueryDataBatch> queryDataBatches = testRunAndReturn(UserBitShared.QueryType.SQL, query);
    return getResultString(queryDataBatches, "|", false);
  }
}
