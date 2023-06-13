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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.rpc.user.QueryDataBatch;

public class InternalSchemaTestBase extends PlanTestBase {

  @BeforeClass
  public static void setUp() {
    setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "true");
    setSystemOption(ExecConstants.ENABLE_INTERNAL_SCHEMA, "true");
  }

  @AfterClass
  public static void cleanUp() {
    setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT, PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG,
      ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_INTERNAL_SCHEMA, ExecConstants.ENABLE_INTERNAL_SCHEMA.getDefault().getBoolVal().toString());
  }

  void verifyRecords(String dirName, String col, Object... values) throws Exception {
    String query = String.format("SELECT %s FROM dfs_test.\"%s\"", col, dirName);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns(col);
    for (Object value : values) {
      testBuilder.baselineValues(value);
    }
    testBuilder.go();
  }

  void runMetadataRefresh(String dirName) throws Exception {
    String query = String.format("alter table dfs_test.\"%s\" refresh metadata force update", dirName);
    runSQL(query);
  }

  void verifyCountStar(String dirName, long result) throws Exception {
    String query = String.format("SELECT count(*) FROM dfs_test.\"%s\"", dirName);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(result);
    testBuilder.go();
  }

  Path copyFilesFromNoMixedTypesSimple(String dirName) {
    return copyFiles(dirName, "json/schema_changes/no_mixed_types/simple/");
  }

  Path copyFilesFromNoMixedTypesComplex(String dirName) {
    return copyFiles(dirName, "json/schema_changes/no_mixed_types/complex/");
  }

  Path copyFilesFromInternalSchemaSimple(String dirName) {
    return copyFiles(dirName, "json/schema_changes/internal_schema/simple/");
  }

  Path copyFilesFromInternalSchemaComplex(String dirName) {
    return copyFiles(dirName, "json/schema_changes/internal_schema/complex/");
  }

  private Path copyFiles(String dirName, String root) {
    Path jsonDir = createDfsTestTableDirWithName(dirName).toPath();
    writeDir(Paths.get(root), jsonDir, dirName);
    return jsonDir;
  }

  void promoteDataset(String dirName) throws Exception {
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    runSQL(query);
  }

  void alterTableChangeColumn(String dirName, String columnName, String dataType) throws Exception {
    String query = String.format("ALTER TABLE dfs_test.\"%s\" CHANGE COLUMN %s %s %s", dirName, columnName, columnName, dataType);
    runSQL(query);
  }

  void alterTableDropColumn(String dirName, String columnName) throws Exception {
    String query = String.format("ALTER TABLE dfs_test.\"%s\" DROP COLUMN %s", dirName, columnName);
    runSQL(query);
  }

  void alterTableDisableSchemaLearning(String dirName) throws Exception {
    String query = String.format("ALTER TABLE dfs_test.\"%s\" DISABLE SCHEMA LEARNING", dirName);
    runSQL(query);
  }

  void alterTableEnableSchemaLearning(String dirName) throws Exception {
    String query = String.format("ALTER TABLE dfs_test.\"%s\" ENABLE SCHEMA LEARNING", dirName);
    runSQL(query);
  }

  void alterTableForgetMetadata(String dirName) throws Exception {
    String query = String.format("ALTER TABLE dfs_test.\"%s\" forget metadata", dirName);
    runSQL(query);
  }

  void triggerSchemaLearning(String dirName) {
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    assertThatExceptionOfType(Exception.class)
      .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
      .havingCause()
      .isInstanceOf(UserRemoteException.class)
      .withMessageContaining("New schema found");
  }

  void triggerOptionalSchemaLearning(String dirName) {
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    try {
      testRunAndReturn(UserBitShared.QueryType.SQL, query);
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof UserRemoteException);
      assertTrue(e.getCause().getMessage().contains("New schema found"));
    }
  }

  String runDescribeQuery(String dirName) throws Exception {
    String query = String.format("describe dfs_test.\"%s\"", dirName);
    List<QueryDataBatch> queryDataBatches = testRunAndReturn(UserBitShared.QueryType.SQL, query);
    return getResultString(queryDataBatches, "|", false);
  }

  void assertCoercionFailure(String dirName, String fileType, String tableType) {
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    assertThatExceptionOfType(Exception.class)
      .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
      .havingCause()
      .isInstanceOf(UserException.class)
      .withMessageContaining("UNSUPPORTED_OPERATION ERROR: Unable to coerce from the file's data type")
      .withMessageContaining(fileType)
      .withMessageContaining("to the column's data type")
      .withMessageContaining(tableType)
      .withMessageContaining("in table")
      .withMessageContaining(dirName)
      .withMessageContaining("and file")
      .withMessageContaining(".json");
  }

  void assertCastFailure(String dirName) {
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    assertThatExceptionOfType(Exception.class)
      .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
      .havingCause()
      .isInstanceOf(UserException.class)
      .withMessageContaining("GandivaException: Failed to cast");
  }
}
