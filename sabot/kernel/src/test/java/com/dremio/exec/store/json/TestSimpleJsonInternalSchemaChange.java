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
import static org.junit.Assert.fail;

import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.io.Resources;
import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class TestSimpleJsonInternalSchemaChange extends InternalSchemaTestBase {

  @Test
  public void testInternalSchemaChangesForBigint() throws Exception {
    String dirName = "bigint";
    copyFilesFromInternalSchemaSimple(dirName);
    promoteDataset(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");
    verifyRecords(dirName, "heading1", 12L);
    verifyCountStar(dirName, 1L);

    // supported
    alterTableChangeColumn(dirName, "heading1", "INT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");
    verifyRecords(dirName, "heading1", 12);
    verifyCountStar(dirName, 1L);

    // Run metadata refresh and verify that dropped fields are honoured
    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");
    verifyRecords(dirName, "heading1", 12);
    verifyCountStar(dirName, 1L);

    alterTableChangeColumn(dirName, "heading1", "FLOAT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");
    verifyRecords(dirName, "heading1", 12.0F);
    verifyCountStar(dirName, 1L);

    // Run metadata refresh and verify that schema didn't change
    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");

    alterTableChangeColumn(dirName, "heading1", "DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");
    verifyRecords(dirName, "heading1", 12.0);
    verifyCountStar(dirName, 1L);

    // Run metadata refresh and verify schema doesn't change
    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");

    alterTableChangeColumn(dirName, "heading1", "DECIMAL(3,1)");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");
    verifyRecords(dirName, "heading1", BigDecimal.valueOf(12));
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");

    alterTableChangeColumn(dirName, "heading1", "VARCHAR");
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");
    verifyRecords(dirName, "heading1", "12");
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");

    // Unsupported
    alterTableChangeColumn(dirName, "heading1", "BOOLEAN");
    assertThat(runDescribeQuery(dirName)).contains("heading1|BOOLEAN");
    assertCoercionFailure(dirName, "int64", "boolean");

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BOOLEAN");
    alterTableForgetMetadata(dirName);
  }

  @Test
  public void testInternalSchemaChangesForBoolean() throws Exception {
    String dirName = "boolean";
    copyFilesFromInternalSchemaSimple(dirName);
    promoteDataset(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BOOLEAN");
    verifyRecords(dirName, "heading1", true);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BOOLEAN");

    // supported
    alterTableChangeColumn(dirName, "heading1", "VARCHAR");
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");
    verifyRecords(dirName, "heading1", "true");
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");

    // Unsupported
    alterTableChangeColumn(dirName, "heading1", "INT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");
    assertCoercionFailure(dirName, "boolean", "int32");

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");

    alterTableChangeColumn(dirName, "heading1", "BIGINT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");
    assertCoercionFailure(dirName, "boolean", "int64");

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");

    alterTableChangeColumn(dirName, "heading1", "FLOAT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");
    assertCoercionFailure(dirName, "boolean", "float");

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");

    alterTableChangeColumn(dirName, "heading1", "DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");
    assertCoercionFailure(dirName, "boolean", "double");

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");

    alterTableChangeColumn(dirName, "heading1", "DECIMAL(3,1)");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");
    assertCoercionFailure(dirName, "boolean", "decimal");

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");
  }

  @Test
  public void testInternalSchemaChangesForDouble() throws Exception {
    String dirName = "double";
    copyFilesFromInternalSchemaSimple(dirName);
    promoteDataset(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");
    verifyRecords(dirName, "heading1", 12.3);
    verifyCountStar(dirName, 1L);

    // supported
    alterTableChangeColumn(dirName, "heading1", "INT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");
    verifyRecords(dirName, "heading1", 12);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");

    alterTableChangeColumn(dirName, "heading1", "BIGINT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");
    verifyRecords(dirName, "heading1", 12L);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");

    alterTableChangeColumn(dirName, "heading1", "FLOAT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");
    verifyRecords(dirName, "heading1", 12.3F);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");

    alterTableChangeColumn(dirName, "heading1", "DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");
    verifyRecords(dirName, "heading1", 12.3);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");

    alterTableChangeColumn(dirName, "heading1", "DECIMAL(3,1)");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");
    verifyRecords(dirName, "heading1", BigDecimal.valueOf(12.3));
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");

    alterTableChangeColumn(dirName, "heading1", "VARCHAR");
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");
    verifyRecords(dirName, "heading1", "12.3");
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");

    // Unsupported
    alterTableChangeColumn(dirName, "heading1", "BOOLEAN");
    assertThat(runDescribeQuery(dirName)).contains("heading1|BOOLEAN");
    assertCoercionFailure(dirName, "double", "boolean");

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BOOLEAN");
  }

  @Test
  public void testInternalSchemaChangesForVarchar() throws Exception {
    String dirName = "varchar";
    copyFilesFromInternalSchemaSimple(dirName);
    promoteDataset(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");
    verifyRecords(dirName, "heading1", "12");
    verifyRecords(dirName, "heading2", "true");
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");

    // supported
    alterTableChangeColumn(dirName, "heading1", "INT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");
    verifyRecords(dirName, "heading1", 12);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");

    alterTableChangeColumn(dirName, "heading1", "BIGINT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");
    verifyRecords(dirName, "heading1", 12L);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");

    alterTableChangeColumn(dirName, "heading1", "FLOAT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");
    verifyRecords(dirName, "heading1", 12.0F);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");

    alterTableChangeColumn(dirName, "heading1", "DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");
    verifyRecords(dirName, "heading1", 12.0);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");

    alterTableChangeColumn(dirName, "heading1", "DECIMAL(3,1)");
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");
    verifyRecords(dirName, "heading1", BigDecimal.valueOf(12));
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DECIMAL");

    alterTableChangeColumn(dirName, "heading2", "BOOLEAN");
    assertThat(runDescribeQuery(dirName)).contains("heading2|BOOLEAN");
    verifyRecords(dirName, "heading2", true);
    verifyCountStar(dirName, 1L);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading2|BOOLEAN");

    // Unsupported
    alterTableChangeColumn(dirName, "heading2", "INT");
    assertThat(runDescribeQuery(dirName)).contains("heading2|INT");
    assertCastFailure(dirName);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading2|INT");

    alterTableChangeColumn(dirName, "heading2", "BIGINT");
    assertThat(runDescribeQuery(dirName)).contains("heading2|BIGINT");
    assertCastFailure(dirName);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading2|BIGINT");

    alterTableChangeColumn(dirName, "heading2", "FLOAT");
    assertThat(runDescribeQuery(dirName)).contains("heading2|FLOAT");
    assertCastFailure(dirName);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading2|FLOAT");

    alterTableChangeColumn(dirName, "heading2", "DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("heading2|DOUBLE");
    assertCastFailure(dirName);

    runMetadataRefresh(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading2|DOUBLE");

    alterTableChangeColumn(dirName, "heading2", "DECIMAL(3,1)");
    assertThat(runDescribeQuery(dirName)).contains("heading2|DECIMAL");
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    assertThatExceptionOfType(Exception.class)
        .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .withMessageContaining("FUNCTION ERROR");

    alterTableChangeColumn(dirName, "heading1", "BOOLEAN");
    assertThat(runDescribeQuery(dirName)).contains("heading1|BOOLEAN");
    assertThatExceptionOfType(Exception.class)
        .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .withMessageContaining("FUNCTION ERROR: Invalid value for boolean");
  }

  @Test
  public void testInternalSchemaAfterSchemaLearning() throws Exception {
    String dirName = "double_and_bigint";
    copyFilesFromNoMixedTypesSimple(dirName);
    // Run a query triggering a schema change
    triggerOptionalSchemaLearning(dirName);
    // Schema should have changed to (DOUBLE,DOUBLE) now, irrespective of which file was picked
    // first
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE").contains("heading2|DOUBLE");
    // Run a query touching all the files and ensure that it returns the correct records
    verifyRecords(dirName, "heading1", 12.3, 12.0, 12.4, 13.0);
    verifyRecords(dirName, "heading2", 12.3, 12.0, 12.4, 13.0);
    verifyCountStar(dirName, 4);

    alterTableChangeColumn(dirName, "heading1", "FLOAT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");
    verifyRecords(dirName, "heading1", 12.3F, 12.0F, 12.4F, 13.0F);
    verifyCountStar(dirName, 4);
    alterTableChangeColumn(dirName, "heading1", "DOUBLE");
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dirName));
  }

  @Test
  public void testDropColumnBeforeSchemaLearning() throws Exception {
    String dirName = "varchar_and_bool";
    copyFilesFromNoMixedTypesSimple(dirName);
    // Run a query triggering a schema change
    alterTableDropColumn(dirName, "heading1");
    assertThat(runDescribeQuery(dirName)).doesNotContain("heading1");
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dirName));
  }

  @Test
  public void testDropColumnAfterSchemaLearning() throws Exception {
    String dirName = "varchar_and_bigint";
    copyFilesFromNoMixedTypesSimple(dirName);
    // Run a query triggering a schema change
    triggerOptionalSchemaLearning(dirName);
    assertThat(runDescribeQuery(dirName))
        .contains("heading1|CHARACTER VARYING")
        .contains("heading2|CHARACTER VARYING");

    alterTableDropColumn(dirName, "heading1");
    assertThat(runDescribeQuery(dirName)).doesNotContain("heading1");
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dirName));
  }

  @Test
  public void testInternalSchemaBeforeSchemaLearning() throws Exception {
    String dirName = "double_and_bigint";
    copyFilesFromNoMixedTypesSimple(dirName);
    // Run a query triggering a schema change
    alterTableChangeColumn(dirName, "heading1", "FLOAT");
    assertThat(runDescribeQuery(dirName)).contains("heading1|FLOAT");
    verifyRecords(dirName, "heading1", 12.3F, 12.0F, 12.4F, 13.0F);
    verifyRecords(dirName, "heading2", 12.3, 12.0, 12.4, 13.0);
    verifyCountStar(dirName, 4);
    alterTableChangeColumn(dirName, "heading1", "DOUBLE");
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dirName));
  }

  // If the values in filters and data can't be cast, cast function fails
  @Test
  public void testInternalSchemaFliterCastFailure() throws Exception {
    String dirName = "large_mixed_file";
    copyFilesFromNoMixedTypesSimple(dirName);
    // Run a query touching all the files and ensure that it returns the correct records
    String query =
        String.format("SELECT * FROM dfs_test.\"%s\" where heading1 != 'hello'", dirName);
    TestBuilder testBuilder = testBuilder().sqlQuery(query).unOrdered().baselineColumns("heading1");
    testBuilder.baselineValues("1");
    testBuilder.go();
    verifyCountStar(dirName, 10201);

    alterTableChangeColumn(dirName, "heading1", "INT");
    String query2 = String.format("SELECT * FROM dfs_test.\"%s\" where heading1 = 1", dirName);
    assertThatExceptionOfType(Exception.class)
        .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query2))
        .havingCause()
        .isInstanceOf(UserException.class)
        .withMessageContaining("GandivaException: Failed to cast the string hello to int32_t");
  }

  // Internal schema does not work where we cannot promote a dataset
  @Test
  public void testInternalSchemaOnInvalidMixedFile() {
    String dirName = "invalid_mixed_file";
    copyFilesFromNoMixedTypesSimple(dirName);
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    assertThatExceptionOfType(Exception.class)
        .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
        .havingCause()
        .isInstanceOf(UserException.class)
        .withMessageContaining(
            "Unable to coerce from the file's data type \"boolean\" to the column's data type \"int64\" in table")
        .withMessageContaining("invalid_mixed_file")
        .withMessageContaining(", column \"heading1\" and file")
        .withMessageContaining("mixed_file_int_bool.json");

    assertThatExceptionOfType(Exception.class)
        .isThrownBy(() -> alterTableChangeColumn(dirName, "heading1", "varchar"))
        .isInstanceOf(UserException.class)
        .withMessageContaining(
            "Unable to coerce from the file's data type \"boolean\" to the column's data type \"int64\" in table")
        .withMessageContaining("invalid_mixed_file")
        .withMessageContaining(", column \"heading1\" and file")
        .withMessageContaining("mixed_file_int_bool.json");
  }

  @Test
  public void testInternalSchemaAcrossInvalidFiles() throws Exception {
    String dirName = "bigint_and_bool";
    copyFilesFromNoMixedTypesSimple(dirName);
    String query = String.format("SELECT * FROM dfs_test.\"%s\"", dirName);
    assertThatExceptionOfType(Exception.class)
        .isThrownBy(() -> testRunAndReturn(UserBitShared.QueryType.SQL, query))
        .havingCause()
        .isInstanceOf(UserException.class)
        .withMessageContaining("Unable to coerce from the file's data type")
        .withMessageContaining("in table")
        .withMessageContaining("bigint_and_bool")
        .withMessageContaining(", column \"heading1\" and file");
    alterTableChangeColumn(dirName, "heading1", "varchar");
    assertThat(runDescribeQuery(dirName)).contains("heading1|CHARACTER VARYING");
    verifyRecords(dirName, "heading1", "12", "true");
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dirName));
  }

  @Test
  public void testInternalSchemaColumnNameChange() throws Exception {
    String dirName = "double_and_bigint";
    copyFilesFromNoMixedTypesSimple(dirName);
    String query =
        String.format(
            "ALTER TABLE dfs_test.\"%s\" CHANGE COLUMN heading1 heading3 varchar", dirName);
    assertThatExceptionOfType(Exception.class)
        .isThrownBy(() -> runSQL(query))
        .isInstanceOf(UserException.class)
        .withMessageContaining("VALIDATION ERROR: Column [heading1] cannot be renamed");
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dirName));
  }

  @Test
  public void testInternalSchemaShouldErrorOutOnPartitionColumns() throws Exception {
    String dirName = "partitionDataset";
    Path jsonDir = createDfsTestTableDirWithName(dirName).toPath();
    copyFromJar("json/schema_changes/internal_schema/simple/partitionDataset", jsonDir);

    String query = "SELECT * FROM dfs_test.partitionDataset";
    runSQL(query);

    assertThatExceptionOfType(Exception.class)
        .isThrownBy(
            () ->
                testRunAndReturn(
                    UserBitShared.QueryType.SQL,
                    "ALTER TABLE dfs_test.partitionDataset CHANGE COLUMN dir0 dir0 int"))
        .havingCause()
        .isInstanceOf(UserException.class)
        .withMessageContaining(
            "Modifications to partition columns are not allowed. Column dir0 is a partition column");
  }

  @Test
  public void testDisableEnableSchemaLearningFlag() throws Exception {
    String dirName = "double_and_bigint";
    Path jsonDir = copyFilesFromNoMixedTypesSimple(dirName);
    // Run a query triggering a schema change
    triggerOptionalSchemaLearning(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("heading2|DOUBLE");
    verifyRecords(dirName, "heading1", 12.3, 12.0, 12.4, 13.0);
    verifyRecords(dirName, "heading2", 12.3, 12.0, 12.4, 13.0);

    alterTableDisableSchemaLearning(dirName);
    Path path =
        Paths.get(
            "json/schema_changes/no_mixed_types/complex/array_array_double_bigint/array_array_double_bigint.json");
    URL resource = Resources.getResource(path.toString());
    java.nio.file.Files.write(
        jsonDir.resolve("array_array_double_bigint.json"), Resources.toByteArray(resource));

    runMetadataRefresh(dirName);
    triggerOptionalSchemaLearning(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("heading2|DOUBLE");
    assertThat(runDescribeQuery(dirName)).doesNotContain("col1");
    assertThat(runDescribeQuery(dirName)).doesNotContain("col2");

    alterTableChangeColumn(dirName, "heading1", "INT");
    runMetadataRefresh(dirName);
    triggerOptionalSchemaLearning(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");
    assertThat(runDescribeQuery(dirName)).contains("heading2|DOUBLE");
    assertThat(runDescribeQuery(dirName)).doesNotContain("col1");
    assertThat(runDescribeQuery(dirName)).doesNotContain("col2");

    alterTableEnableSchemaLearning(dirName);
    runMetadataRefresh(dirName);
    triggerOptionalSchemaLearning(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|INT");
    assertThat(runDescribeQuery(dirName)).contains("heading2|DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("col1");
    assertThat(runDescribeQuery(dirName)).contains("col2");

    alterTableChangeColumn(dirName, "heading1", "BIGINT");
    alterTableDisableSchemaLearning(dirName);
    runMetadataRefresh(dirName);
    triggerOptionalSchemaLearning(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");
    assertThat(runDescribeQuery(dirName)).contains("heading2|DOUBLE");
    assertThat(runDescribeQuery(dirName)).contains("col1");
    assertThat(runDescribeQuery(dirName)).contains("col2");

    alterTableEnableSchemaLearning(dirName);
    alterTableChangeColumn(dirName, "heading1", "DOUBLE");
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dirName));
  }

  @Test
  public void testInternalSchemaChangesPrimitiveToComplex() throws Exception {
    String dirName = "bigint";
    copyFilesFromInternalSchemaSimple(dirName);
    promoteDataset(dirName);
    assertThat(runDescribeQuery(dirName)).contains("heading1|BIGINT");
    verifyRecords(dirName, "heading1", 12L);
    try {
      alterTableChangeColumn(dirName, "heading1", "list<bigint>");
      fail("Primitive type cannot be changed to complex");
    } catch (UserRemoteException e) {
      assertThat(e.getMessage())
          .contains(
              "INVALID_DATASET_METADATA ERROR: Field heading1: Int(64, true) and List are incompatible types, for type changes please ensure both columns are either of primitive types or complex but not mixed.");
    }
    alterTableForgetMetadata(dirName);
  }
}
