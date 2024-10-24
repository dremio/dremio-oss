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
package com.dremio.exec.planner.sql;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.ITCopyIntoBase.FileFormat;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.OnErrorAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITCopyIntoTransformations extends ITDmlQueryBase {
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;
  private static AutoCloseable enableTransformations;

  @BeforeClass
  public static void setup() throws Exception {
    enableTransformations = withSystemOption(ExecConstants.COPY_INTO_ENABLE_TRANSFORMATIONS, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    enableTransformations.close();
  }

  @Test
  public void testNoMappingNoOnErrorOption() throws Exception {
    CopyIntoTransformationTests.testNoMapping(allocator, SOURCE, null, FileFormat.PARQUET);
  }

  @Test
  public void testNoMappingOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testNoMapping(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testNoMappingOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testNoMapping(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testWithMappingOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testWithMapping(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testWithMappingOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testWithMapping(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testInvalidMapping() throws Exception {
    CopyIntoTransformationTests.testInvalidMapping(SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testUnsupportedFileFormat() throws Exception {
    CopyIntoTransformationTests.testUnsupportedFileFormat(SOURCE, FileFormat.JSON);
  }

  @Test
  public void testMultipleInputsOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testMultipleInputs(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testMultipleInputsOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testMultipleInputs(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testIncompatibleTransformationTypesOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testIncompatibleTransformationTypes(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testIncompatibleTransformationTypesOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testIncompatibleTransformationTypes(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveTransformationsOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testPrimitiveTransformations(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveTransformationsOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testPrimitiveTransformations(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveNestedTransformationsOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testPrimitiveNestedTransformations(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveNestedTransformationsOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testPrimitiveNestedTransformations(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveFromComplexOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testPrimitiveFromComplex(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveFromComplexOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testPrimitiveFromComplex(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testMapComplexTypeOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testMapComplexType(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testMapComplexTypeOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testMapComplexType(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testComplexFromComplexOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testComplexFromComplex(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testComplexFromComplexOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testComplexFromComplex(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveTransformationFromComplexOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testPrimitiveTransformationFromComplex(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testPrimitiveTransformationFromComplexOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testPrimitiveTransformationFromComplex(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testSelectNonExistentColumnFromSource() throws Exception {
    CopyIntoTransformationTests.testSelectNonExistentColumnFromSource(
        SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testTypeErrorOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testTypeError(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testTypeErrorOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testTypeError(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testSyntaxErrorOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testSyntaxError(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testSyntaxErrorOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testSyntaxError(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testMultipleInputsWithErrorOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testMultipleInputsWithError(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testMultipleInputsWithErrorOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testMultipleInputsWithError(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }

  @Test
  public void testComplexWithRepeatingNamesOnErrorAbort() throws Exception {
    CopyIntoTransformationTests.testComplexWithRepeatingNames(
        allocator, SOURCE, OnErrorAction.ABORT, FileFormat.PARQUET);
  }

  @Test
  public void testComplexWithRepeatingNamesOnErrorSkipFile() throws Exception {
    CopyIntoTransformationTests.testComplexWithRepeatingNames(
        allocator, SOURCE, OnErrorAction.SKIP_FILE, FileFormat.PARQUET);
  }
}
