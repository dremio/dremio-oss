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

import com.dremio.exec.planner.sql.DmlQueryTestUtils.DmlRowwiseOperationWriteMode;
import org.junit.Test;

/**
 * Runs test cases on the local filesystem-based Hadoop source.
 *
 * <p>Note: Contains all tests in MergeTests.
 */
public class ITMerge extends ITDmlQueryBase {

  // Defining SOURCE such that you can easily copy and paste the same test across other test
  // variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;
  private static final DmlRowwiseOperationWriteMode dmlWriteMode =
      DmlRowwiseOperationWriteMode.COPY_ON_WRITE;

  @Test
  public void testMalformedMergeQueries() throws Exception {
    MergeTests.testMalformedMergeQueries(SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeOnView() throws Exception {
    MergeTests.testMergeOnView(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateAllWithLiteral() throws Exception {
    MergeTests.testMergeUpdateAllWithLiteral(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateAllWithScalar() throws Exception {
    MergeTests.testMergeUpdateAllWithScalar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateAllWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateAllWithSubQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateHalfWithLiteral() throws Exception {
    MergeTests.testMergeUpdateHalfWithLiteral(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateHalfWithScalar() throws Exception {
    MergeTests.testMergeUpdateHalfWithScalar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateHalfWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateHalfWithSubQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateWithFloat() throws Exception {
    MergeTests.testMergeUpdateWithFloat(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateUsingSubQueryWithLiteral() throws Exception {
    MergeTests.testMergeUpdateUsingSubQueryWithLiteral(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateWithStar() throws Exception {
    MergeTests.testMergeUpdateWithStar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateWithStarColumnCountNotMatch1() throws Exception {
    MergeTests.testMergeUpdateWithStarColumnCountNotMatch1(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateWithStarColumnCountNotMatch2() throws Exception {
    MergeTests.testMergeUpdateWithStarColumnCountNotMatch2(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateWithStarSchemaNotMatchUpdateOnly() throws Exception {
    MergeTests.testMergeUpdateWithStarSchemaNotMatchUpdateOnly(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateWithStarSchemaNotMatch() throws Exception {
    MergeTests.testMergeUpdateWithStarSchemaNotMatch(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateInsertWithStar() throws Exception {
    MergeTests.testMergeUpdateInsertWithStar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateInsertWithReversedColumnSelectInSource() throws Exception {
    MergeTests.testMergeUpdateInsertWithReversedColumnSelectInSource(
        allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeInsertWithScalar() throws Exception {
    MergeTests.testMergeInsertWithScalar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeInsertWithLiteral() throws Exception {
    MergeTests.testMergeInsertWithLiteral(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeInsertWithFloat() throws Exception {
    MergeTests.testMergeInsertWithFloat(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeInsertWithStar() throws Exception {
    MergeTests.testMergeInsertWithStar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeInsertWrongColumnOrderWithStar() throws Exception {
    MergeTests.testMergeInsertWrongColumnOrderWithStar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateInsertWithLiteral() throws Exception {
    MergeTests.testMergeUpdateInsertWithLiteral(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeInsertWithStarColumnCountNotMatch() throws Exception {
    MergeTests.testMergeInsertWithStarColumnCountNotMatch(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateInsertWithFloats() throws Exception {
    MergeTests.testMergeUpdateInsertWithFloats(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateInsertWithScalar() throws Exception {
    MergeTests.testMergeUpdateInsertWithScalar(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateInsertWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateInsertWithSubQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeUpdateInsertStarWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateInsertStarWithSubQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeWithSubQuerySourceAndInsert() throws Exception {
    MergeTests.testMergeWithSubQuerySourceAndInsert(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeWithSelectiveInsertColumns() throws Exception {
    MergeTests.testMergeWithSelectiveInsertColumns(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeWithUnOrderedInsertColumns() throws Exception {
    MergeTests.testMergeWithUnOrderedInsertColumns(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeTargetTableWithAndWithoutAlias() throws Exception {
    MergeTests.testMergeTargetTableWithAndWithoutAlias(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeWithDupsInSource() throws Exception {
    MergeTests.testMergeWithDupsInSource(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeWithWrongContextWithPathTable() throws Exception {
    MergeTests.testMergeWithWrongContextWithPathTable(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeWithContextWithPathTable() throws Exception {
    MergeTests.testMergeWithContextWithPathTable(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testMergeWithStockIcebergTargetTable() throws Exception {
    MergeTests.testMergeWithStockIcebergTargetTable(allocator, SOURCE, dmlWriteMode);
  }
}
