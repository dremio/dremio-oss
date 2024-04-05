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

  @Test
  public void testMalformedMergeQueries() throws Exception {
    MergeTests.testMalformedMergeQueries(SOURCE);
  }

  @Test
  public void testMergeOnView() throws Exception {
    MergeTests.testMergeOnView(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateAllWithLiteral() throws Exception {
    MergeTests.testMergeUpdateAllWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateAllWithScalar() throws Exception {
    MergeTests.testMergeUpdateAllWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateAllWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateAllWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateHalfWithLiteral() throws Exception {
    MergeTests.testMergeUpdateHalfWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateHalfWithScalar() throws Exception {
    MergeTests.testMergeUpdateHalfWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateHalfWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateHalfWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateWithFloat() throws Exception {
    MergeTests.testMergeUpdateWithFloat(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateUsingSubQueryWithLiteral() throws Exception {
    MergeTests.testMergeUpdateUsingSubQueryWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateWithStar() throws Exception {
    MergeTests.testMergeUpdateWithStar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateWithStarColumnCountNotMatch1() throws Exception {
    MergeTests.testMergeUpdateWithStarColumnCountNotMatch1(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateWithStarColumnCountNotMatch2() throws Exception {
    MergeTests.testMergeUpdateWithStarColumnCountNotMatch2(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateWithStarSchemaNotMatchUpdateOnly() throws Exception {
    MergeTests.testMergeUpdateWithStarSchemaNotMatchUpdateOnly(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateWithStarSchemaNotMatch() throws Exception {
    MergeTests.testMergeUpdateWithStarSchemaNotMatch(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithStar() throws Exception {
    MergeTests.testMergeUpdateInsertWithStar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithReversedColumnSelectInSource() throws Exception {
    MergeTests.testMergeUpdateInsertWithReversedColumnSelectInSource(allocator, SOURCE);
  }

  @Test
  public void testMergeInsertWithScalar() throws Exception {
    MergeTests.testMergeInsertWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeInsertWithLiteral() throws Exception {
    MergeTests.testMergeInsertWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeInsertWithFloat() throws Exception {
    MergeTests.testMergeInsertWithFloat(allocator, SOURCE);
  }

  @Test
  public void testMergeInsertWithStar() throws Exception {
    MergeTests.testMergeInsertWithStar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithLiteral() throws Exception {
    MergeTests.testMergeUpdateInsertWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeInsertWithStarColumnCountNotMatch() throws Exception {
    MergeTests.testMergeInsertWithStarColumnCountNotMatch(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithFloats() throws Exception {
    MergeTests.testMergeUpdateInsertWithFloats(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithScalar() throws Exception {
    MergeTests.testMergeUpdateInsertWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateInsertWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertStarWithSubQuery() throws Exception {
    MergeTests.testMergeUpdateInsertStarWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testMergeWithSubQuerySourceAndInsert() throws Exception {
    MergeTests.testMergeWithSubQuerySourceAndInsert(allocator, SOURCE);
  }

  @Test
  public void testMergeWithSelectiveInsertColumns() throws Exception {
    MergeTests.testMergeWithSelectiveInsertColumns(allocator, SOURCE);
  }

  @Test
  public void testMergeTargetTableWithAndWithoutAlias() throws Exception {
    MergeTests.testMergeTargetTableWithAndWithoutAlias(allocator, SOURCE);
  }

  @Test
  public void testMergeWithDupsInSource() throws Exception {
    MergeTests.testMergeWithDupsInSource(allocator, SOURCE);
  }

  @Test
  public void testMergeWithWrongContextWithPathTable() throws Exception {
    MergeTests.testMergeWithWrongContextWithPathTable(allocator, SOURCE);
  }

  @Test
  public void testMergeWithContextWithPathTable() throws Exception {
    MergeTests.testMergeWithContextWithPathTable(allocator, SOURCE);
  }

  @Test
  public void testMergeWithStockIcebergTargetTable() throws Exception {
    MergeTests.testMergeWithStockIcebergTargetTable(allocator, SOURCE);
  }
}
