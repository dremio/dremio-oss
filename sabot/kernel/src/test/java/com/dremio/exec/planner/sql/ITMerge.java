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
 */
public class ITMerge extends ITDmlQueryBase {

  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testMalformedMergeQueries() throws Exception {
    MergeTestCases.testMalformedMergeQueries(SOURCE);
  }

  @Test
  public void testMergeUpdateAllWithLiteral() throws Exception {
    MergeTestCases.testMergeUpdateAllWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateAllWithScalar() throws Exception {
    MergeTestCases.testMergeUpdateAllWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateAllWithSubQuery() throws Exception {
    MergeTestCases.testMergeUpdateAllWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateHalfWithLiteral() throws Exception {
    MergeTestCases.testMergeUpdateHalfWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateHalfWithScalar() throws Exception {
    MergeTestCases.testMergeUpdateHalfWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateHalfWithSubQuery() throws Exception {
    MergeTestCases.testMergeUpdateHalfWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateWithFloat() throws Exception {
    MergeTestCases.testMergeUpdateWithFloat(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateUsingSubQueryWithLiteral() throws Exception {
    MergeTestCases.testMergeUpdateUsingSubQueryWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeInsertWithScalar() throws Exception {
    MergeTestCases.testMergeInsertWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeInsertWithLiteral() throws Exception {
    MergeTestCases.testMergeInsertWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithLiteral() throws Exception {
    MergeTestCases.testMergeUpdateInsertWithLiteral(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithScalar() throws Exception {
    MergeTestCases.testMergeUpdateInsertWithScalar(allocator, SOURCE);
  }

  @Test
  public void testMergeUpdateInsertWithSubQuery() throws Exception {
    MergeTestCases.testMergeUpdateInsertWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testMergeTargetTableWithAndWithoutAlias() throws Exception {
    MergeTestCases.testMergeTargetTableWithAndWithoutAlias(allocator, SOURCE);
  }

  @Test
  public void testMergeWithDupsInSource() throws Exception {
    MergeTestCases.testMergeWithDupsInSource(allocator, SOURCE);
  }
}
