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
public class ITUpdate extends ITDmlQueryBase {

  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testMalformedUpdateQueries() throws Exception {
    UpdateTestCases.testMalformedUpdateQueries(SOURCE);
  }

  @Test
  public void testUpdateAll() throws Exception {
    UpdateTestCases.testUpdateAll(allocator, SOURCE);
  }

  @Test
  public void testUpdateAllColumns() throws Exception {
    UpdateTestCases.testUpdateAllColumns(allocator, SOURCE);
  }

  @Test
  public void testUpdateById() throws Exception {
    UpdateTestCases.testUpdateById(allocator, SOURCE);
  }

  @Test
  public void testUpdateTargetTableWithAndWithoutAlias() throws Exception {
    UpdateTestCases.testUpdateTargetTableWithAndWithoutAlias(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdInEquality() throws Exception {
    UpdateTestCases.testUpdateByIdInEquality(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithInClauseFilter() throws Exception {
    UpdateTestCases.testUpdateWithInClauseFilter(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdAndColumn0() throws Exception {
    UpdateTestCases.testUpdateByIdAndColumn0(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdOrColumn0() throws Exception {
    UpdateTestCases.testUpdateByIdOrColumn0(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdTwoColumns() throws Exception {
    UpdateTestCases.testUpdateByIdTwoColumns(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQuery() throws Exception {
    UpdateTestCases.testUpdateByIdWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQueryWithDecimal() throws Exception {
    UpdateTestCases.testUpdateByIdWithSubQueryWithDecimal(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithScalarSubQuery() throws Exception {
    UpdateTestCases.testUpdateByIdWithScalarSubQuery(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQueries() throws Exception {
    UpdateTestCases.testUpdateByIdWithSubQueries(allocator, SOURCE);
  }
}
