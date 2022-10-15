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
 * Note: Contains all tests in UpdateTests.
 */
public class ITUpdate extends ITDmlQueryBase {

  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testMalformedUpdateQueries() throws Exception {
    UpdateTests.testMalformedUpdateQueries(SOURCE);
  }

  @Test
  public void testUpdateOnView() throws Exception {
    UpdateTests.testUpdateOnView(allocator, SOURCE);
  }

  @Test
  public void testUpdateAll() throws Exception {
    UpdateTests.testUpdateAll(allocator, SOURCE);
  }

  @Test
  public void testUpdateAllColumns() throws Exception {
    UpdateTests.testUpdateAllColumns(allocator, SOURCE);
  }

  @Test
  public void testUpdateById() throws Exception {
    UpdateTests.testUpdateById(allocator, SOURCE);
  }

  @Test
  public void testUpdateTargetTableWithAndWithoutAlias() throws Exception {
    UpdateTests.testUpdateTargetTableWithAndWithoutAlias(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdInEquality() throws Exception {
    UpdateTests.testUpdateByIdInEquality(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithInClauseFilter() throws Exception {
    UpdateTests.testUpdateWithInClauseFilter(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdAndColumn0() throws Exception {
    UpdateTests.testUpdateByIdAndColumn0(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdOrColumn0() throws Exception {
    UpdateTests.testUpdateByIdOrColumn0(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdTwoColumns() throws Exception {
    UpdateTests.testUpdateByIdTwoColumns(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithDecimals() throws Exception {
    UpdateTests.testUpdateByIdWithDecimals(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithDoubles() throws Exception {
    UpdateTests.testUpdateByIdWithDoubles(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithFloats() throws Exception {
    UpdateTests.testUpdateByIdWithFloats(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQuery() throws Exception {
    UpdateTests.testUpdateByIdWithSubQuery(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQueryWithFloat() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueryWithFloat(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQueryAndLiteralWithFloats() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueryAndLiteralWithFloats(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQueryWithDecimal() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueryWithDecimal(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithScalarSubQuery() throws Exception {
    UpdateTests.testUpdateByIdWithScalarSubQuery(allocator, SOURCE);
  }

  @Test
  public void testUpdateByIdWithSubQueries() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueries(allocator, SOURCE);
  }

  @Test
  public void testUpdateALLWithOneUnusedSourceTable() throws Exception {
    UpdateTests.testUpdateALLWithOneUnusedSourceTable(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithOneSourceTableFullPath() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableFullPath(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithViewAsSourceTable() throws Exception {
    UpdateTests.testUpdateWithViewAsSourceTable(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithOneSourceTableNoAlias() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableNoAlias(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithOneSourceTableUseAlias() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableUseAlias(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithOneSourceTableSubQuery() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableSubQuery(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithTwoSourceTables() throws Exception {
    UpdateTests.testUpdateWithTwoSourceTables(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithTwoSourceTableOneSourceQuery() throws Exception {
    UpdateTests.testUpdateWithTwoSourceTableOneSourceQuery(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithDupsInSource() throws Exception {
    UpdateTests.testUpdateWithDupsInSource(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithUnrelatedConditionToSourceTableNoCondition() throws Exception {
    UpdateTests.testUpdateWithUnrelatedConditionToSourceTableNoCondition(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithUnrelatedConditionToSourceTable() throws Exception {
    UpdateTests.testUpdateWithUnrelatedConditionToSourceTable(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithSchemaNotMatch() throws Exception {
    UpdateTests.testUpdateWithSchemaNotMatch(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithImplicitTypeCasting1() throws Exception {
    UpdateTests.testUpdateWithImplicitTypeCasting1(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithImplicitTypeCasting2() throws Exception {
    UpdateTests.testUpdateWithImplicitTypeCasting2(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithWrongContextWithPathTable() throws Exception {
    UpdateTests.testUpdateWithWrongContextWithPathTable(allocator, SOURCE);
  }

  @Test
  public void testUpdateWithContextWithPathTable() throws Exception {
    UpdateTests.testUpdateWithContextWithPathTable(allocator, SOURCE);
  }
}
