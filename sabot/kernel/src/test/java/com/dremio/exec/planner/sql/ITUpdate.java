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
 * <p>Note: Contains all tests in UpdateTests.
 */
public class ITUpdate extends ITDmlQueryBase {

  // Defining SOURCE such that you can easily copy and paste the same test across other test
  // variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  private static DmlRowwiseOperationWriteMode dmlWriteMode =
      DmlRowwiseOperationWriteMode.COPY_ON_WRITE;

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
    UpdateTests.testUpdateAll(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateAllColumns() throws Exception {
    UpdateTests.testUpdateAllColumns(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateById() throws Exception {
    UpdateTests.testUpdateById(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithEqualNull() throws Exception {
    UpdateTests.testUpdateByIdWithEqualNull(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateTargetTableWithAndWithoutAlias() throws Exception {
    UpdateTests.testUpdateTargetTableWithAndWithoutAlias(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdInEquality() throws Exception {
    UpdateTests.testUpdateByIdInEquality(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithInClauseFilter() throws Exception {
    UpdateTests.testUpdateWithInClauseFilter(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdAndColumn0() throws Exception {
    UpdateTests.testUpdateByIdAndColumn0(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdOrColumn0() throws Exception {
    UpdateTests.testUpdateByIdOrColumn0(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdTwoColumns() throws Exception {
    UpdateTests.testUpdateByIdTwoColumns(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithDecimals() throws Exception {
    UpdateTests.testUpdateByIdWithDecimals(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithDoubles() throws Exception {
    UpdateTests.testUpdateByIdWithDoubles(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithFloats() throws Exception {
    UpdateTests.testUpdateByIdWithFloats(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithSubQuery() throws Exception {
    UpdateTests.testUpdateByIdWithSubQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithSubQueryWithFloat() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueryWithFloat(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithSubQueryAndLiteralWithFloats() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueryAndLiteralWithFloats(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithSubQueryWithDecimal() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueryWithDecimal(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithScalarSubQuery() throws Exception {
    UpdateTests.testUpdateByIdWithScalarSubQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateByIdWithSubQueries() throws Exception {
    UpdateTests.testUpdateByIdWithSubQueries(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateALLWithOneUnusedSourceTable() throws Exception {
    UpdateTests.testUpdateALLWithOneUnusedSourceTable(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithOneSourceTableFullPath() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableFullPath(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithViewAsSourceTable() throws Exception {
    UpdateTests.testUpdateWithViewAsSourceTable(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithOneSourceTableNoAlias() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableNoAlias(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithOneSourceTableUseAlias() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableUseAlias(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithOneSourceTableSubQuery() throws Exception {
    UpdateTests.testUpdateWithOneSourceTableSubQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithTwoSourceTables() throws Exception {
    UpdateTests.testUpdateWithTwoSourceTables(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithTwoSourceTableOneSourceQuery() throws Exception {
    UpdateTests.testUpdateWithTwoSourceTableOneSourceQuery(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithDupsInSource() throws Exception {
    UpdateTests.testUpdateWithDupsInSource(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithUnrelatedConditionToSourceTableNoCondition() throws Exception {
    UpdateTests.testUpdateWithUnrelatedConditionToSourceTableNoCondition(
        allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithUnrelatedConditionToSourceTable() throws Exception {
    UpdateTests.testUpdateWithUnrelatedConditionToSourceTable(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithSchemaNotMatch() throws Exception {
    UpdateTests.testUpdateWithSchemaNotMatch(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithImplicitTypeCasting1() throws Exception {
    UpdateTests.testUpdateWithImplicitTypeCasting1(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithImplicitTypeCasting2() throws Exception {
    UpdateTests.testUpdateWithImplicitTypeCasting2(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithWrongContextWithPathTable() throws Exception {
    UpdateTests.testUpdateWithWrongContextWithPathTable(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithContextWithPathTable() throws Exception {
    UpdateTests.testUpdateWithContextWithPathTable(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithPartitionTransformation() throws Exception {
    UpdateTests.testUpdateWithPartitionTransformation(allocator, SOURCE, dmlWriteMode);
  }

  @Test
  public void testUpdateWithStockIcebergTable() throws Exception {
    UpdateTests.testUpdateWithStockIcebergTable(allocator, SOURCE, dmlWriteMode);
  }
}
