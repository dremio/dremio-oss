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
package com.dremio.exec.store.excel;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.util.TestTools;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;

public class TestXlsxExcelFormatPlugin extends TestExcelFormatPluginBase {

  private static ExcelTestHelper helper;

  @BeforeClass
  public static void before() throws Exception {
    helper = new ExcelTestHelper(getDfsTestTmpSchemaLocation(), false);
  }

  @AfterClass
  public static void after() {
    helper.close();
  }

  @Override
  ExcelTestHelper getHelper() {
    return helper;
  }

  @Test
  public void testEmptyXlsx() throws Exception {
    final String filePath = TestTools.getWorkingPath() + "/src/test/resources/excel/empty.xlsx";
    final String query = String.format("SELECT * FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => true, xls => false))", filePath);

    testAndExpectUserException(query, ErrorType.DATA_READ, "Selected table has no columns.");
  }

  @Test
  public void testEmpty2Xls() throws Exception {
    final String filePath = getExcelDir() + "empty2.xlsx";
    final String query = String.format("SELECT * FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => true, xls => false))", filePath);

    // There are actual columns in this table, but they contain no value. Hence, show the columns with nulls.
    test(query);
  }

  @Test
  public void testInlineString() throws Exception {
    final String filePath = getExcelDir() + "InlineString.xlsx";
    final String query = String.format("SELECT * FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    // This will fail if the inline string column is ignored, as no columns will be detected.
    test(query);
  }

  @Test
  public void testCountStarQuery() throws Exception {
    String filePath = getExcelDir() + "simple.xlsx";
    String query = String.format("SELECT COUNT(*) FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    assertThat(getResultString(testSqlWithResults(query), ""), containsString("9"));
  }

  @Test
  public void testCountStarQueryOnEmptySheet() throws Exception {
    String filePath = getExcelDir() + "empty.xlsx";
    String query = String.format("SELECT COUNT(*) FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    testAndExpectUserException(query, ErrorType.DATA_READ, "Selected table has no columns");
  }

  @Test
  public void testCountOneQuery() throws Exception {
    String filePath = getExcelDir() + "simple.xlsx";
    String query = String.format("SELECT COUNT(1) FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    assertThat(getResultString(testSqlWithResults(query), ""), containsString("9"));
  }

  @Test
  public void testCountStarAndAvgQuery() throws Exception {
    String filePath = getExcelDir() + "simple.xlsx";
    String query = String.format("SELECT COUNT(*),AVG(\"Age\") FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    assertThat(getResultString(testSqlWithResults(query), "|"), containsString("9|"));
  }

  @Test
  public void testCountOneAndAvgQuery() throws Exception {
    String filePath = getExcelDir() + "simple.xlsx";
    String query = String.format("SELECT COUNT(1),AVG(\"Age\") FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    assertThat(getResultString(testSqlWithResults(query), "|"), containsString("9|"));
  }

  @Test
  public void testCountColumnQuery() throws Exception {
    String filePath = getExcelDir() + "simple.xlsx";
    String query = String.format("SELECT COUNT(\"Age\") FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    assertThat(getResultString(testSqlWithResults(query), ""), containsString("9"));
  }

  @Test
  public void testCountStarQueryXls() throws Exception {
    String filePath = getExcelDir() + "simple.xls";
    String query = String.format("SELECT COUNT(*) FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => true))", filePath);

    assertThat(getResultString(testSqlWithResults(query), ""), containsString("9"));
  }

  @Test
  public void testCountOneQueryXls() throws Exception {
    String filePath = getExcelDir() + "simple.xls";
    String query = String.format("SELECT COUNT(1) FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => true))", filePath);

    assertThat(getResultString(testSqlWithResults(query), ""), containsString("9"));
  }

  @Test
  public void testCountStarQueryOnEmptySheetXls() throws Exception {
    String filePath = getExcelDir() + "empty.xls";
    String query = String.format("SELECT COUNT(*) FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => true))", filePath);

    testAndExpectUserException(query, ErrorType.DATA_READ, "Selected table has no columns");
  }

  @Test
  public void testCountStarAndAvgQueryXls() throws Exception {
    String filePath = getExcelDir() + "simple.xls";
    String query = String.format("SELECT COUNT(*),AVG(\"Age\") FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => true))", filePath);

    assertThat(getResultString(testSqlWithResults(query), "|"), containsString("9|"));
  }

  @Test
  public void testCountOneAndAvgQueryXls() throws Exception {
    String filePath = getExcelDir() + "simple.xls";
    String query = String.format("SELECT COUNT(1),AVG(\"Age\") FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => true))", filePath);

    assertThat(getResultString(testSqlWithResults(query), "|"), containsString("9|"));
  }

  @Test
  public void testCountColumnQueryXls() throws Exception {
    String filePath = getExcelDir() + "simple.xls";
    String query = String.format("SELECT COUNT(\"Age\") FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => true))", filePath);

    assertThat(getResultString(testSqlWithResults(query), ""), containsString("9"));
  }

  @Test
  public void testHeaderOnly() throws Exception {
    final String filePath = getExcelDir() + "header_only.xlsx";
    final String query = String.format("SELECT * FROM TABLE(dfs.\"%s\" (type => 'excel', extractHeader => true, hasMergedCells => false, xls => false))", filePath);

    testAndExpectUserException(query, ErrorType.DATA_READ, "Selected table has no columns.");
  }

  @Test
  public void testProjectAll1() throws Exception {
    getHelper().test(testBuilder(), "sheet 1", true,  true);
  }

  @Test
  public void testProjectAll2() throws Exception {
    getHelper().test(testBuilder(), "sheet 1", true,  false);
  }

  @Test
  public void testProjectPushdown1() throws Exception {
    getHelper().testProjectPushdown1(testBuilder(), "sheet 1", true,  true);
  }

  @Test
  public void testProjectPushdown2() throws Exception {
    getHelper().testProjectPushdown1(testBuilder(), "sheet 1", true,  false);
  }

  @Test
  public void testProjectPushdown3() throws Exception {
    getHelper().testProjectPushdown2(testBuilder(), "sheet 1", true,  true);
  }

  @Test
  public void testProjectPushdown4() throws Exception {
    getHelper().testProjectPushdown2(testBuilder(), "sheet 1", true,  false);
  }

  @Test
  public void testProjectPushdown5() throws Exception {
    getHelper().testProjectPushdown3(testBuilder(), "sheet 1", true,  true);
  }

  @Test
  public void testProjectPushdown6() throws Exception {
    getHelper().testProjectPushdown3(testBuilder(), "sheet 1", true,  false);
  }
}
