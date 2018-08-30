/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;

@Ignore("Move these tests to function regression")
public class TestCompareExcelAndTextPlugins extends BaseTestQuery {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(5000, TimeUnit.SECONDS);

  private static final String E_SIMPLE = String.format(
      "SELECT * FROM " +
          "TABLE(%s (" +
              "type => 'excel', " +
              "sheet => 'Sheet1', " +
              "extractHeader => true, " +
              "hasMergedCells => true)" +
          ") ",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.xlsx\"");

  private static final String T_SIMPLE = String.format(
      "SELECT * FROM " +
          "TABLE(%s (" +
              "type => 'text', " +
              "fieldDelimiter => ',', " +
              "extractHeader => true)" +
          ") ",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.csv\"");

  private static final String E_GROUPBY = String.format(
      "SELECT \"Number\", count(*) AS cnt FROM " +
          "TABLE(%s (" +
              "type => 'excel', " +
              "sheet => 'Sheet1', " +
              "extractHeader => true, " +
              "hasMergedCells => true)" +
          ") " +
          "GROUP BY \"Number\"",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.xlsx\"");

  private static final String T_GROUPBY = String.format(
      "SELECT \"Number\", count(*) AS cnt FROM " +
          "TABLE(%s (" +
              "type => 'text', " +
              "fieldDelimiter => ',', " +
              "extractHeader => true)" +
          ") " +
          "GROUP BY \"Number\"",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.csv\"");

  private static final String E_ORDERBY = String.format(
      "SELECT * FROM " +
          "TABLE(%s (" +
              "type => 'excel', " +
              "sheet => 'Sheet1', " +
              "extractHeader => true, " +
              "hasMergedCells => true)" +
          ") ORDER BY \"Number\" ",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.xlsx\"");

  private static final String T_ORDERBY = String.format(
      "SELECT * FROM " +
          "TABLE(%s (" +
              "type => 'text', " +
              "fieldDelimiter => ',', " +
              "extractHeader => true)" +
          ") ORDER BY \"Number\" ",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.csv\"");

  private static final String E_JOIN = String.format(
      "SELECT * FROM " +
          "TABLE(%s (" +
              "type => 'excel', " +
              "sheet => 'Sheet1', " +
              "extractHeader => true, " +
              "hasMergedCells => true)" +
          ") e70k " +
          "JOIN " +
          "TABLE(%s (" +
              "type => 'excel', " +
              "sheet => 'Sheet1', " +
              "extractHeader => true, " +
              "hasMergedCells => true)" +
          ") e13k " +
          "ON e70k.\"Number\" = e13k.\"Number\" " +
          "LIMIT 10",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.xlsx\"",
      "dfs.\"/Users/venki/test_data/excel/simple_15k.xlsx\"");

  private static final String T_JOIN = String.format(
      "SELECT * FROM " +
          "TABLE(%s (" +
              "type => 'text', " +
              "fieldDelimiter => ',', " +
              "extractHeader => true)" +
          ") e70k " +
          "JOIN " +
          "TABLE(%s (" +
              "type => 'text', " +
              "fieldDelimiter => ',', " +
              "extractHeader => true)" +
          ") e13k " +
          "ON e70k.\"Number\" = e13k.\"Number\" " +
          "LIMIT 10",
      "dfs.\"/Users/venki/test_data/excel/simple_70k.csv\"",
      "dfs.\"/Users/venki/test_data/excel/simple_15k.csv\"");

  private static final String E_WITH_PICTURE_CHART = String.format(
      "SELECT * FROM " +
          "TABLE(%s (" +
          "type => 'excel', " +
          "sheet => 'Sheet1', " +
          "extractHeader => false, " +
          "hasMergedCells => false)" +
          ") ",
      "dfs.\"/Users/venki/test_data/excel/with-pic-chart.xlsx\"");

  @Test
  public void simpleExcel() throws Exception {
    test(E_SIMPLE);
  }

  @Test
  public void simpleText() throws Exception {
    test(T_SIMPLE);
  }

  @Test
  public void simpleCompareCountExcelText() throws Exception {
    testBuilder()
        .sqlQuery(countQuery(E_SIMPLE))
        .unOrdered()
        .sqlBaselineQuery(countQuery(T_SIMPLE))
        .go();
  }

  @Test
  public void groupByExcel() throws Exception {
    test(E_GROUPBY);
  }

  @Test
  public void groupByText() throws Exception {
    test(T_GROUPBY);
  }

  @Test
  public void groupByCompareCountExcelText() throws Exception {
    testBuilder()
        .sqlQuery(countQuery(E_GROUPBY))
        .unOrdered()
        .sqlBaselineQuery(countQuery(T_GROUPBY))
        .go();
  }

  @Test
  public void orderByExcel() throws Exception {
    test(E_ORDERBY);
  }

  @Test
  public void orderByText() throws Exception {
    test(T_ORDERBY);
  }

  @Test
  public void orderByCompareCountExcelText() throws Exception {
    testBuilder()
        .sqlQuery(countQuery(E_ORDERBY))
        .unOrdered()
        .sqlBaselineQuery(countQuery(T_ORDERBY))
        .go();
  }

  @Test
  public void joinExcel() throws Exception {
    test(E_JOIN);
  }

  @Test
  public void joinText() throws Exception {
    test(T_JOIN);
  }

  @Test
  public void joinCompareCountExcelText() throws Exception {
    testBuilder()
        .sqlQuery(countQuery(E_JOIN))
        .unOrdered()
        .sqlBaselineQuery(countQuery(T_JOIN))
        .go();
  }

  @Test
  public void readingSheetWithPictureAndChart() throws Exception {
    test(E_WITH_PICTURE_CHART);
  }

  private static String countQuery(final String query) {
    return String.format("SELECT count(\"Number\") FROM (%s)", query);
  }
}
