/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.File;
import java.io.FileOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.util.LocaleUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.joda.time.LocalDateTime;

import com.dremio.TestBuilder;

/**
 * Helper class that generates test data for XLS/XLSX unit tests
 */
class ExcelTestHelper {

  private static final LocalDateTime EXP_DATE_1 =
          new LocalDateTime(1983, 5, 18, 4, 0, 0, 0);
  private static final LocalDateTime EXP_DATE_2 =
          new LocalDateTime(2013, 07, 05, 5, 0, 1, 0);

  private final boolean xls;
  private final String testFilePath;

  ExcelTestHelper(final String parent, boolean generateXls) throws Exception {
    this.xls = generateXls;

    // Create a test Excel sheet with all types of supported data
    Workbook wb = generateXls ? new HSSFWorkbook() : new XSSFWorkbook();

    CreationHelper creationHelper = wb.getCreationHelper();
    DataFormat dataFormat = creationHelper.createDataFormat();
    short fmt = dataFormat.getFormat("yyyy-mm-dd hh:mm:ss");
    CellStyle style = wb.createCellStyle();
    style.setDataFormat(fmt);

    Sheet sheetWithHeader = wb.createSheet("Sheet 1");

    // Create header row
    Row headerRow = sheetWithHeader.createRow((short) 0);
    headerRow.createCell(0).setCellValue("Number");
    headerRow.createCell(1).setCellValue("String1");
    headerRow.createCell(2).setCellValue("String2");
    headerRow.createCell(3).setCellValue("MyTime");
    headerRow.createCell(4).setCellValue("Formula");
    headerRow.createCell(5).setCellValue("Boolean");
    headerRow.createCell(6).setCellValue("Error");
    generateSheetData(sheetWithHeader, style, (short)1);

    Sheet sheetWithoutHeader = wb.createSheet("Sheet 2");
    generateSheetData(sheetWithoutHeader, style, (short)0);

    testFilePath = new File(parent, "excelTestFile").getPath();

    // Write the output to a file
    FileOutputStream fileOut = new FileOutputStream(testFilePath);
    wb.write(fileOut);
    fileOut.close();
  }

  private static void generateSheetData(final Sheet sheet, final CellStyle style, short startingRow) {
    int currentRow = startingRow;
    // Create first row values
    Row row1 = sheet.createRow(currentRow++);
    row1.createCell(0).setCellValue(1.0);
    row1.createCell(1).setCellValue("One");
    row1.createCell(2).setCellValue("One");
    Cell c13 = row1.createCell(3);
    c13.setCellValue(LocaleUtil.getLocaleCalendar(1983, 04/*zero based*/, 18, 4, 0, 0));
    c13.setCellStyle(style);
    Cell c14 = row1.createCell(4);
    c14.setCellFormula("A2+1");
    // For formulas we read pre-computed values. Editors set the precomputed value by default. We need to add it here
    // explicitly as the library doesn't pre compute the formula value.
    c14.setCellValue(2.0d);
    row1.createCell(5).setCellValue(true);
    row1.createCell(6).setCellFormula("B2*20");
    row1.createCell(6).setCellValue("#ERROR");

    // Create second row values
    Row row2 = sheet.createRow(currentRow++);
    row2.createCell(0).setCellValue(2.0);
    row2.createCell(1).setCellValue("Two");
    row2.createCell(2).setCellValue("Two");
    Cell c23 = row2.createCell(3);
    c23.setCellValue(LocaleUtil.getLocaleCalendar(2013, 06/*zero based*/, 05, 5, 0, 1));
    c23.setCellStyle(style);
    Cell c24 = row2.createCell(4);
    c24.setCellFormula("A3+1");
    c24.setCellValue(3.0d);
    row2.createCell(5).setCellValue(false);
    row2.createCell(6).setCellFormula("B3*20");
    row2.createCell(6).setCellValue("#ERROR");

    // Create third row values
    Row row3 = sheet.createRow(currentRow++);
    row3.createCell(0).setCellValue(3.0);
    row3.createCell(1).setCellValue("Three and Three");
    row3.createCell(5).setCellValue(false);

    // Create fourth row values
    Row row4 = sheet.createRow(currentRow++);
    row4.createCell(0).setCellValue(4.0);
    row4.createCell(1).setCellValue("Four and Four, Five and Five");

    // Create fifth row values
    Row row5 = sheet.createRow(currentRow++);
    row5.createCell(0).setCellValue(5.0);

    sheet.addMergedRegion(new CellRangeAddress(startingRow + 2, startingRow + 2, 1, 2));
    sheet.addMergedRegion(new CellRangeAddress(startingRow + 2, startingRow + 4, 5, 5));
    sheet.addMergedRegion(new CellRangeAddress(startingRow + 3, startingRow + 4, 1, 2));
  }

  void test(final TestBuilder testBuilder, String sheetName, boolean header, boolean mergedCellExpansion) throws Exception {
    final StringBuilder builder = new StringBuilder()
            .append("SELECT * FROM ")
            .append("TABLE(dfs.`").append(testFilePath).append("` (")
            .append("type => 'excel'");

    if (sheetName != null) {
      builder.append(", sheet => '").append(sheetName).append("'");
    }

    if (header) {
      builder.append(", extractHeader => true");
    }

    if (mergedCellExpansion) {
      builder.append(", hasMergedCells => true");
    }

    if (xls) {
      builder.append(", xls => true");
    }

    builder.append("))");

    testBuilder
              .sqlQuery(builder.toString())
              .unOrdered();

    if (header) {
      testBuilder.baselineColumns("Number", "String1", "String2", "MyTime", "Formula", "Boolean", "Error");
    } else {
      testBuilder.baselineColumns("A", "B", "C", "D", "E", "F", "G");
    }

    if (mergedCellExpansion) {
      testBuilder
              .baselineValues(1.0d, "One", "One", EXP_DATE_1, 2.0d, true, "#ERROR")
              .baselineValues(2.0d, "Two", "Two", EXP_DATE_2, 3.0d, false, "#ERROR")
              .baselineValues(3.0d, "Three and Three", "Three and Three", null, null, false, null)
              .baselineValues(4.0d, "Four and Four, Five and Five", "Four and Four, Five and Five", null, null, false, null)
              .baselineValues(5.0d, "Four and Four, Five and Five", "Four and Four, Five and Five", null, null, false, null);
    } else {
      testBuilder
              .baselineValues(1.0d, "One", "One", EXP_DATE_1, 2.0d, true, "#ERROR")
              .baselineValues(2.0d, "Two", "Two", EXP_DATE_2, 3.0d, false, "#ERROR")
              .baselineValues(3.0d, "Three and Three", null, null, null, false, null)
              .baselineValues(4.0d, "Four and Four, Five and Five", null, null, null, null, null)
              .baselineValues(5.0d, null, null, null, null, null, null);
    }

    testBuilder.go();
  }

  void testProjectPushdown1(final TestBuilder testBuilder, String sheetName, boolean header, boolean mergedCellExpansion) throws Exception {
    final StringBuilder builder = new StringBuilder()
      .append("SELECT Number, MyTime FROM ")
      .append("TABLE(dfs.`").append(testFilePath).append("` (")
      .append("type => 'excel'");

    if (sheetName != null) {
      builder.append(", sheet => '").append(sheetName).append("'");
    }

    if (header) {
      builder.append(", extractHeader => true");
    }

    if (mergedCellExpansion) {
      builder.append(", hasMergedCells => true");
    }

    if (xls) {
      builder.append(", xls => true");
    }

    builder.append("))");

    testBuilder
      .sqlQuery(builder.toString())
      .unOrdered();

    if (header) {
      testBuilder.baselineColumns("Number", "MyTime");
    } else {
      testBuilder.baselineColumns("A", "D");
    }

    /* the output for NUMBER, DateTime should be same regardless of whether we use mergedCellExpansion or not */
    testBuilder
        .baselineValues(1.0d, EXP_DATE_1)
        .baselineValues(2.0d, EXP_DATE_2)
        .baselineValues(3.0d, null)
        .baselineValues(4.0d, null)
        .baselineValues(5.0d, null);

    testBuilder.go();
  }

  void testProjectPushdown2(final TestBuilder testBuilder, String sheetName, boolean header, boolean mergedCellExpansion) throws Exception {
    final StringBuilder builder = new StringBuilder()
      .append("SELECT  Number, String2 FROM ")
      .append("TABLE(dfs.`").append(testFilePath).append("` (")
      .append("type => 'excel'");

    if (sheetName != null) {
      builder.append(", sheet => '").append(sheetName).append("'");
    }

    if (header) {
      builder.append(", extractHeader => true");
    }

    if (mergedCellExpansion) {
      builder.append(", hasMergedCells => true");
    }

    if (xls) {
      builder.append(", xls => true");
    }

    builder.append("))");

    testBuilder
      .sqlQuery(builder.toString())
      .unOrdered();

    if (header) {
      testBuilder.baselineColumns("Number", "String2");
    } else {
      testBuilder.baselineColumns("A", "C");
    }

    /* STRING2 is the projected column and has merged cell values from STRING1 which is
     * a non-projected column
     */
    if(mergedCellExpansion) {
      testBuilder
        .baselineValues(1.0d, "One")
        .baselineValues(2.0d, "Two")
        .baselineValues(3.0d, "Three and Three")
        .baselineValues(4.0d, "Four and Four, Five and Five")
        .baselineValues(5.0d, "Four and Four, Five and Five");
    }
    else {
      testBuilder
        .baselineValues(1.0d, "One")
        .baselineValues(2.0d, "Two")
        .baselineValues(3.0d, null)
        .baselineValues(4.0d, null)
        .baselineValues(5.0d, null);
    }

    testBuilder.go();
  }

  void testProjectPushdown3(final TestBuilder testBuilder, String sheetName, boolean header, boolean mergedCellExpansion) throws Exception {
    final StringBuilder builder = new StringBuilder()
      .append("SELECT  String1, String2 FROM ")
      .append("TABLE(dfs.`").append(testFilePath).append("` (")
      .append("type => 'excel'");

    if (sheetName != null) {
      builder.append(", sheet => '").append(sheetName).append("'");
    }

    if (header) {
      builder.append(", extractHeader => true");
    }

    if (mergedCellExpansion) {
      builder.append(", hasMergedCells => true");
    }

    if (xls) {
      builder.append(", xls => true");
    }

    builder.append("))");

    testBuilder
      .sqlQuery(builder.toString())
      .unOrdered();

    if (header) {
      testBuilder.baselineColumns("String1", "String2");
    } else {
      testBuilder.baselineColumns("B", "C");
    }

    /* STRING2 is the projected column and has merged cell values from STRING1 which is
     * also a projected column
     */
    if(mergedCellExpansion) {
      testBuilder
        .baselineValues("One", "One")
        .baselineValues("Two", "Two")
        .baselineValues("Three and Three", "Three and Three")
        .baselineValues("Four and Four, Five and Five", "Four and Four, Five and Five")
        .baselineValues("Four and Four, Five and Five", "Four and Four, Five and Five");
    }
    else {
      testBuilder
        .baselineValues("One", "One")
        .baselineValues("Two", "Two")
        .baselineValues("Three and Three", null)
        .baselineValues("Four and Four, Five and Five", null)
        .baselineValues(null, null);
    }

    testBuilder.go();
  }

  void close() {
    if (testFilePath != null) {
      FileUtils.deleteQuietly(new File(testFilePath));
    }
  }
}
