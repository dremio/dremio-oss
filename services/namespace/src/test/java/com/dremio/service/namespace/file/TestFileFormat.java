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
package com.dremio.service.namespace.file;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.dremio.service.namespace.file.proto.AvroFileConfig;
import com.dremio.service.namespace.file.proto.ExcelFileConfig;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.file.proto.XlsFileConfig;

/**
 * FileFormat tests
 */
public class TestFileFormat {

  private static void assertContains(String expectedContains, String string) {
    assertTrue(string + " should contain " + expectedContains, string.contains(expectedContains));
  }

  @Test
  public void testDefaultTextFileFormatOptions() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'text'", tableOptions);
    assertContains("fieldDelimiter => ','", tableOptions);
    assertContains("comment => '#'", tableOptions);
    assertContains("\"escape\" => '\"'", tableOptions);
    assertContains("quote => '\"'", tableOptions);
    assertContains("lineDelimiter => '\r\n'", tableOptions);
    assertContains("extractHeader => false", tableOptions);
    assertContains("skipFirstLine => false", tableOptions);
    assertContains("autoGenerateColumnNames => true", tableOptions);
  }

  @Test
  public void testLineDelimiterTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setLineDelimiter("\t");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("lineDelimiter => '\t'", tableOptions);
  }

  @Test
  public void testLineDelimiterTextFileWithSingleQuote() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setLineDelimiter("'a");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("lineDelimiter => '''a'", tableOptions);
  }

  @Test
  public void testCommentTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setComment("$");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("comment => '$'", tableOptions);
  }

  @Test
  public void testEscapeTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setEscape("\\");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("\"escape\" => '\\", tableOptions);
  }

  @Test
  public void testQuoteTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setQuote("\"");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("quote => '\"'", tableOptions);
  }

  @Test
  public void testSingleQuoteTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setQuote("'");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("quote => ''''", tableOptions);
  }

  @Test
  public void testFieldDelimiterTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setFieldDelimiter("@");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("fieldDelimiter => '@'", tableOptions);
  }

  @Test
  public void testExtractHeaderTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setExtractHeader(true);
    String tableOptions = fileFormat.toTableOptions();
    assertContains("extractHeader => true", tableOptions);
  }

  @Test
  public void testsetSkipFirstLineTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setSkipFirstLine(true);
    String tableOptions = fileFormat.toTableOptions();
    assertContains("skipFirstLine => true", tableOptions);
  }

  @Test
  public void testAutoGenerateColumnNamesTextFile() throws Exception {
    TextFileConfig fileFormat = new TextFileConfig();
    fileFormat.setAutoGenerateColumnNames(false);
    String tableOptions = fileFormat.toTableOptions();
    assertContains("autoGenerateColumnNames => false", tableOptions);
  }


  @Test
  public void testDefaultJsonFileFormatOptions() throws Exception {
    JsonFileConfig fileFormat = new JsonFileConfig();
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'json'", tableOptions);
  }

  @Test
  public void testDefaultParquetFileFormatOptions() throws Exception {
    ParquetFileConfig fileFormat = new ParquetFileConfig();
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'parquet'", tableOptions);
  }

  @Test
  public void testDefaultAvroFileFormatOptions() throws Exception {
    AvroFileConfig fileFormat = new AvroFileConfig();
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'avro'", tableOptions);
  }

  @Test
  public void testDefaultExcelFileFormatOptions() throws Exception {
    ExcelFileConfig fileFormat = new ExcelFileConfig();
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("extractHeader => false", tableOptions);
    assertContains("hasMergedCells => false", tableOptions);
    assertContains("xls => false", tableOptions);
  }

  @Test
  public void testDefaultXlsFileFormatOptions() throws Exception {
    XlsFileConfig fileFormat = new XlsFileConfig();
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("extractHeader => false", tableOptions);
    assertContains("hasMergedCells => false", tableOptions);
    assertContains("xls => true", tableOptions);
  }

  @Test
  public void testSheetExcelFile() throws Exception {
    ExcelFileConfig fileFormat = new ExcelFileConfig();
    fileFormat.setSheetName("foo");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("xls => false", tableOptions);
    assertContains("sheet => 'foo'", tableOptions);
  }

  @Test
  public void testSheetExcelFileWithSingleQuote() throws Exception {
    ExcelFileConfig fileFormat = new ExcelFileConfig();
    fileFormat.setSheetName("fo'o");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("xls => false", tableOptions);
    assertContains("sheet => 'fo''o'", tableOptions);
  }

  @Test
  public void testSheetXlsFile() throws Exception {
    XlsFileConfig fileFormat = new XlsFileConfig();
    fileFormat.setSheetName("foo");
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("xls => true", tableOptions);
    assertContains("sheet => 'foo'", tableOptions);
  }

  @Test
  public void testExtractHeaderExcelFile() throws Exception {
    ExcelFileConfig fileFormat = new ExcelFileConfig();
    fileFormat.setExtractHeader(true);
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("xls => false", tableOptions);
    assertContains("extractHeader => true", tableOptions);
  }

  @Test
  public void testHasMergedCellsExcelFile() throws Exception {
    ExcelFileConfig fileFormat = new ExcelFileConfig();
    fileFormat.setHasMergedCells(true);
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("xls => false", tableOptions);
    assertContains("hasMergedCells => true", tableOptions);
  }

  @Test
  public void testExtractHeaderXlsFile() throws Exception {
    XlsFileConfig fileFormat = new XlsFileConfig();
    fileFormat.setExtractHeader(true);
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("xls => true", tableOptions);
    assertContains("extractHeader => true", tableOptions);
  }

  @Test
  public void testHasMergedCellsXlsFile() throws Exception {
    XlsFileConfig fileFormat = new XlsFileConfig();
    fileFormat.setHasMergedCells(true);
    String tableOptions = fileFormat.toTableOptions();
    assertContains("type => 'excel'", tableOptions);
    assertContains("xls => true", tableOptions);
    assertContains("hasMergedCells => true", tableOptions);
  }
}
