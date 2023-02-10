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

public class ITCopyIntoTests extends ITDmlQueryBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testFilesLimit() throws Exception {
    CopyIntoTests.testFilesLimit(allocator, SOURCE);
  }

  @Test
  public void testEmptyAsNullDefault() throws Exception {
    CopyIntoTests.testEmptyAsNullDefault(allocator, SOURCE);
  }

  @Test
  public void testCSVSingleBoolCol() throws Exception {
    CopyIntoTests.testCSVSingleBoolCol(allocator, SOURCE);
  }

  @Test
  public void testJsonWithDuplicateColumnNames() throws Exception {
    CopyIntoTests.testJsonWithDuplicateColumnNames(allocator, SOURCE);
  }

  @Test
  public void testJsonWithNullValuesAtRootLevel() throws Exception {
    CopyIntoTests.testJsonWithNullValuesAtRootLevel(allocator, SOURCE);
  }

  @Test
  public void testCSVWithDuplicateColumnNames() throws Exception {
    CopyIntoTests.testCSVWithDuplicateColumnNames(allocator, SOURCE);
  }

  @Test
  public void testCSVWithFilePatternStringType() throws Exception {
    CopyIntoTests.testCSVWithFilePatternStringType(allocator, SOURCE);
  }

  @Test
  public void testCSVWithFilePatternDoubleType() throws Exception {
    CopyIntoTests.testCSVWithFilePatternDoubleType(allocator, SOURCE);
  }

  @Test
  public void testCSVWithFilePatternFloatType() throws Exception {
    CopyIntoTests.testCSVWithFilePatternFloatType(allocator, SOURCE);
  }

  @Test
  public void testCSVWithFilePatternDecimalType() throws Exception {
    CopyIntoTests.testCSVWithFilePatternDecimalType(allocator, SOURCE);
  }

  @Test
  public void testCSVWithRegex() throws Exception {
    CopyIntoTests.testCSVWithRegex(allocator, SOURCE);
  }

  @Test
  public void testCSVWithQuote() throws Exception {
    CopyIntoTests.testCSVWithQuote(allocator, SOURCE);
  }

  @Test
  public void testCSVJSONWithSpace() throws Exception {
    CopyIntoTests.testCSVJSONWithSpace(allocator, SOURCE);
  }

  @Test
  public void testCSVWithEmpyData() throws Exception {
    CopyIntoTests.testCSVWithEmpyData(allocator, SOURCE);
  }

  @Test
  public void testCSVJSONWithNoTargetColumn() throws Exception {
    CopyIntoTests.testCSVJSONWithNoTargetColumn(allocator, SOURCE);
  }

  @Test
  public void testCSVWithDateType() throws Exception {
    CopyIntoTests.testCSVWithDateType(allocator, SOURCE);
  }

  @Test
  public void testRegexOnPath() throws Exception {
    CopyIntoTests.testRegexOnPath(allocator, SOURCE);
  }

  @Test
  public void testFilesKeywordWithCSV() throws Exception {
    CopyIntoTests.testFilesKeywordWithCSV(allocator, SOURCE);
  }
  @Test
  public void testJSONWithDateType() throws Exception {
    CopyIntoTests.testJSONWithDateType(allocator, SOURCE);
  }

  @Test
  public void testJSONWithListOfDates() throws Exception {
    CopyIntoTests.testJSONWithListOfDates(allocator, SOURCE);
  }

  @Test
  public void testCSVWithStringDelimiter() throws Exception {
    CopyIntoTests.testCSVWithStringDelimiter(allocator, SOURCE);
  }

  @Test
  public void testJSONComplexNullValues() throws Exception {
    CopyIntoTests.testJSONComplexNullValues(allocator, SOURCE);
  }

  @Test
  public void testFilesWithCSVFilesAtImmediateSource() throws Exception {
    CopyIntoTests.testFilesWithCSVFilesAtImmediateSource(allocator, SOURCE);
  }

  @Test
  public void testJSONComplex() throws Exception {
    CopyIntoTests.testJSONComplex(allocator, SOURCE);
  }

  @Test
  public void testEmptyFiles() throws Exception{
    CopyIntoTests.testEmptyFiles(allocator, SOURCE);
  }

  @Test
  public void testOneColumnOneData() throws Exception{
    CopyIntoTests.testOneColumnOneData(allocator, SOURCE);
  }

  @Test
  public void testOneColumnNoData() throws Exception{
    CopyIntoTests.testOneColumnNoData(SOURCE);
  }

  @Test
  public void testManyColumnsWhitespaceData() throws Exception{
    CopyIntoTests.testManyColumnsWhitespaceData(allocator, SOURCE);
  }

  @Test
  public void testManyColumnsEmptyData() throws Exception{
    CopyIntoTests.testManyColumnsEmptyData(allocator, SOURCE);
  }

  @Test
  public void testManyColumnsWhitespaceTabData() throws Exception{
    CopyIntoTests.testManyColumnsWhitespaceTabData(allocator, SOURCE);
  }

  @Test
  public void testNoHeaderOnlyData() throws Exception{
    CopyIntoTests.testNoHeaderOnlyData(allocator, SOURCE);
  }

  @Test
  public void testMoreValuesThanHeaders() throws Exception{
    CopyIntoTests.testMoreValuesThanHeaders(allocator, SOURCE);
  }

  @Test
  public void testMoreHeadersThanValues() throws Exception{
    CopyIntoTests.testMoreHeadersThanValues(allocator, SOURCE);
  }

  @Test
  public void testColumnsTableUppercaseFileLowerCase() throws Exception{
    CopyIntoTests.testColumnsTableUppercaseFileLowerCase(allocator, SOURCE);
  }

  @Test
  public void testColumnsTableLowercaseFileUppercase() throws Exception{
    CopyIntoTests.testColumnsTableLowercaseFileUppercase(allocator, SOURCE);
  }

  @Test
  public void testManyColumnsNoValues() throws Exception{
    CopyIntoTests.testManyColumnsNoValues(allocator, SOURCE);
  }

  @Test
  public void testNullValueAtRootLevel() throws Exception{
    CopyIntoTests.testNullValueAtRootLevel(allocator, SOURCE);
  }

  @Test
  public void testValuesInSingleQuotes() throws Exception{
    CopyIntoTests.testValuesInSingleQuotes(allocator, SOURCE);
  }


  @Test
  public void testValuesInDoubleQuotes() throws Exception{
    CopyIntoTests.testValuesInDoubleQuotes(allocator, SOURCE);
  }

  @Test
  public void testBooleanCaseSensitivity() throws Exception{
    CopyIntoTests.testBooleanCaseSensitivity(allocator, SOURCE);
  }

  @Test
  public void testHeaderEmptyString() throws Exception{
    CopyIntoTests.testHeaderEmptyString(allocator, SOURCE);
  }

  @Test
  public void testCSVWithEscapeChar() throws Exception {
    CopyIntoTests.testCSVWithEscapeChar(allocator, SOURCE);
  }

  @Test
  public void testComplexTypesToVarcharCoercion() throws Exception{
    CopyIntoTests.testComplexTypesToVarcharCoercion(SOURCE);
  }

  @Test
  public void testListOfListToListCoercion() throws Exception{
    CopyIntoTests.testListOfListToListCoercion(SOURCE);
  }

  @Test
  public void testFileExtensionCaseSensitivity() throws Exception{
    CopyIntoTests.testFileExtensionCaseSensitivity(allocator, SOURCE);
  }

  @Test
  public void testDifferentExtensions() throws Exception{
    CopyIntoTests.testDifferentExtensions(allocator, SOURCE);
  }

  @Test
  public void testComplexValuesCsv() throws Exception{
    CopyIntoTests.testComplexValuesCsv(allocator, SOURCE);
  }

  @Test
  public void testRegexCaseSensitivity() throws Exception{
    CopyIntoTests.testRegexCaseSensitivity(allocator, SOURCE);
  }

  @Test
  public void testNullIf() throws Exception{
    CopyIntoTests.testNullIf(allocator, SOURCE);
  }

  @Test
  public void testIntegerCoercion() throws Exception{
    CopyIntoTests.testIntegerCoercion(allocator, SOURCE);
  }

  @Test
  public void testNumberCoercion() throws Exception{
    CopyIntoTests.testNumberCoercion(allocator, SOURCE);
  }

  @Test
  public void testColumnNameCaseSensitivity() throws Exception{
    CopyIntoTests.testColumnNameCaseSensitivity(allocator, SOURCE);
  }

  @Test
  public void testDifferentFieldDelimitersForCsv() throws Exception{
    CopyIntoTests.testDifferentFieldDelimitersForCsv(allocator, SOURCE);
  }
}
