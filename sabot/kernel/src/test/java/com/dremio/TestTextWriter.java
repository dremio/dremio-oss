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
package com.dremio;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.ExecConstants;

/**
 *
 */
public class TestTextWriter extends PlanTestBase {
  @Before
  public void setupOptions() throws Exception {
    testNoResult("ALTER SESSION SET `%s` = true", ExecConstants.ENABLE_VERBOSE_ERRORS.getOptionName());
  }

  @After
  public void resetOptions() throws Exception {
    testNoResult("ALTER SESSION RESET ALL");
  }

  @Test
  public void testCsvWithCommas() throws Exception {
    final String valA = "a";
    final String valB = "b,2";
    final String valC = "\"c,3,3\"";
    final String valD = "d,\"4";
    final String testValues = "(values('" + valA + "'), ('" + valB + "'), ('" + valC + "'), ('" + valD + "')) as t(testvals)";
    test("create table dfs_test.commas STORE AS (type => 'text', fieldDelimiter => ',') WITH SINGLE WRITER AS select trim(testvals) as testvals from " + testValues);
    testBuilder()
      .sqlQuery("select * from table(dfs_test.commas(type => 'text', fieldDelimiter => ',', extractHeader => true, skipFirstLine => false, autoGenerateColumnNames => true, trimHeader => true))")
      .unOrdered()
      .baselineColumns("testvals")
      .baselineValues(valA)
      .baselineValues(valB)
      .baselineValues(valC)
      .baselineValues(valD)
      .build()
      .run();
  }

  @Test
  public void testCsvWithLineBreaks() throws Exception {
    final String valA = "aaa";
    final String valB = "b\nb";
    final String valC = "ccc";
    final String valD = "d\"dd";
    final String testValues = "(values('" + valA + "'), ('" + valB + "'), ('" + valC + "'), ('" + valD + "')) as t(testvals)";
    test("create table dfs_test.line_breaks STORE AS (type => 'text', fieldDelimiter => ',', lineDelimiter => '\n') WITH SINGLE WRITER AS select trim(testvals) as testvals from " + testValues);
    testBuilder()
      .sqlQuery("select * from table(dfs_test.line_breaks(type => 'text', fieldDelimiter => ',', lineDelimiter => '\n', extractHeader => true, skipFirstLine => false, autoGenerateColumnNames => true, trimHeader => true))")
      .unOrdered()
      .baselineColumns("testvals")
      .baselineValues(valA)
      .baselineValues(valB)
      .baselineValues(valC)
      .baselineValues(valD)
      .build()
      .run();
  }

  @Test
  public void testCsvWithAltLineBreaks() throws Exception {
    // Create a CSV file that's CRLF-delimited. Fields containing newlines should still be quoted
    final String valA = "aaa";
    final String valB = "b\nb";
    final String valC = "ccc";
    final String valD = "d\"dd";
    final String testValues = "(values('" + valA + "'), ('" + valB + "'), ('" + valC + "'), ('" + valD + "')) as t(testvals)";
    test("create table dfs_test.alt_line_breaks STORE AS (type => 'text', fieldDelimiter => ',', lineDelimiter => '\r\n') WITH SINGLE WRITER AS select trim(testvals) as testvals from " + testValues);
    testBuilder()
      .sqlQuery("select * from table(dfs_test.alt_line_breaks(type => 'text', fieldDelimiter => ',', lineDelimiter => '\r\n', extractHeader => true, skipFirstLine => false, autoGenerateColumnNames => true, trimHeader => true))")
      .unOrdered()
      .baselineColumns("testvals")
      .baselineValues(valA)
      .baselineValues(valB)
      .baselineValues(valC)
      .baselineValues(valD)
      .build()
      .run();
  }
}
