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
package com.dremio;

import static com.dremio.TestBuilder.listOf;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.base.Joiner;

public class TestSelectWithOption extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSelectWithOption.class);

  private File genCSVFile(String name, String... rows) throws IOException {
    File file = new File(format("target/%s_%s.csv", this.getClass().getName(), name));
    try (FileWriter fw = new FileWriter(file)) {
      for (int i = 0; i < rows.length; i++) {
        fw.append(rows[i] + "\n");
      }
    }
    return file;
  }

  private String genCSVTable(String name, String... rows) throws IOException {
    File f = genCSVFile(name, rows);
    return format("dfs.\"${WORKING_PATH}/%s\"", f.getPath());
  }

  private void testWithResult(String query, Object... expectedResult) throws Exception {
    TestBuilder builder = testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("columns");
    for (Object o : expectedResult) {
      builder = builder.baselineValues(o);
    }
    builder.build().run();
  }

  @Test
  public void columnLimitExceeded() throws Exception {
    String tableName = genCSVTable("columnLimitExceeded",
        "\"a1\",\"a2\",\"a3\",\"a4\",\"a5\",\"a6\",\"a7\",\"a8\",\"a9\",\"a0\"",
        "\"a1\",\"a2\",\"a3\",\"a4\",\"a5\",\"a6\",\"a7\",\"a8\",\"a9\",\"a0\""
    );

    String queryTemplate = "select columns from table(%s (type => 'TeXT', fieldDelimiter => '%s', extractHeader => true))";
    try (AutoCloseable closeable = setSystemOptionWithAutoReset("store.plugin.max_metadata_leaf_columns", "2")) {
      test(format(queryTemplate, tableName, ","));
      fail("query should have failed");
    } catch (UserException e) {
      assertEquals(e.getErrorType(), UserBitShared.DremioPBError.ErrorType.VALIDATION);
      assertTrue(e.getMessage().contains("exceeded the maximum number of fields of 2"));
    }
  }

  @Test
  public void testTextFieldDelimiter() throws Exception {
    String tableName = genCSVTable("testTextFieldDelimiter",
        "\"b\"|\"0\"",
        "\"b\"|\"1\"",
        "\"b\"|\"2\"");

    String queryTemplate =
        "select columns from table(%s (type => 'TeXT', fieldDelimiter => '%s'))";
    testWithResult(format(queryTemplate, tableName, ","),
        listOf("b\"|\"0"),
        listOf("b\"|\"1"),
        listOf("b\"|\"2")
      );
    testWithResult(format(queryTemplate, tableName, "|"),
        listOf("b", "0"),
        listOf("b", "1"),
        listOf("b", "2")
      );
  }

  @Test
  public void testTabFieldDelimiter() throws Exception {
    String tableName = genCSVTable("testTabFieldDelimiter",
        "1\ta",
        "2\tb");
    testWithResult(format("select columns from table(%s(type=>'TeXT', fieldDelimiter => '\t'))", tableName),
        listOf("1", "a"),
        listOf("2", "b"));
  }

  @Test
  public void testSingleTextLineDelimiter() throws Exception {
    String tableName = genCSVTable("testSingleTextLineDelimiter",
        "a|b|c");

    testWithResult(format("select columns from table(%s(type => 'TeXT', lineDelimiter => '|'))", tableName),
        listOf("a"),
        listOf("b"),
        listOf("c"));
  }

  @Test
  // '\n' is treated as standard delimiter
  // if user has indicated custom line delimiter but input file contains '\n', split will occur on both
  public void testCustomTextLineDelimiterAndNewLine() throws Exception {
    String tableName = genCSVTable("testTextLineDelimiter",
        "b|1",
        "b|2");

    testWithResult(format("select columns from table(%s(type => 'TeXT', lineDelimiter => '|'))", tableName),
        listOf("b"),
        listOf("1"),
        listOf("b"),
        listOf("2"));
  }

  @Test
  public void testTextLineDelimiterWithCarriageReturn() throws Exception {
    String tableName = genCSVTable("testTextLineDelimiterWithCarriageReturn",
        "1, a\r",
        "2, b\r");
    testWithResult(format("select columns from table(%s(type=>'TeXT', lineDelimiter => '\r\n'))", tableName),
        listOf("1, a"),
        listOf("2, b"));
  }

  @Test
  public void testMultiByteLineDelimiter() throws Exception {
    String tableName = genCSVTable("testMultiByteLineDelimiter",
        "1abc2abc3abc");
    test(format("select columns from table(%s(type=>'TeXT', lineDelimiter => 'abc'))", tableName));
    testWithResult(format("select columns from table(%s(type=>'TeXT', lineDelimiter => 'abc'))", tableName),
        listOf("1"),
        listOf("2"),
        listOf("3"),
        null);
  }

  @Test
  public void testExtendedCharDelimiters() throws Exception {
    String tableName = genCSVTable("testExtendedCharDelimiters",
      "1¦22¦333∆x¦yy¦zzz");
    testWithResult(format("select columns from table(%s(type=>'TeXT', fieldDelimiter => '¦', lineDelimiter => '∆'))", tableName),
      listOf("1", "22", "333"),
      listOf("x", "yy", "zzz"));
  }

  @Test
  public void testDataWithPartOfMultiByteLineDelimiter() throws Exception {
    String tableName = genCSVTable("testDataWithPartOfMultiByteLineDelimiter",
        "ab1abc2abc3abc");
    testWithResult(format("select columns from table(%s(type=>'TeXT', lineDelimiter => 'abc'))", tableName),
        listOf("ab1"),
        listOf("2"),
        listOf("3"),
        null);
  }

  @Test
  public void testTextQuote() throws Exception {
    String tableName = genCSVTable("testTextQuote",
        "\"b\"|\"0\"",
        "\"b\"|\"1\"",
        "\"b\"|\"2\"");

    testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', quote => '@'))", tableName),
        listOf("\"b\"", "\"0\""),
        listOf("\"b\"", "\"1\""),
        listOf("\"b\"", "\"2\"")
        );

    String quoteTableName = genCSVTable("testTextQuote2",
        "@b@|@0@",
        "@b$@c@|@1@");
    // It seems that a parameter can not be called "escape"
    testWithResult(format("select columns from table(%s(\"escape\" => '$', type => 'TeXT', fieldDelimiter => '|', quote => '@'))", quoteTableName),
        listOf("b", "0"),
        listOf("b@c", "1")
        );
  }

  @Test
  public void testTextComment() throws Exception {
      String commentTableName = genCSVTable("testTextComment",
          "b|0",
          "@ this is a comment",
          "b|1");
      testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', comment => '@'))", commentTableName),
          listOf("b", "0"),
          listOf("b", "1")
          );
  }

  @Test
  public void testTextHeader() throws Exception {
    String headerTableName = genCSVTable("testTextHeader",
        "b|a",
        "b|0",
        "b|1");
    testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', skipFirstLine => true))", headerTableName),
        listOf("b", "0"),
        listOf("b", "1")
        );

    testBuilder()
        .sqlQuery(format("select a, b from table(%s(type => 'TeXT', fieldDelimiter => '|', extractHeader => true))", headerTableName))
        .ordered()
        .baselineColumns("b", "a")
        .baselineValues("b", "0")
        .baselineValues("b", "1")
        .build().run();
  }

  @Test // DX-2584
  public void testCountWithTableOptions() throws Exception {
    String headerTableName = genCSVTable("testTextHeader",
        "b|a",
        "b |0",
        " b|1",
        "s|",
        "|s",
        "|",
        "");

    String query = format(
        "select count(*) as cnt from table(%s(type => 'TeXT', fieldDelimiter => '|', extractHeader => true))",
        headerTableName);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cnt")
        .baselineValues(6L)
        .build().run();

    query = format(
        "select count(*) as cnt from table(%s(type => 'TeXT', fieldDelimiter => '|', extractHeader => false))",
        headerTableName);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cnt")
        .baselineValues(7L)
        .build().run();

    query = format(
        "select count(*) as cnt from table(%s(type => 'TeXT', fieldDelimiter => '|', skipFirstLine => true))",
        headerTableName);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cnt")
        .baselineValues(6L)
        .build().run();

    query = format(
        "select count(*) as cnt from table(%s(type => 'TeXT', fieldDelimiter => '|', skipFirstLine => true, " +
            " extractHeader => true))",
        headerTableName);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cnt")
        .baselineValues(6L) // skipFirstLine is ignored when extractHeader is true
        .build().run();
  }

  @Test
  public void testVariationsCSV() throws Exception {
    String csvTableName = genCSVTable("testVariationsCSV",
        "a,b",
        "c|d");
    // Using the defaults in TextFormatConfig (the field delimiter is neither "," not "|")
    String[] csvQueries = {
//        format("select columns from %s ('TeXT')", csvTableName),
//        format("select columns from %s('TeXT')", csvTableName),
        format("select columns from table(%s ('TeXT'))", csvTableName),
        format("select columns from table(%s (type => 'TeXT'))", csvTableName),
//        format("select columns from %s (type => 'TeXT')", csvTableName)
    };
    for (String csvQuery : csvQueries) {
      testWithResult(csvQuery,
          listOf("a,b"),
          listOf("c|d"));
    }
    // the sabot config file binds .csv to "," delimited
//    testWithResult(format("select columns from %s", csvTableName),
//          listOf("a", "b"),
//          listOf("c|d"));
    // setting the delimiter
    testWithResult(format("select columns from table(%s (type => 'TeXT', fieldDelimiter => ','))", csvTableName),
        listOf("a", "b"),
        listOf("c|d"));
    testWithResult(format("select columns from table(%s (type => 'TeXT', fieldDelimiter => '|'))", csvTableName),
        listOf("a,b"),
        listOf("c", "d"));
  }

  @Test
  public void testVariationsJSON() throws Exception {
    String jsonTableName = genCSVTable("testVariationsJSON",
        "{\"columns\": [\"f\",\"g\"]}");
    // the extension is actually csv
    String[] jsonQueries = {
        format("select columns from table(%s ('JSON'))", jsonTableName),
        format("select columns from table(%s(type => 'JSON'))", jsonTableName),
//        format("select columns from %s ('JSON')", jsonTableName),
//        format("select columns from %s (type => 'JSON')", jsonTableName),
//        format("select columns from %s(type => 'JSON')", jsonTableName),
        // we can use named format plugin configurations too!
        format("select columns from table(%s(type => 'Named', name => 'json'))", jsonTableName),
    };
    for (String jsonQuery : jsonQueries) {
      testWithResult(jsonQuery, listOf("f","g"));
    }
  }

  @Test
  public void testUse() throws Exception {
    final String schema = getValueInFirstRecord("select current_schema", "current_schema");
    File f = genCSVFile("testUse",
        "{\"columns\": [\"f\",\"g\"]}");
    String jsonTableName = format("\"${WORKING_PATH}/%s\"", f.getPath());
    // the extension is actually csv
    test("use dfs");
    try {
      String[] jsonQueries = {
          format("select columns from table(%s ('JSON'))", jsonTableName),
          format("select columns from table(%s(type => 'JSON'))", jsonTableName),
      };
      for (String jsonQuery : jsonQueries) {
        testWithResult(jsonQuery, listOf("f","g"));
      }

      testWithResult(format("select length(columns[0]) as columns from table(%s ('JSON'))", jsonTableName), 1);
    } finally {
      //setting the schema back
      if(!schema.isEmpty()) {
        test(format("use %s", schema));
      }
    }
  }

  @Test // DX-3796
  public void testLongRows() throws Exception {
    List<String> values = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      values.add(format("%05d", i));
    }
    String row = Joiner.on(",").join(values);
    String csvTable = genCSVTable("longRows", row, row);

    test(format("select * from table(%s (type => 'Text', fieldDelimiter => '\t', autoGenerateColumnNames => false, extractHeader => true, skipFirstLine => false))", csvTable), values);
  }
}
