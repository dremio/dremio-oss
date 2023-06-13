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
package com.dremio.exec.store.easy.text.compliant;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.service.namespace.file.proto.TextFileConfig;

@RunWith(Parameterized.class)
public class TestTextReaderResults extends TestTextReaderHelper {

  public TestTextReaderResults(TextFileConfig fileFormat, String[][] expected, String testFileName) {
    super(fileFormat, expected, testFileName);
  }

  @Parameterized.Parameters(name = "{index}: test file: {2}, Table Options: {0} ")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
          new TextFileConfig().setLineDelimiter("\n").setTrimHeader(true),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1c1", "r1c2", "r1c3"},
            {"r2c1", "r2c2", "r2c3"}
          },
          "trim_header.csv"
        },
        {
          new TextFileConfig().setQuote("'").setLineDelimiter("\n"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1c1", "r1c2", "r1c3"},
            {"r2c1", "r2c2", "r2c3"}
          },
          "custom_quote.csv"
        },
        {
          new TextFileConfig().setExtractHeader(true).setComment("!").setLineDelimiter("\n"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1c1", "r1c2", "r1c3"},
            {"r2c1", "r2c2", "r2c3"}
          },
          "custom_comment.csv"
        },
        {
          new TextFileConfig().setFieldDelimiter(",$").setLineDelimiter("\n"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1c1", "r1c2", "r1c3"},
            {"r2c1", "r2c2", "r2c3"}
          },
          "multi_char_field_delimiter.txt"
        },
        {
          new TextFileConfig().setLineDelimiter("\n").setFieldDelimiter("¦"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1c1", "r1c2", "r1c3"},
            {"r2c1", "r2c2", "r2c3"}
          },
          "broken_pipe.txt"
        },
        {
          new TextFileConfig().setEscape("'").setLineDelimiter("\n"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1\"c1", "r1c2", "r1\"c3"},
            {"r2c1", "r2c2\"", "r2c3"}
          },
          "unescaped_quote.csv"
        },
        {
          new TextFileConfig().setLineDelimiter("$"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1c1", "r1c2", "r1c3"},
            {"r2c1", "r2c2", "r2c3"}
          },
          "custom_line_delimiter.csv"
        },
        {
          new TextFileConfig().setLineDelimiter("\n"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"\"r1\"c1\"", "r1c2", "\"r1\"c3\""},
            {"r2c1", "\"r2c2\"", "r2c3"}
          },
          "quote_escape.csv"
        },
        {
          new TextFileConfig().setEscape("\\").setLineDelimiter("\n"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"\"r1\"\\c1\"", "r1c2", "\"r1c3\""},
            {"r2c1", "\"r2c2\"", "r2c3"}
          },
          "custom_quote_escape.csv"
        },
        {
          // Failure to load
          new TextFileConfig().setEscape("\\").setLineDelimiter("\n"),
          new String[][]{
            {"c1", "c2", "c3"},
            {"r1c1", "This is value field value with an \"embedded\" quoted word using backslash-quote", "r1c3"},
            {"r2c1", "This is value field value with an \"embedded\" quoted word using double-double-quote", "r2c3"}
          },
          "double_double_quote.csv"
        },
        {
          new TextFileConfig().setLineDelimiter("$"),
          new String[][] {
            {"c1","c2","c3"},
            {"r1c1$","r1c2","r1c3$"},
            {"r2c1","r2c2$","r2c3"}
          },
          "custom_ld_inside_quoted.csv"
        },
      }
    );
  }

  @Test
  public void testSelectQueryResults() {
    try {
      testTableOptionsSelectAll();
      testTableOptionsSelectCol();
    } catch (Throwable t) {
      fail("Above calls are not expected to throw");
    }
  }
}
