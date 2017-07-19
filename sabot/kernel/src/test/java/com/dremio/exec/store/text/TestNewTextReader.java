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
package com.dremio.exec.store.text;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.easy.text.compliant.CompliantTextRecordReader;

public class TestNewTextReader extends BaseTestQuery {

  @Test
  public void fieldDelimiterWithinQuotes() throws Exception {
    testBuilder()
        .sqlQuery("select columns[1] as col1 from cp.`textinput/input1.csv`")
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("foo,bar")
        .go();
  }

  @Ignore ("Not needed any more. (DRILL-3178)")
  @Test
  public void ensureFailureOnNewLineDelimiterWithinQuotes() {
    try {
      test("select columns[1] as col1 from cp.`textinput/input2.csv`");
      fail("Expected exception not thrown.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Cannot use newline character within quoted string"));
    }
  }

  @Test
  public void ensureColumnNameDisplayedinError() throws Exception {
    final String COL_NAME = "col1";

    try {
      test("select max(columns[1]) as %s from cp.`textinput/input1.csv` where %s is not null", COL_NAME, COL_NAME);
      fail("Query should have failed");
    } catch(UserRemoteException ex) {
      assertEquals(ErrorType.VALIDATION, ex.getErrorType());
      assertTrue("Error message should contain " + COL_NAME, ex.getMessage().contains(COL_NAME));
    }
  }

  @Test // see DRILL-3718
  public void testTabSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuote.tsv").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
        "from dfs_test.`%s` ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .build()
        .run();
  }

  @Test // see DRILL-3718
  public void testSpaceSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuote.ssv").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
        "from dfs_test.`%s` ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .build()
        .run();
  }

  @Test // see DRILL-3718
  public void testPipSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuote.tbl").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
            "from dfs_test.`%s` ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .build()
        .run();
  }

  @Test
  public void testValidateEmptyColumnNames() throws Exception {
    assertNull(null, CompliantTextRecordReader.validateColumnNames(null));
    assertEquals(0, CompliantTextRecordReader.validateColumnNames(new String[0]).length);
  }

  @Test
  public void testValidateColumnNamesSimple() throws Exception {
    String [] input = new String[] {"a", "b", "c"};
    String [] expected = new String[] {"a", "b", "c"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test
  public void testValidateColumnNamesDuplicate() throws Exception {
    String [] input = new String[] {"a", "b", "a", "b", "a", "a", "b", "c"};
    String [] expected = new String[] {"a", "b", "a0", "b0", "a1", "a2", "b1", "c"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test
  public void testValidateColumnNamesFillEmpty() throws Exception {
    String [] input = new String[] {"", "col1", "col2", "", "col3", ""};
    String [] expected = new String[] {"A", "col1", "col2", "D", "col3", "F"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test
  public void testValidateColumnNamesFillEmptyDuplicate() throws Exception {
    String [] input = new String[]    {"A", "", "", "B", "A", "B", "A", "",  "A", "B", "C", ""};
    String [] expected = new String[] {"A", "B", "C", "B0", "A0", "B1", "A1", "H", "A2", "B2", "C0", "L"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test // see DRILL-3718
  public void testCrLfSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuotedCrLf.tbl").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
        "from dfs_test.`%s` ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a\n1", "a", "a")
        .baselineValues("a", "a\n2", "a")
        .baselineValues("a", "a", "a\n3")
        .build()
        .run();
  }
}
