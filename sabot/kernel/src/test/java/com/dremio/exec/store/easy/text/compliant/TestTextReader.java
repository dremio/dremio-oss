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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.proto.UserBitShared;
import java.io.File;
import java.io.FileWriter;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Class to test exception handling for TextInput related to bad lineDelimiter and column size
 * exceeds limit
 */
public class TestTextReader extends BaseTestQuery {
  // small file
  private static String TMP_CSV_FILE_SMALL = "my_small.csv";
  // large columns size file
  private static String TMP_CSV_FILE_LARGE = "my_large.csv";
  // count star file
  private static String TMP_CSV_FILE_COUNT_STAR = "my_count_star.csv";
  // two rows file
  private static String TMP_CSV_FILE_TWO_ROWS = "my_two_rows.csv";
  // Multiple 8K chunks ending at newline
  private static String TMP_CSV_MULTIPLE_8K_CHUNKS = "multiple_8k_chunks.csv";
  // row count in count star file
  private static long ROW_COUNT = 1023;
  private static long ROW_COUNT_8K_CHUNKS = 1442;

  // normal query
  private static String QUERY =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_SMALL
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\n'"
          + ", extractHeader => false, skipFirstLine => false, autoGenerateColumnNames => true))";

  // normal query large columns
  private static String QUERY_LARGE =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_LARGE
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\n'"
          + ", extractHeader => false, skipFirstLine => false, autoGenerateColumnNames => true))";

  // query to skip line
  private static String QUERY_SKIPLINE_SMALL =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_SMALL
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\n'"
          + ", extractHeader => false, skipFirstLine => true, autoGenerateColumnNames => true))";

  // query to skip line large data
  private static String QUERY_SKIPLINE_LARGE =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_LARGE
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\n'"
          + ", extractHeader => false, skipFirstLine => true, autoGenerateColumnNames => true))";

  // bad lineDelimiter, no skipLine
  private static String QUERY_BAD_LINEDL =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_SMALL
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '~'"
          + ", extractHeader => false, skipFirstLine => false, autoGenerateColumnNames => true))";

  // bad lineDelimiter, no skipLine
  private static String QUERY_BAD_LINEDL_LARGE =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_LARGE
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '~'"
          + ", extractHeader => false, skipFirstLine => false, autoGenerateColumnNames => true))";

  // bad lineDelimiter, skip line
  private static String QUERY_BAD_LINEDL_SKIP_SMALL =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_SMALL
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '~'"
          + ", extractHeader => false, skipFirstLine => true, autoGenerateColumnNames => true))";

  private static String QUERY_BAD_LINEDL_SKIP_LARGE =
      "select * from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_LARGE
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '~'"
          + ", extractHeader => false, skipFirstLine => true, autoGenerateColumnNames => true))";

  // good lineDelimiter for count star, skip line
  private static String QUERY_BAD_LINEDL_SKIP_COUNT_STAR =
      "select count(*) from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_COUNT_STAR
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\r\n'"
          + ", extractHeader => false, skipFirstLine => false, autoGenerateColumnNames => false))";

  // incorrect lineDelimiter, skip line
  private static String QUERY_TWO_ROWS_SKIP_LINE =
      "select cast(A as int) column1, B column2, C column3, D column4 from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_FILE_TWO_ROWS
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\r\n'"
          + ", extractHeader => false, skipFirstLine => false, autoGenerateColumnNames => true))";

  private static String QUERY_MULTIPLE_8K_CHUNKS_FILTER =
      "select fcode from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_MULTIPLE_8K_CHUNKS
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\r\n'"
          + ", extractHeader => true, skipFirstLine => false, autoGenerateColumnNames => false))"
          + " where dtchex='123456'";

  private static String QUERY_MULTIPLE_8K_CHUNKS_COUNT_STAR =
      "select count(*) from table("
          + TEMP_SCHEMA
          + ".\""
          + TMP_CSV_MULTIPLE_8K_CHUNKS
          + "\""
          + " (type => 'text', fieldDelimiter => ',', "
          + "comment => '#', quote => '\"', "
          + "lineDelimiter => '\r\n'"
          + ", extractHeader => true, skipFirstLine => false, autoGenerateColumnNames => false))";

  private static File tblPathSmall = null;
  private static File tblPathLarge = null;
  private static File tblPathCountStar = null;
  private static File tblPathTwoRows = null;
  private static File tblPathMultiple8kChunks = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    setupDefaultTestCluster();
    tblPathSmall = new File(getDfsTestTmpSchemaLocation(), TMP_CSV_FILE_SMALL);
    tblPathLarge = new File(getDfsTestTmpSchemaLocation(), TMP_CSV_FILE_LARGE);
    tblPathCountStar = new File(getDfsTestTmpSchemaLocation(), TMP_CSV_FILE_COUNT_STAR);
    tblPathTwoRows = new File(getDfsTestTmpSchemaLocation(), TMP_CSV_FILE_TWO_ROWS);
    tblPathMultiple8kChunks = new File(getDfsTestTmpSchemaLocation(), TMP_CSV_MULTIPLE_8K_CHUNKS);

    FileUtils.deleteQuietly(tblPathSmall);
    FileUtils.deleteQuietly(tblPathLarge);
    FileUtils.deleteQuietly(tblPathCountStar);
    FileUtils.deleteQuietly(tblPathTwoRows);
    FileUtils.deleteQuietly(tblPathMultiple8kChunks);
    startTest();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    FileUtils.deleteQuietly(tblPathSmall);
    FileUtils.deleteQuietly(tblPathLarge);
    FileUtils.deleteQuietly(tblPathCountStar);
    FileUtils.deleteQuietly(tblPathTwoRows);
    FileUtils.deleteQuietly(tblPathMultiple8kChunks);
  }

  private static void startTest() throws Exception {
    final char myChar = 'a';
    try (FileWriter fwriter = new FileWriter(tblPathSmall)) {
      int j = 0;
      while (j++ < 3) {
        for (int i = 0; i < 8; i++) {
          fwriter.append(myChar);
        }
        fwriter.append('\n');
      }
    }

    try (FileWriter fwriter = new FileWriter(tblPathLarge)) {
      int boundary = 32001;
      int j = 0;
      while (j++ < 3) {
        for (int i = 0; i < boundary; i++) {
          fwriter.append(myChar);
        }
        fwriter.append('\n');
      }
    }

    try (FileWriter fwriter = new FileWriter(tblPathCountStar)) {
      long rows = ROW_COUNT;
      int columns = 20;
      for (long i = 0; i < rows; i++) {
        for (int j = 0; j < columns - 1; j++) {
          fwriter.append(myChar);
          fwriter.append(',');
        }
        fwriter.append(myChar);
        fwriter.append('\n');
      }
    }

    try (FileWriter fwriter = new FileWriter(tblPathTwoRows)) {
      long rows = 2;
      int columns = 20;
      final char numberChar = '1';
      for (long i = 0; i < rows; i++) {
        for (int j = 0; j < columns - 1; j++) {
          fwriter.append(numberChar);
          fwriter.append(',');
        }
        fwriter.append(numberChar);
        fwriter.append('\n');
      }
    }

    FileUtils.copyFile(
        com.dremio.common.util.FileUtils.getResourceAsFile(
            "/store/text/data/multiple_8k_chunks.csv"),
        tblPathMultiple8kChunks);
  }

  @Test
  public void testNormalQuery() throws Exception {
    runSQL(QUERY);
  }

  @Test
  public void testColumnExceedsSize() throws Exception {
    try {
      runSQL(QUERY_LARGE);
    } catch (Exception ex) {
      if (!(ex instanceof UserRemoteException)) {
        fail("Unexpected Error");
      }
      UserRemoteException urex = (UserRemoteException) ex;
      assertEquals(
          UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION, urex.getErrorType());
      boolean errorMsgMatched =
          Pattern.compile("(.*)Field with index(.*)exceeds the size limit(.*)", Pattern.DOTALL)
              .matcher(ex.getMessage())
              .matches();
      assertTrue(errorMsgMatched);
    }
  }

  @Test
  public void testSkipLineNormalQuery() throws Exception {
    runSQL(QUERY_SKIPLINE_SMALL);
  }

  @Test
  public void testSkipLineColumnExceedsSize() throws Exception {
    try {
      runSQL(QUERY_SKIPLINE_LARGE);
    } catch (Exception ex) {
      if (!(ex instanceof UserRemoteException)) {
        fail("Unexpected Error");
      }
      UserRemoteException urex = (UserRemoteException) ex;
      assertEquals(
          UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION, urex.getErrorType());
    }
  }

  @Test
  public void testBadLineDelimiterNormalQuery() throws Exception {
    runSQL(QUERY_BAD_LINEDL);
  }

  @Test
  public void testBadLineDelimiterColumnExceedsSize() throws Exception {
    runSQL(QUERY_BAD_LINEDL);
  }

  @Test
  public void testBadLineDelimiterSkipLineNormalQuery() throws Exception {
    try {
      runSQL(QUERY_BAD_LINEDL_SKIP_SMALL);
    } catch (Exception ex) {
      if (!(ex instanceof UserRemoteException)) {
        fail("Unexpected Error");
      }
      UserRemoteException urex = (UserRemoteException) ex;
      assertEquals(UserBitShared.DremioPBError.ErrorType.DATA_READ, urex.getErrorType());
    }
  }

  @Test
  public void testBadLineDelimiterSkipLineColumnExceedsSize() throws Exception {
    try {
      runSQL(QUERY_BAD_LINEDL_SKIP_LARGE);
    } catch (Exception ex) {
      if (!(ex instanceof UserRemoteException)) {
        fail("Unexpected Error");
      }
      UserRemoteException urex = (UserRemoteException) ex;
      assertEquals(
          UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION, urex.getErrorType());
    }
  }

  @Test
  public void testLineDelimiterSkipLineCountStar() throws Exception {
    testBuilder()
        .sqlQuery(QUERY_BAD_LINEDL_SKIP_COUNT_STAR)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(ROW_COUNT)
        .go();
  }

  @Test
  public void testIncorrectLineDelimiterSkipLine() throws Exception {
    runSQL(QUERY_TWO_ROWS_SKIP_LINE);
  }

  @Test
  public void testMultiple8kChunkFileFilter() throws Exception {
    testBuilder()
        .sqlQuery(QUERY_MULTIPLE_8K_CHUNKS_FILTER)
        .unOrdered()
        .baselineColumns("fcode")
        .baselineValues("12345")
        .baselineValues("12345")
        .go();
  }

  @Test
  public void testMultiple8kChunkFileCountStar() throws Exception {
    testBuilder()
        .sqlQuery(QUERY_MULTIPLE_8K_CHUNKS_COUNT_STAR)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(ROW_COUNT_8K_CHUNKS)
        .go();
  }
}
