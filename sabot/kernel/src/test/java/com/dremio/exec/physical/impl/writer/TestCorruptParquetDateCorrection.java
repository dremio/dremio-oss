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
package com.dremio.exec.physical.impl.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.LocalDateTime;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;

/**
 * Tests for compatibility reading old parquet files after date corruption
 * issue was fixed in DRILL-4203.
 *
 * Drill was writing non-standard dates into parquet files for all releases
 * before 1.9.0. The values have been read by Drill correctly by Drill, but
 * external tools like Spark reading the files will see corrupted values for
 * all dates that have been written by Drill.
 *
 * This change corrects the behavior of the Drill parquet writer to correctly
 * store dates in the format given in the parquet specification.
 *
 * To maintain compatibility with old files, the parquet reader code has
 * been updated to check for the old format and automatically shift the
 * corrupted values into corrected ones automatically.
 *
 * The test cases included here should ensure that all files produced by
 * historical versions of Drill will continue to return the same values they
 * had in previous releases. For compatibility with external tools, any old
 * files with corrupted dates can be re-written using the CREATE TABLE AS
 * command (as the writer will now only produce the specification-compliant
 * values, even if after reading out of older corrupt files).
 *
 * While the old behavior was a consistent shift into an unlikely range
 * to be used in a modern database (over 10,000 years in the future), these are still
 * valid date values. In the case where these may have been written into
 * files intentionally, and we cannot be certain from the metadata if Drill
 * produced the files, an option is included to turn off the auto-correction.
 * Use of this option is assumed to be extremely unlikely, but it is included
 * for completeness.
 */
public class TestCorruptParquetDateCorrection extends PlanTestBase {

  // 4 files are in the directory:
  //    - one created with the fixed version of the reader
  //        - files have extra meta field: is.date.correct = true
  //    - one from and old version of Drill, before we put in proper created by in metadata
  //        - this is read properly by looking at a Max value in the file statistics, to see that
  //          it is way off of a typical date value
  //        - this behavior will be able to be turned off, but will be on by default
  //    - one from the 0.6 version of Drill, before files had min/max statistics
  //        - detecting corrupt values must be deferred to actual data page reading
  //    - one from 1.4, where there is a proper created-by, but the corruption is present
  public static final String MIXED_CORRUPTED_AND_CORRECTED_DATES_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/mixed_drill_versions";
  // partitioned with 1.4.0, date values are known to be corrupt
  private static final String CORRUPTED_PARTITIONED_DATES_1_4_0_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/partitioned_with_corruption_4203";
  // partitioned with 1.2.0, no certain metadata that these were written with Drill
  // the value will be checked to see that they look corrupt and they will be corrected
  // by default. Users can use the format plugin option autoCorrectCorruptDates to disable
  // this behavior if they have foreign parquet files with valid rare date values that are
  // in the similar range as Drill's corrupt values
  private static final String CORRUPTED_PARTITIONED_DATES_1_2_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/partitioned_with_corruption_4203_1_2";
  private static final String PARQUET_DATE_FILE_WITH_NULL_FILLED_COLS =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/null_date_cols_with_corruption_4203.parquet";
  private static final String PARQUET_FILE_WITH_ZERO_ROWGROUPS =
    "[WORKING_PATH]/src/test/resources/parquet/zero_row_groups_empty.parquet";
  private static final String CORRECTED_PARTITIONED_DATES_1_9_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/1_9_0_partitioned_no_corruption";
  private static final String CORRECTED_PARTITIONED_DATES_1_10_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/1_10_0_partitioned_no_corruption";
  private static final String VARCHAR_PARTITIONED =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/fewtypes_varcharpartition";
  private static final String DATE_PARTITIONED =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/fewtypes_datepartition";
  private static final String EXCEPTION_WHILE_PARSING_CREATED_BY_META =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/hive1dot2_fewtypes_null";
  private static final String PARQUET_DATE_FILE_WITH_DATES_BEYOND_5000 =
      "[WORKING_PATH]/src/test/resources/parquet/dates_beyond_5000.parquet";

  private static FileSystem fs;
  private static Path path;
  static String PARTITIONED_1_2_FOLDER = "partitioned_with_corruption_4203_1_2";
  static String MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER = "mixed_partitioned";

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");
    fs = FileSystem.get(conf);
    path = new Path(getDfsTestTmpSchemaLocation());

    // Move files into temp directory, rewrite the metadata cache file to contain the appropriate absolute
    // path
    copyDirectoryIntoTempSpace(CORRUPTED_PARTITIONED_DATES_1_2_PATH);
    copyDirectoryIntoTempSpace(CORRUPTED_PARTITIONED_DATES_1_2_PATH, MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER);
    copyDirectoryIntoTempSpace(CORRECTED_PARTITIONED_DATES_1_9_PATH, MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER);
    copyDirectoryIntoTempSpace(CORRUPTED_PARTITIONED_DATES_1_4_0_PATH, MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER);
  }

  /**
   * Test reading a directory full of partitioned parquet files with dates, these files have a drill version
   * number of 1.9.0-SNAPSHOT and is.date.correct = true label in their footers, so we can be certain
   * they do not have corruption. The option to disable the correction is passed, but it will not change the result
   * in the case where we are certain correction is NOT needed. For more info see DRILL-4203.
   */
  @Test
  public void testReadPartitionedOnCorrectedDates() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_AUTO_CORRECT_DATES));
      for(String testPath : Arrays.asList(CORRECTED_PARTITIONED_DATES_1_9_PATH, CORRECTED_PARTITIONED_DATES_1_10_PATH)) {
        for (String selection : new String[]{"*", "date_col"}) {
          // for sanity, try reading all partitions without a filter
          TestBuilder builder = testBuilder()
              .sqlQuery("select " + selection + " from table(dfs.\"" + testPath + "\"" +
                  "(type => 'parquet', autoCorrectCorruptDates => false))")
              .unOrdered()
              .baselineColumns("date_col");
          addDateBaselineVals(builder);
          builder.go();

          String query = "select " + selection + " from table(dfs.\"" + testPath + "\" " +
              "(type => 'parquet', autoCorrectCorruptDates => false))" + " where date_col = date '1970-01-01'";
          // verify that pruning is actually taking place
          testPlanMatchingPatterns(query, new String[]{"splits=\\[1"}, null);

          // read with a filter on the partition column
          testBuilder()
              .sqlQuery(query)
              .unOrdered()
              .baselineColumns("date_col")
              .baselineValues(new LocalDateTime(1970, 1, 1, 0, 0))
              .go();
        }
      }
    } finally {
      test("alter session reset all");
    }
  }

  @Test
  public void testVarcharPartitionedReadWithCorruption() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_AUTO_CORRECT_DATES));

      testBuilder()
        .sqlQuery("select date_col from " +
          "dfs.\"" + VARCHAR_PARTITIONED + "\"" +
          "where length(varchar_col) = 12")
        .baselineColumns("date_col")
        .unOrdered()
        .baselineValues(new LocalDateTime(2039, 4, 9, 0, 0))
        .baselineValues(new LocalDateTime(1999, 1, 8, 0, 0))
        .go();
    } finally {
      test("alter session reset all");
    }
  }

  @Test
  public void testDatePartitionedReadWithCorruption() throws Exception {
    try (AutoCloseable c1 = disableUnlimitedSplitsSupportFlags();
         AutoCloseable c2 = withSystemOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR, true)) {
      testBuilder()
        .sqlQuery("select date_col from " +
          "dfs.\"" + DATE_PARTITIONED + "\"" +
          "where date_col = '1999-04-08'")
        .baselineColumns("date_col")
        .unOrdered()
        .baselineValues(new LocalDateTime(1999, 4, 8, 0, 0))
        .go();

      String sql = "select date_col from dfs.\"" + DATE_PARTITIONED + "\" where date_col > '1999-04-08'";
      testPlanMatchingPatterns(sql, new String[]{"splits=\\[6"}, null);
    }
  }

  @Test
  public void testCorrectDatesAndExceptionWhileParsingCreatedBy() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_AUTO_CORRECT_DATES));
      testBuilder()
          .sqlQuery("select date_col from " +
              "dfs.\"" + EXCEPTION_WHILE_PARSING_CREATED_BY_META +
              "\" where to_date(date_col, 'YYYY-MM-DD') < '1997-01-02'")
          .baselineColumns("date_col")
          .unOrdered()
          .baselineValues(new LocalDateTime(1996, 1, 29, 0, 0))
          .baselineValues(new LocalDateTime(1996, 3, 1, 0, 0))
          .baselineValues(new LocalDateTime(1996, 3, 2, 0, 0))
          .go();
    } finally {
      test("alter session reset all");
    }
  }

  /**
   * Test reading a directory full of partitioned parquet files with dates, these files have a drill version
   * number of 1.4.0 in their footers, so we can be certain they are corrupt. The option to disable the
   * correction is passed, but it will not change the result in the case where we are certain correction
   * is needed. For more info see DRILL-4203.
   */
  @Test
  public void testReadPartitionedOnCorruptedDates() throws Exception {
    for (String selection : new String[]{"*", "date_col"}) {
      // for sanity, try reading all partitions without a filter
      TestBuilder builder = testBuilder()
        .sqlQuery("select " + selection + " from table(dfs.\"" + CORRUPTED_PARTITIONED_DATES_1_4_0_PATH + "\"" +
          "(type => 'parquet'))")
        .unOrdered()
        .baselineColumns("date_col");
      addDateBaselineVals(builder);
      builder.go();

      String query = "select " + selection + " from table(dfs.\"" + CORRUPTED_PARTITIONED_DATES_1_4_0_PATH + "\" " +
        "(type => 'parquet'))" + " where date_col = date '1970-01-01'";
      // verify that pruning is actually taking place
      testPlanMatchingPatterns(query, new String[]{"splits=\\[1"}, null);

      // read with a filter on the partition column
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("date_col")
        .baselineValues(new LocalDateTime(1970, 1, 1, 0, 0))
        .go();
    }
  }

  // Temporarily disabled until calcite supports year with more than 4 digits
  @Ignore("DX-11468")
  @Test
  public void testReadPartitionedOnCorruptedDates_UserDisabledCorrection() throws Exception {
    for (String selection : new String[]{"*", "date_col"}) {
      // for sanity, try reading all partitions without a filter
      TestBuilder builder = testBuilder()
        .sqlQuery("select " + selection + " from table(dfs.\"" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "\"" +
          "(type => 'parquet'))")
        .unOrdered()
        .baselineColumns("date_col");
      addCorruptedDateBaselineVals(builder);
      builder.go();

      String query = "select " + selection + " from table(dfs.\"" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "\" " +
        "(type => 'parquet'))" + " where date_col > cast('15334-03-17' as date)";
      // verify that pruning is actually taking place
      testPlanMatchingPatterns(query, new String[]{"splits=\\[1"}, null);

      // read with a filter on the partition column
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("date_col")
        .baselineValues(new LocalDateTime(15334, 03, 17, 0, 0))
        .go();
    }
  }

  @Test
  public void testCorruptValDetectionDuringPruning() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_AUTO_CORRECT_DATES));
      for (String selection : new String[]{"*", "date_col"}) {
        // for sanity, try reading all partitions without a filter
        TestBuilder builder = testBuilder()
            .sqlQuery("select " + selection + " from dfs.\"" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "\"")
            .unOrdered()
            .baselineColumns("date_col");
        addDateBaselineVals(builder);
        builder.go();

        String query = "select " + selection + " from dfs.\"" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "\"" +
            " where date_col = date '1970-01-01'";
        // verify that pruning is actually taking place
        testPlanMatchingPatterns(query, new String[]{"splits=\\[1"}, null);

        // read with a filter on the partition column
        testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new LocalDateTime(1970, 1, 1, 0, 0))
            .go();
      }
    } finally {
      test("alter session reset all");
    }
  }

  /**
   * To fix some of the corrupted dates fixed as part of DRILL-4203 it requires
   * actually looking at the values stored in the file. A column with date values
   * actually stored must be located to check a value. Just because we find one
   * column where the all values are null does not mean we can safely avoid reading
   * date columns with auto-correction, although null values do not need fixing,
   * other columns may contain actual corrupt date values.
   *
   * This test checks the case where the first columns in the file are all null filled
   * and a later column must be found to identify that the file is corrupt.
   */
  @Test
  public void testReadCorruptDatesWithNullFilledColumns() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_AUTO_CORRECT_DATES));
      testBuilder()
          .sqlQuery("select null_dates_1, null_dates_2, date_col from dfs.\"" +
              PARQUET_DATE_FILE_WITH_NULL_FILLED_COLS + "\"")
          .unOrdered()
          .baselineColumns("null_dates_1", "null_dates_2", "date_col")
          .baselineValues(null, null, new LocalDateTime(1970, 1, 1, 0, 0))
          .baselineValues(null, null, new LocalDateTime(1970, 1, 2, 0, 0))
          .baselineValues(null, null, new LocalDateTime(1969, 12, 31, 0, 0))
          .baselineValues(null, null, new LocalDateTime(1969, 12, 30, 0, 0))
          .baselineValues(null, null, new LocalDateTime(1900, 1, 1, 0, 0))
          .baselineValues(null, null, new LocalDateTime(2015, 1, 1, 0, 0))
          .go();
    } finally {
      test("alter session reset all");
    }
  }

  @Test
  public void testReadZeroRowGroupEmptyParquetFile() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_AUTO_CORRECT_DATES));
      testBuilder()
        .sqlQuery("select * from dfs.\"" +
          PARQUET_FILE_WITH_ZERO_ROWGROUPS + "\"")
        .unOrdered()
        .baselineColumns("f1", "f2")
        .expectsEmptyResultSet()
        .go();
    } finally {
      test("alter session reset all");
    }
  }

  @Test
  public void testUserOverrideDateCorrection() throws Exception {
    try {
      // read once with the flat reader
      readFilesWithUserDisabledAutoCorrection();
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_NEW_RECORD_READER));
      // read all of the types with the complex reader
      readFilesWithUserDisabledAutoCorrection();
    } finally {
      test("alter session reset all");
    }
  }

  /**
   * Test reading a directory full of parquet files with dates, some of which have corrupted values
   * due to DRILL-4203.
   *
   * Tests reading the files with both the vectorized and complex parquet readers.
   *
   * @throws Exception
   */
  @Test
  public void testReadMixedOldAndNewBothReaders() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_AUTO_CORRECT_DATES));
      /// read once with the flat reader
      readMixedCorruptedAndCorrectedDates();
      // read all of the types with the complex reader
      readMixedCorruptedAndCorrectedDates();
    } finally {
      test(String.format("alter session set %s = false", ExecConstants.PARQUET_NEW_RECORD_READER));
    }
  }

  /**
   * Input file has a column with dates beyond 5000th year.
   * By default, date correction is disabled. So, those dates should be in output without any conversion
   * @throws Exception
   */
  @Test
  public void testDeaultDateCorrectionDisabled() throws Exception {
    testBuilder()
      .sqlQuery("select f1, f2 from dfs.\"" + PARQUET_DATE_FILE_WITH_DATES_BEYOND_5000 + "\"")
      .unOrdered()
      .baselineColumns("f1", "f2")
      .baselineValues(new LocalDateTime(2020, 1, 1, 0, 0), new LocalDateTime(5050, 1, 1, 0, 0))
      .baselineValues(new LocalDateTime(2020, 1, 2, 0, 0), new LocalDateTime(5050, 1, 2, 0, 0))
      .go();
  }

  /**
   * Read a directory with parquet files where some have corrupted dates, see DRILL-4203.
   * @throws Exception
   */
  private void readMixedCorruptedAndCorrectedDates() throws Exception {
    // ensure that selecting the date column explicitly or as part of a star still results
    // in checking the file metadata for date columns (when we need to check the statistics
    // for bad values) to set the flag that the values are corrupt
    for (String selection : new String[] {"*", "date_col"}) {
      TestBuilder builder = testBuilder()
          .sqlQuery("select " + selection + " from dfs.\"" + MIXED_CORRUPTED_AND_CORRECTED_DATES_PATH + "\"")
          .unOrdered()
          .baselineColumns("date_col");
      for (int i = 0; i < 4; i++) {
        addDateBaselineVals(builder);
      }
      builder.go();
    }
  }


  private void addDateBaselineVals(TestBuilder builder) {
    builder
        .baselineValues(new LocalDateTime(1970, 1, 1, 0, 0))
        .baselineValues(new LocalDateTime(1970, 1, 2, 0, 0))
        .baselineValues(new LocalDateTime(1969, 12, 31, 0, 0))
        .baselineValues(new LocalDateTime(1969, 12, 30, 0, 0))
        .baselineValues(new LocalDateTime(1900, 1, 1, 0, 0))
        .baselineValues(new LocalDateTime(2015, 1, 1, 0, 0));
  }

  /**
   * These are the same values added in the addDateBaselineVals, shifted as corrupt values
   */
  private void addCorruptedDateBaselineVals(TestBuilder builder) {
    builder
        .baselineValues(new LocalDateTime(15334, 03, 17, 0, 0))
        .baselineValues(new LocalDateTime(15334, 03, 18, 0, 0))
        .baselineValues(new LocalDateTime(15334, 03, 15, 0, 0))
        .baselineValues(new LocalDateTime(15334, 03, 16, 0, 0))
        .baselineValues(new LocalDateTime(15264, 03, 16, 0, 0))
        .baselineValues(new LocalDateTime(15379, 03, 17, 0, 0));
  }

  private void readFilesWithUserDisabledAutoCorrection() throws Exception {
    // ensure that selecting the date column explicitly or as part of a star still results
    // in checking the file metadata for date columns (when we need to check the statistics
    // for bad values) to set the flag that the values are corrupt
    for (String selection : new String[] {"*", "date_col"}) {
      TestBuilder builder = testBuilder()
          .sqlQuery("select " + selection + " from table(dfs.\"" + MIXED_CORRUPTED_AND_CORRECTED_DATES_PATH + "\"" +
              "(type => 'parquet'))")
          .unOrdered()
          .baselineColumns("date_col");
      addDateBaselineVals(builder);
      addDateBaselineVals(builder);
      addCorruptedDateBaselineVals(builder);
      addCorruptedDateBaselineVals(builder);
      builder.go();
    }
  }

  private static String replaceWorkingPathInString(String orig) {
    return orig.replaceAll(Pattern.quote("[WORKING_PATH]"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
  }

  private static void copyDirectoryIntoTempSpace(String resourcesDir) throws IOException {
    copyDirectoryIntoTempSpace(resourcesDir, null);
  }

  private static void copyDirectoryIntoTempSpace(String resourcesDir, String destinationSubDir) throws IOException {
    Path destination = path;
    if (destinationSubDir != null) {
      destination = new Path(path, destinationSubDir);
    }
    fs.copyFromLocalFile(
        new Path(replaceWorkingPathInString(resourcesDir)),
        destination);
  }

}
