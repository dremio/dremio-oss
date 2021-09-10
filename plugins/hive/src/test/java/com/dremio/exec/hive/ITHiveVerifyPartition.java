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
package com.dremio.exec.hive;

import static com.dremio.exec.hive.HiveTestUtilities.executeQuery;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hive.ql.Driver;
import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ITHiveVerifyPartition extends LazyDataGeneratingHiveTestBase {

  private static AutoCloseable enableUnlimitedSplitsSupportFlags;

  private final String format;
  private static final String tableName = "v2_partition_verify_test_";

  public ITHiveVerifyPartition(String formatForTest) {
    this.format = formatForTest;
  }

  @Parameterized.Parameters
  public static Collection tableInputFormats() {
    return Arrays.asList(new Object[][] {
      { "orc" },
      { "parquet" }
    });
  }

  @BeforeClass
  public static void setup() throws Exception {
    dataGenerator.generateTestData(ITHiveVerifyPartition::generateTestData);
    enableUnlimitedSplitsSupportFlags = enableUnlimitedSplitsSupportFlags();
  }

  @AfterClass
  public static void disableV2Flow() throws Exception {
    dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName + "orc");
    dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName + "parquet");
    enableUnlimitedSplitsSupportFlags.close();
  }

  @Test
  public void testBooleanPartitions() throws Exception {
    testBuilder()
      .sqlQuery("select * from hive." + tableName + format + " where boolean_part is true")
      .unOrdered()
      .baselineColumns("col", "boolean_part", "tinyint_part",
        "decimal0_part", "decimal9_part", "decimal18_part",
        "decimal28_part", "decimal38_part", "double_part",
        "float_part", "int_part", "bigint_part",
        "smallint_part", "string_part", "varchar_part",
        "timestamp_part", "date_part", "char_part")
      .baselineValues(
        2,
        true,
        65,
        new BigDecimal("38"),
        new BigDecimal("37.90"),
        new BigDecimal("3289379873.94565"),
        new BigDecimal("39579334534535.4"),
        new BigDecimal("363945093845093891.900"),
        9.345d,
        5.67f,
        123457,
        234236L,
        3456,
        "ab",
        "ab",
        new LocalDateTime(Timestamp.valueOf("2013-07-06 17:01:00").getTime()),
        new LocalDateTime(Date.valueOf("2013-07-06").getTime()),
        "ab")
      .baselineValues(
        1,
        true,
        64,
        new BigDecimal("37"),
        new BigDecimal("36.90"),
        new BigDecimal("3289379872.94565"),
        new BigDecimal("39579334534534.4"),
        new BigDecimal("363945093845093890.900"),
        8.345d,
        4.67f,
        123456,
        234235L,
        3455,
        "aa",
        "aa",
        new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
        new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
        "aa")
      .baselineValues(
        3,
        true,
        66,
        new BigDecimal("39"),
        new BigDecimal("38.90"),
        new BigDecimal("3289379874.94565"),
        new BigDecimal("39579334534536.4"),
        new BigDecimal("363945093845093892.900"),
        10.345d,
        6.67f,
        123458,
        234237L,
        3457,
        "ac",
        "ac",
        new LocalDateTime(Timestamp.valueOf("2013-07-07 17:01:00").getTime()),
        new LocalDateTime(Date.valueOf("2013-07-07").getTime()),
        "ac")
      .build()
      .run();
  }


  @Test
  public void testDecimal0Partition() throws Exception {
    testPartitions("decimal0_part", "38");
  }

  @Test
  public void testDecimal9Partition() throws Exception {
    testPartitions("decimal9_part", "37.9");
  }

  @Test
  public void testDecimal18Partition() throws Exception {
    testPartitions("decimal18_part", "3289379873.94565");
  }

  @Test
  public void testDecimal28Partition() throws Exception {
    testPartitions("decimal28_part", "39579334534535.4");
  }

  @Test
  public void testDecimal38Partition() throws Exception {
    testPartitions("decimal38_part", "363945093845093891.9");
  }

  @Test
  public void testDoublePartition() throws Exception {
    testPartitions("double_part", "9.345");
  }

  @Test
  public void testIntegerPartition() throws Exception {
    testPartitions("int_part", "123457");
  }

  @Test
  public void testBigIntPartition() throws Exception {
    testPartitions("bigint_part", "234236");
  }

  @Test
  public void testSmallIntPartition() throws Exception {
    testPartitions("smallint_part", "3456");
  }

  @Test
  public void testStringPartition() throws Exception {
    testPartitions("string_part", "'ab'");
  }

  @Test
  public void testVarcharPartition() throws Exception {
    testPartitions("varchar_part", "'ab'");
  }

  @Test
  public void testTimestampPartition() throws Exception {
    testPartitions("timestamp_part", "'2013-07-06 17:01:00'");
  }

  @Test
  public void testDatePartition() throws Exception {
    testPartitions("date_part", "'2013-07-06'");
  }

  @Test
  public void testCharPartition() throws Exception {
    testPartitions("char_part", "'ab'");
  }

  @Test
  public void testTinyIntPartition() throws Exception {
    testPartitions("tinyint_part", "'65'");
  }

  @Test
  public void testFloatPartition() throws Exception {
    testPartitions("float_part", "'5.67'");
  }

  private void testPartitions(String partition, String value) throws Exception {
    String queryEqual = String.format("select * from hive." + tableName + "%s where %s = %s", format, partition, value);
    String queryGreaterThan = String.format("select * from hive." + tableName + "%s where %s > %s", format, partition, value);
    String queryLessThan = String.format("select * from hive." + tableName + "%s where %s < %s", format, partition, value);

    testPhysicalPlan(queryEqual, "IcebergManifestList(table=");
    testPhysicalPlan(queryGreaterThan, "IcebergManifestList(table=");
    testPhysicalPlan(queryLessThan, "IcebergManifestList(table=");

    testBuilder()
      .sqlQuery(queryEqual)
      .unOrdered()
      .baselineColumns("col", "boolean_part", "tinyint_part",
        "decimal0_part", "decimal9_part", "decimal18_part",
        "decimal28_part", "decimal38_part", "double_part",
        "float_part", "int_part", "bigint_part",
        "smallint_part", "string_part", "varchar_part",
        "timestamp_part", "date_part", "char_part")
      .baselineValues(
        2,
        true,
        65,
        new BigDecimal("38"),
        new BigDecimal("37.90"),
        new BigDecimal("3289379873.94565"),
        new BigDecimal("39579334534535.4"),
        new BigDecimal("363945093845093891.900"),
        9.345d,
        5.67f,
        123457,
        234236L,
        3456,
        "ab",
        "ab",
        new LocalDateTime(Timestamp.valueOf("2013-07-06 17:01:00").getTime()),
        new LocalDateTime(Date.valueOf("2013-07-06").getTime()),
        "ab")
      .build()
      .run();

    testBuilder()
      .sqlQuery(queryGreaterThan)
      .unOrdered()
      .baselineColumns("col", "boolean_part", "tinyint_part",
        "decimal0_part", "decimal9_part", "decimal18_part",
        "decimal28_part", "decimal38_part", "double_part",
        "float_part", "int_part", "bigint_part",
        "smallint_part", "string_part", "varchar_part",
        "timestamp_part", "date_part", "char_part")
      .baselineValues(
        3,
        true,
        66,
        new BigDecimal("39"),
        new BigDecimal("38.90"),
        new BigDecimal("3289379874.94565"),
        new BigDecimal("39579334534536.4"),
        new BigDecimal("363945093845093892.900"),
        10.345d,
        6.67f,
        123458,
        234237L,
        3457,
        "ac",
        "ac",
        new LocalDateTime(Timestamp.valueOf("2013-07-07 17:01:00").getTime()),
        new LocalDateTime(Date.valueOf("2013-07-07").getTime()),
        "ac")
      .build()
      .run();

    testBuilder()
      .sqlQuery(queryLessThan)
      .unOrdered()
      .baselineColumns("col", "boolean_part", "tinyint_part",
        "decimal0_part", "decimal9_part", "decimal18_part",
        "decimal28_part", "decimal38_part", "double_part",
        "float_part", "int_part", "bigint_part",
        "smallint_part", "string_part", "varchar_part",
        "timestamp_part", "date_part", "char_part")
      .baselineValues(
        1,
        true,
        64,
        new BigDecimal("37"),
        new BigDecimal("36.90"),
        new BigDecimal("3289379872.94565"),
        new BigDecimal("39579334534534.4"),
        new BigDecimal("363945093845093890.900"),
        8.345d,
        4.67f,
        123456,
        234235L,
        3455,
        "aa",
        "aa",
        new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
        new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
        "aa")
      .build()
      .run();
  }

  private static Void generateTestData(Driver hiveDriver) {
    createAllTypesTable(hiveDriver, "orc");
    createAllTypesTable(hiveDriver, "parquet");
    return null;
  }

  private static void createAllTypesTable(Driver hiveDriver, String format) {
    executeQuery(hiveDriver,
      "CREATE TABLE " + tableName + format + "(" +
        "  col INT ) PARTITIONED BY (" +
        // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
        // "  binary_part BINARY," +
        "  boolean_part BOOLEAN," +
        "  tinyint_part TINYINT," +
        "  decimal0_part DECIMAL," +
        "  decimal9_part DECIMAL(6, 2)," +
        "  decimal18_part DECIMAL(15, 5)," +
        "  decimal28_part DECIMAL(23, 1)," +
        "  decimal38_part DECIMAL(30, 3)," +
        "  double_part DOUBLE," +
        "  float_part FLOAT," +
        "  int_part INT," +
        "  bigint_part BIGINT," +
        "  smallint_part SMALLINT," +
        "  string_part STRING," +
        "  varchar_part VARCHAR(50)," +
        "  timestamp_part TIMESTAMP," +
        "  date_part DATE," +
        "  char_part CHAR(10)" +
        ") STORED AS " + format
    );

    executeQuery(hiveDriver, "INSERT INTO " + tableName + format +
      " PARTITION (" +
      // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
      // "  binary_part='binary', " +
      "  boolean_part='true', " +
      "  tinyint_part='64', " +
      "  decimal0_part='36.9', " +
      "  decimal9_part='36.9', " +
      "  decimal18_part='3289379872.945645', " +
      "  decimal28_part='39579334534534.35345', " +
      "  decimal38_part='363945093845093890.9', " +
      "  double_part='8.345', " +
      "  float_part='4.67', " +
      "  int_part='123456', " +
      "  bigint_part='234235', " +
      "  smallint_part='3455', " +
      "  string_part='aa', " +
      "  varchar_part='aa', " +
      "  timestamp_part='2013-07-05 17:01:00', " +
      "  date_part='2013-07-05', " +
      "  char_part='aa'" +
      ") values (1)");

    executeQuery(hiveDriver, "INSERT INTO " + tableName + format +
      " PARTITION (" +
      // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
      // "  binary_part='binary', " +
      "  boolean_part='true', " +
      "  tinyint_part='65', " +
      "  decimal0_part='37.9', " +
      "  decimal9_part='37.9', " +
      "  decimal18_part='3289379873.945645', " +
      "  decimal28_part='39579334534535.35345', " +
      "  decimal38_part='363945093845093891.9', " +
      "  double_part='9.345', " +
      "  float_part='5.67', " +
      "  int_part='123457', " +
      "  bigint_part='234236', " +
      "  smallint_part='3456', " +
      "  string_part='ab', " +
      "  varchar_part='ab', " +
      "  timestamp_part='2013-07-06 17:01:00', " +
      "  date_part='2013-07-06', " +
      "  char_part='ab'" +
      ") values (2)");

    executeQuery(hiveDriver, "INSERT INTO " + tableName + format +
      " PARTITION (" +
      // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
      // "  binary_part='binary', " +
      "  boolean_part='true', " +
      "  tinyint_part='66', " +
      "  decimal0_part='38.9', " +
      "  decimal9_part='38.9', " +
      "  decimal18_part='3289379874.945645', " +
      "  decimal28_part='39579334534536.35345', " +
      "  decimal38_part='363945093845093892.9', " +
      "  double_part='10.345', " +
      "  float_part='6.67', " +
      "  int_part='123458', " +
      "  bigint_part='234237', " +
      "  smallint_part='3457', " +
      "  string_part='ac', " +
      "  varchar_part='ac', " +
      "  timestamp_part='2013-07-07 17:01:00', " +
      "  date_part='2013-07-07', " +
      "  char_part='ac'" +
      ") values (3)");
  }
}
