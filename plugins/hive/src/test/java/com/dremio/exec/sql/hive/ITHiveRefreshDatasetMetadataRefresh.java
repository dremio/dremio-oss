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
package com.dremio.exec.sql.hive;

import static com.dremio.exec.store.hive.exec.HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.fsDelete;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.verifyIcebergMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.hive.LazyDataGeneratingHiveTestBase;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.hive.HiveTestDataGenerator;
import com.google.common.collect.Sets;

public class ITHiveRefreshDatasetMetadataRefresh extends LazyDataGeneratingHiveTestBase {

  private static final String HIVE = "hive.";
  private static final String REFRESH_DATASET = "ALTER PDS %s REFRESH METADATA";
  private static final String FORGET = "ALTER PDS %s FORGET METADATA";
  static final String EXPLAIN_PLAN = "EXPLAIN PLAN FOR ";
  public static String formatType = "PARQUET";
  private static FileSystem fs;
  private static String finalIcebergMetadataLocation;
  protected static String warehouseDir;

  @BeforeClass
  public static void generateHiveWithoutData() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();
    LazyDataGeneratingHiveTestBase.generateHiveWithoutData();

    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_test_mapColumn_" + formatType + "(col1 INT, col2 MAP<STRING,STRING>) STORED AS " + formatType);
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_v2_test_" + formatType + "(col1 INT, col2 STRING) STORED AS " + formatType);
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_v2_test_partition_" + formatType + "(id INT) PARTITIONED BY (year INT, month STRING) STORED AS " + formatType);
    // By default will create TextFileFormat table which is not supported in refresh dataset flow
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_v2_test_invalid_" + formatType + "(col1 INT, col2 STRING)");
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_v2_test_special_chars_partitions_" + formatType + " (int_field INT) PARTITIONED BY (timestamp_part TIMESTAMP, char_part CHAR(10)) STORED AS " + formatType);
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_v2_test_table_permission_" + formatType + "(col1 INT, col2 STRING) STORED AS " + formatType);
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_v2_test_partition_permission_" + formatType + "(id INT) PARTITIONED BY (year INT, month STRING) STORED AS " + formatType);
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS refresh_v2_test_deleted_partition_" + formatType + "(id INT) PARTITIONED BY (year INT, month STRING) STORED AS " + formatType);

    // Dremio supports 800 columns by default, let's create a table with more columns.
    final String createWideTable = IntStream.range(0, 810).mapToObj(i -> "COL" + i + " int").collect(Collectors.joining(", ", "CREATE TABLE refresh_v2_test_maxwidth_" + formatType + "(", ") STORED AS " + formatType));
    dataGenerator.executeDDL(createWideTable);

    warehouseDir = dataGenerator.getWhDir();
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
    fs = setupLocalFS();
  }

  @AfterClass
  public static void dropTables() throws Exception {
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_test_mapColumn_" + formatType);
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_" + formatType );
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_partition_" + formatType);
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_invalid_" + formatType);
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_special_chars_partitions_" + formatType);
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_maxwidth_" + formatType);
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_table_permission_" + formatType);
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_partition_permission_" + formatType);
    dataGenerator.executeDDL("DROP TABLE IF EXISTS refresh_v2_test_deleted_partition_" + formatType);
  }

  @After
  public void cleanUp() throws IOException {
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
    //TODO: also cleanup the KV store so that if 2 tests are working on the same dataset we don't get issues.
  }

  @Test
  public void testFullRefreshWithoutPartition() throws Exception {
    final String tableName = "refresh_v2_test_" + formatType;
    final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
    dataGenerator.executeDDL(insertCmd);

    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
    runSQL(sql);

    Schema expectedSchema = new Schema(Arrays.asList(
      Types.NestedField.optional(1, "col1", new Types.IntegerType()),
      Types.NestedField.optional(2, "col2", new Types.StringType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, new HashSet<>(), 1);

    String selectQuery = "SELECT * from " + HIVE + tableName;
    testBuilder()
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, "a")
      .go();

    verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
  }

  @Test
  public void testFullRefreshWithPartition() throws Exception {
    final String tableName = "refresh_v2_test_partition_" + formatType;
    final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(1)";
    final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Jan') VALUES(2)";
    dataGenerator.executeDDL(insertCmd1);
    dataGenerator.executeDDL(insertCmd2);

    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
    runSQL(sql);

    Schema expectedSchema = new Schema(Arrays.asList(
      Types.NestedField.optional(1, "id", new Types.IntegerType()),
      Types.NestedField.optional(2, "month", new Types.StringType()),
      Types.NestedField.optional(3, "year", new Types.IntegerType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema, Sets.newHashSet("year", "month"), 2);

    String selectQuery = "SELECT * from " + HIVE + tableName;
    testBuilder()
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns("id", "month", "year")
      .baselineValues(1, "Feb", 2020)
      .baselineValues(2, "Jan", 2021)
      .go();

    verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
  }

  @Test
  public void testInvalidTableFullRefresh() throws Exception {
    final String tableName = "refresh_v2_test_invalid_" + formatType;
    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
    runSQL(sql);

    // Check that no iceberg table created
    assertThat(isDirEmpty(Paths.get(finalIcebergMetadataLocation))).isTrue();
  }

  @Test
  public void testFullRefreshSpecialCharPaths() throws Exception {
    final String tableName = "refresh_v2_test_special_chars_partitions_" + formatType;
    dataGenerator.executeDDL("INSERT INTO " + tableName + " PARTITION(timestamp_part = '2013-07-05 17:01:00', char_part = 'spa c es') values (1)");

    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
    runSQL(sql);

    Schema expectedSchema = new Schema(Arrays.asList(
            Types.NestedField.optional(1, "int_field", new Types.IntegerType()),
            Types.NestedField.optional(2, "timestamp_part", Types.TimestampType.withZone()),
            Types.NestedField.optional(3, "char_part", new Types.StringType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("timestamp_part", "char_part"), 1);
    String selectQuery = "SELECT int_field from " + HIVE + tableName;
    testBuilder()
            .sqlQuery(selectQuery)
            .unOrdered()
            .baselineColumns("int_field")
            .baselineValues(1)
            .go();

    verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
  }

  @Test
  public void testFailTableOptionQuery() throws Exception {
    final String tableName = "refresh_v2_test_table_option_" + formatType;
    try {
      dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS " + tableName + "(col1 INT, col2 STRING) STORED AS PARQUET");
      runSQL(String.format(REFRESH_DATASET, HIVE + tableName));

      assertThatThrownBy(() -> runSQL(setTableOptionQuery("hive.\"default\"." + tableName,
        HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true")))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("ALTER unsupported on table 'hive.\"default\".refresh_v2_test_table_option_" + formatType + "'");
    } finally {
      dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
    }
  }

  @Test
  public void testFullRefreshWideCols() throws Exception {
    final String tableName = "refresh_v2_test_maxwidth_" + formatType;
    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);

    try (AutoCloseable c1 = setMaxLeafColumns(1000)) {
      runSQL(sql);
      // no exception expected
    } finally {
      runSQL(String.format(FORGET, HIVE + tableName));
    }

    try (AutoCloseable c1 = setMaxLeafColumns(800)) {
      assertThatThrownBy(() -> runSQL(sql))
          .hasMessageContaining("Number of fields in dataset exceeded the maximum number of fields of 800");
    }
  }

  @Test
  public void testCheckTableHasPermission() throws Exception {
    final String tableName = "refresh_v2_test_table_permission_" + formatType;
    final String insertCmd1 = "INSERT INTO " + tableName + " VALUES(2020, 'Jan')";
    dataGenerator.executeDDL(insertCmd1);

    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
    // this will do a full refresh first
    runSQL(sql);

    final Path tableDir = new Path(dataGenerator.getWhDir() + "/" + tableName.toLowerCase(Locale.ROOT));
    try {
      // no exec on dir
      fs.setPermission(tableDir, new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE));
      assertThatThrownBy(() -> test("SELECT * from " + HIVE + tableName))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("PERMISSION ERROR: Access denied reading dataset")
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .isEqualTo(UserBitShared.DremioPBError.ErrorType.PERMISSION);
    } finally {
      fs.setPermission(tableDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  @Test
  public void testCheckPartitionHasPermission() throws Exception {
    final String tableName = "refresh_v2_test_partition_permission_" + formatType;
    final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
    dataGenerator.executeDDL(insertCmd1);

    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
    // this will do a full refresh first
    runSQL(sql);

    final Path partitionDir = new Path(dataGenerator.getWhDir() + "/" + tableName.toLowerCase(Locale.ROOT) + "/year=2020/month=Jan");
    try {
      // no exec on dir
      fs.setPermission(partitionDir, new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE));
      assertThatThrownBy(() -> test("SELECT * from " + HIVE + tableName))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("PERMISSION ERROR: Access denied reading dataset")
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .isEqualTo(UserBitShared.DremioPBError.ErrorType.PERMISSION);
    } finally {
      fs.setPermission(partitionDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  @Test
  public void testFullRefreshWithNonExistentPartitionDir() throws Exception {
    final String tableName = "refresh_v2_test_deleted_partition_" + formatType;
    final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
    final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Jan') VALUES(2)";
    dataGenerator.executeDDL(insertCmd1);
    dataGenerator.executeDDL(insertCmd2);

    // deleting a partition directory
    final Path partitionDir = new Path(dataGenerator.getWhDir() + "/" + tableName.toLowerCase(Locale.ROOT) + "/year=2020/month=Jan");
    assertThat(fs.delete(partitionDir, true)).isTrue();

    final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
    // this will do a full refresh
    runSQL(sql);

    String selectQuery = "SELECT * from " + HIVE + tableName;
    testBuilder()
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns("id", "month", "year")
      .baselineValues(2, "Jan", 2021)
      .go();
  }

  @Test
  public void testRefreshWithMapColumn() throws Exception {
    try (AutoCloseable c1 = enableHiveParquetComplexTypes();
         AutoCloseable c2 = enableMapDataType()) {
      final String tableName = "refresh_test_mapColumn_" + formatType;
      final String insertCmd = "INSERT INTO " + tableName + " SELECT 1, map('a','aa')";
      final String selectQuery = "SELECT col2['a'] as m1 FROM " + HIVE + tableName;

      dataGenerator.executeDDL(insertCmd); // insert 1st row

      final String sql = String.format(REFRESH_DATASET, HIVE + tableName);

      runSQL(sql);

      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("m1")
        .baselineValues("aa")
        .go();

      dataGenerator.executeDDL(insertCmd); // insert 2nd row

      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("m1")
        .baselineValues("aa")  // only 1st row is returned since we didn't run refresh metadata
        .go();

      runSQL(sql); //running refresh metadata query

      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("m1")
        .baselineValues("aa") // 2 rows are returned after alter table refresh metadata query.
        .baselineValues("aa")
        .go();
    }
  }

  private static boolean isDirEmpty(final java.nio.file.Path directory) throws IOException {
    try(DirectoryStream<java.nio.file.Path> dirStream = Files.newDirectoryStream(directory)) {
      return !dirStream.iterator().hasNext();
    }
  }

  static void verifyIcebergExecution(String query, String icebergMetadataLocation) throws Exception {
    final String plan = getPlanInString(query, OPTIQ_FORMAT);
    // Check and make sure that IcebergManifestList is present in the plan
    assertThat(plan).contains("IcebergManifestList");
    if (icebergMetadataLocation != null && !icebergMetadataLocation.isEmpty()) {
      assertThat(plan).contains("metadataFileLocation=[file://" + icebergMetadataLocation);
    }
  }

  static HiveTestDataGenerator getDataGenerator() {
    return dataGenerator;
  }
}
