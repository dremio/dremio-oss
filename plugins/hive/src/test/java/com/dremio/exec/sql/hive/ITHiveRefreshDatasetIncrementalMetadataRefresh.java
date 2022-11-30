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

import static com.dremio.exec.sql.hive.ITHiveRefreshDatasetMetadataRefresh.EXPLAIN_PLAN;
import static com.dremio.exec.sql.hive.ITHiveRefreshDatasetMetadataRefresh.verifyIcebergExecution;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.fsDelete;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.setupLocalFS;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.verifyIcebergMetadata;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.hive.LazyDataGeneratingHiveTestBase;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.iceberg.model.IncrementalMetadataRefreshCommitter;
import com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.google.common.collect.Sets;

public class ITHiveRefreshDatasetIncrementalMetadataRefresh extends LazyDataGeneratingHiveTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ITHiveRefreshDatasetIncrementalMetadataRefresh.class);
  protected static final String HIVE = "hive.";
  protected static final String ALTER_PDS_REFRESH = "ALTER PDS %s REFRESH METADATA";
  protected static final String REFRESH_DATASET = "REFRESH DATASET %s";
  private static final String FORGET_DATASET = "ALTER TABLE %s FORGET METADATA";
  private static final String REFRESH_DONE = "Metadata for table '%s' refreshed.";
  private static final String REFRESH_NOT_DONE = "Table '%s' read signature reviewed but source stated metadata is unchanged, no refresh occurred.";

  private static FileSystem fs;
  protected static String finalIcebergMetadataLocation;
  private static AutoCloseable enableUnlimitedSplitsSupportFlags;

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(250, TimeUnit.SECONDS);

  @BeforeClass
  public static void generateHiveWithoutData() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();
    LazyDataGeneratingHiveTestBase.generateHiveWithoutData();
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
    fs = setupLocalFS();
    enableUnlimitedSplitsSupportFlags = enableUnlimitedSplitsSupportFlags();
  }

  @AfterClass
  public static void close() throws Exception {
    enableUnlimitedSplitsSupportFlags.close();
  }

  @After
  public void tearDown() throws IOException {
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
  }

  @Test
  public void testIncrementalRefreshWithNoChanges() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.IntegerType()),
        Types.NestedField.optional(2, "col2", new Types.StringType())));

      verifyMetadata(expectedSchema, 1);

      Table icebergTable = getIcebergTable();
      long oldSnapShotId = icebergTable.currentSnapshot().snapshotId();
      runFullRefreshExpectingNoChange(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //No change in snapshot Id
      assertEquals(oldSnapShotId, icebergTable.currentSnapshot().snapshotId());
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshWithChangesThenNoChanges() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      final String insertCmd2 = "INSERT INTO " + tableName + " VALUES(2, 'b')";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.IntegerType()),
        Types.NestedField.optional(2, "col2", new Types.StringType())));

      verifyMetadata(expectedSchema, 2);

      long nextSnapshotId = icebergTable.currentSnapshot().snapshotId();
      runFullRefreshExpectingNoChange(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //No change in snapshot Id
      assertEquals(nextSnapshotId, icebergTable.currentSnapshot().snapshotId());
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testMultipleIncrementalRefreshWithAndWithoutChanges() throws Exception {
    final String tableName = "incrrefresh_v2_test_repeat_complex_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(a INT, b STRUCT<a:INT>, c INT)");
      final String insertCmd = "INSERT INTO " + tableName + " SELECT 1, named_struct('a', 1), 1";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      final String insertCmd2 = "INSERT INTO " + tableName + " SELECT 2, named_struct('a', 2), 2";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      long nextSnapshotId = icebergTable.currentSnapshot().snapshotId();
      runFullRefreshExpectingNoChange(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //No change in snapshot Id
      assertEquals(nextSnapshotId, icebergTable.currentSnapshot().snapshotId());

      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      nextSnapshotId = icebergTable.currentSnapshot().snapshotId();
      runFullRefreshExpectingNoChange(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //No change in snapshot Id
      assertEquals(nextSnapshotId, icebergTable.currentSnapshot().snapshotId());
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }


  @Test
  public void testIncrementalRefreshFileAddition() throws Exception {
    final String tableName = "incrrefresh_v2_test_file_add_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      final String insertCmd2 = "INSERT INTO " + tableName + " VALUES(2, 'b')";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.IntegerType()),
        Types.NestedField.optional(2, "col2", new Types.StringType())));

      verifyMetadata(expectedSchema, 2);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(2, "b")
        .baselineValues(1, "a")
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshPartitionAddition() throws Exception {
    final String tableName = "incrrefresh_v2_test_partition_add_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Jan') VALUES(2)";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "id", new Types.IntegerType()),
        Types.NestedField.optional(2, "month", new Types.StringType()),
        Types.NestedField.optional(3, "year", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("year", "month"), 2);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(2, "Jan", 2021)
        .baselineValues(1, "Feb", 2020)
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshPartitionAdditionFailAfterIcebergCommit() throws Exception {
    final String tableName = "incrrefresh_v2_test_partition_add_fail_after_iceberg_commit_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);

      final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
      runSQL(sql);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Jan') VALUES(2)";
      dataGenerator.executeDDL(insertCmd2);

      final String errorAfterIcebergCommit = Controls.newBuilder()
        .addException(IncrementalMetadataRefreshCommitter.class, IncrementalMetadataRefreshCommitter.INJECTOR_AFTER_ICEBERG_COMMIT_ERROR,
          UnsupportedOperationException.class)
        .build();
      ControlsInjectionUtil.setControls(client, errorAfterIcebergCommit);
      try {
        //this will fail after iceberg commit
        runSQL(sql);
        Assert.fail("expecting injected exception to be thrown");
      } catch (Exception ex) {
      }

      //Refresh the same iceberg table again
      icebergTable.refresh();
      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "id", new Types.IntegerType()),
        Types.NestedField.optional(2, "month", new Types.StringType()),
        Types.NestedField.optional(3, "year", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("year", "month"), 2);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(1, "Feb", 2020)
        .go();

      // this refresh should repair datasetconfig
      runSQL(sql);

      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(2, "Jan", 2021)
        .baselineValues(1, "Feb", 2020)
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshPartitionDeletion() throws Exception {
    final String tableName = "incrrefresh_v2_test_partition_delete_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(1)";
      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Jan') VALUES(2)";
      dataGenerator.executeDDL(insertCmd1);
      dataGenerator.executeDDL(insertCmd2);

      final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
      runSQL(sql);

      final String dropCmd1 = "ALTER TABLE " + tableName + " DROP PARTITION(year=2020, month='Feb')";
      dataGenerator.executeDDL(dropCmd1);
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "id", new Types.IntegerType()),
        Types.NestedField.optional(2, "month", new Types.StringType()),
        Types.NestedField.optional(3, "year", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 0, 1, expectedSchema, Sets.newHashSet("year", "month"), 1);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(2, "Jan", 2021)
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshSchemaEvolution() throws Exception {
    final String tableName = "incrrefresh_v2_test_schema_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();

      final String alterTable = "ALTER TABLE " + tableName + " CHANGE col1 col3 DOUBLE";
      final String insertCmd2 = "INSERT INTO " + tableName + " VALUES(2.0, 'b')";
      dataGenerator.executeDDL(alterTable);
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //col2 renamed to col3 and schema updated from int -> double and column id also changed
      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col2", new Types.StringType()),
        Types.NestedField.optional(2, "col3", new Types.DoubleType())));

      verifyMetadata(expectedSchema, 2);

    /*TODO: Uncomment this once DX-34794 is fixed
    String selectQuery = "SELECT * from " + HIVE + tableName;
    testBuilder()
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns("col2", "col3")
      .baselineValues("a", null)
      .baselineValues("b", 2.0)
      .go();

    verifyIcebergExecution(EXPLAIN_PLAN + selectQuery);*/
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshSchemaEvolWithoutDataUpdate() throws Exception {
    final String tableName = "incrrefresh_v2_test_schema_update_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      final String alterTable = "ALTER TABLE " + tableName + " ADD COLUMNS (col3 int)";
      dataGenerator.executeDDL(alterTable);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //col2 renamed to col3 and schema updated from int -> double and column id also changed
      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.IntegerType()),
        Types.NestedField.optional(2, "col2", new Types.StringType()),
        Types.NestedField.optional(3, "col3", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 0, 0, expectedSchema, new HashSet<>(),1);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(1, "a", null)
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshSchemaEvolOnPartitionTable() throws Exception {
    final String tableName = "incrrefresh_v2_test_schema_update_on_partitioned" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING) PARTITIONED BY (year INT)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020) VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      final String alterTable = "ALTER TABLE " + tableName + " ADD COLUMNS (col3 string)";
      dataGenerator.executeDDL(alterTable);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //col2 renamed to col3 and schema updated from int -> double and column id also changed
      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.IntegerType()),
        Types.NestedField.optional(2, "col2", new Types.StringType()),
        Types.NestedField.optional(3, "col3", new Types.StringType()),
        Types.NestedField.optional(4, "year", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 0, 0, expectedSchema, Sets.newHashSet("year"), 1);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "year")
        .baselineValues(1, "a", null, 2020)
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testIncrementalRefreshFailPartitionEvolution() throws Exception {
    final String tableName = "incrrefresh_v2_test_fail_partition_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(month STRING) PARTITIONED BY (year INT)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020) VALUES('Jan')";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      // Alter partition spec
      final String alterPartition = "ALTER TABLE " + tableName + " partition column (year DOUBLE)";
      dataGenerator.executeDDL(alterPartition);

      assertThatThrownBy(() -> runFullRefresh(tableName))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Change in Hive partition definition detected for table");
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testPartialRefreshExistingPartition() throws Exception {
    final String tableName = "incrrefresh_v2_test_existing_partition_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      icebergTable.refresh();
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      // inserting single row in different partitions
      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(2)";
      final String insertCmd3 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(3)";
      dataGenerator.executeDDL(insertCmd2);
      dataGenerator.executeDDL(insertCmd3);

      // this will do an partial refresh now on particular partition
      // only 1 file addition will be shown as a single partition is refreshed
      runPartialRefresh(tableName, "(\"year\" = '2020', \"month\" = 'Jan')");

      //Refresh the same iceberg table again
      icebergTable.refresh();
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, new Schema(Arrays.asList(
        Types.NestedField.optional(1, "id", new Types.IntegerType()),
        Types.NestedField.optional(2, "year", new Types.IntegerType()),
        Types.NestedField.optional(3, "month", new Types.StringType()))), Sets.newHashSet("year", "month"), 2);

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(2, "Jan", 2020)
        .baselineValues(1, "Jan", 2020)
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testPartialRefreshNewPartition() throws Exception {
    final String tableName = "incrrefresh_v2_test_new_partition_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      icebergTable.refresh();
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      // inserting single row in a new partition
      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(2)";
      dataGenerator.executeDDL(insertCmd2);

      // No file addition should happen for this partition
      runPartialRefresh(tableName, "(\"year\" = '2020', \"month\" = 'Jan')");

      //Refresh the same iceberg table again
      icebergTable.refresh();
      //No new snapshot created
      assertEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      // Do partial refresh. File addition is expected for this partition refresh
      runPartialRefresh(tableName, "(\"year\" = '2020', \"month\" = 'Feb')");

      //Refresh the same iceberg table again
      icebergTable.refresh();
      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, new Schema(Arrays.asList(
        Types.NestedField.optional(1, "id", new Types.IntegerType()),
        Types.NestedField.optional(2, "year", new Types.IntegerType()),
        Types.NestedField.optional(3, "month", new Types.StringType()))), Sets.newHashSet("year", "month"), 2);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testPartialRefreshNewPartitionFailAfterIcebergCommit() throws Exception {
    final String tableName = "incrrefresh_v2_test_new_partition_partial_refresh_restores_read_sig_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);

      final String sql = String.format(REFRESH_DATASET, HIVE + tableName);
      // this will do a full refresh first
      runSQL(sql);

      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(2)";
      dataGenerator.executeDDL(insertCmd2);
      final String insertCmd3 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Feb') VALUES(2)";
      dataGenerator.executeDDL(insertCmd3);

      final String errorAfterIcebergCommit = Controls.newBuilder()
        .addException(IncrementalMetadataRefreshCommitter.class, IncrementalMetadataRefreshCommitter.INJECTOR_AFTER_ICEBERG_COMMIT_ERROR,
          UnsupportedOperationException.class)
        .build();
      ControlsInjectionUtil.setControls(client, errorAfterIcebergCommit);
      try {
        //this will fail after iceberg commit
        runSQL(sql);
        Assert.fail("expecting injected exception to be thrown");
      } catch (Exception ex) {
      }

      final String insertCmd4 = "INSERT INTO " + tableName + " PARTITION(year=2022, month='Feb') VALUES(3)";
      dataGenerator.executeDDL(insertCmd4);

      // this should still give old results since catalog still has old icebergMetadata
      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(1, "Jan", 2020)
        .go();

      // this will fix catalog metadata and do partial refresh
      // even though this is a partial refresh on (2020, Feb), restore read signature should contain (2021, Feb) as well
      runSQL(sql + " FOR PARTITIONS (\"year\" = '2020', \"month\" = 'Feb')");

      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(1, "Jan", 2020)
        .baselineValues(2, "Feb", 2020)
        .baselineValues(2, "Feb", 2021)
        .go();

      runSQL(sql); // this will add new partition (2022, Feb)
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id", "month", "year")
        .baselineValues(1, "Jan", 2020)
        .baselineValues(2, "Feb", 2020)
        .baselineValues(2, "Feb", 2021)
        .baselineValues(3, "Feb", 2022)
        .go();
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testCheckPartitionHasPermissionInIncremental() throws Exception {
    final String tableName = "incrrefresh_v2_test_set_permission_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Feb') VALUES(1)";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      final Path partitionDir = new Path(dataGenerator.getWhDir() + "/" + tableName + "/year=2021/month=Feb");

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
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testCheckPartitionHasPermissionInPartial() throws Exception {
    final String tableName = "partialrefresh_v2_test_set_permission_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Feb') VALUES(1)";
      dataGenerator.executeDDL(insertCmd2);
      runPartialRefresh(tableName, "(\"year\" = '2021', \"month\" = 'Feb')");

      final Path partitionDir = new Path(dataGenerator.getWhDir() + "/" + tableName + "/year=2021/month=Feb");

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
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testPartialRefreshWithDifferentTypeColumns() throws Exception {
    final String tableName = "parrefresh_v2_different_partition_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) partitioned by (p_tinyint tinyint, p_smallint smallint, p_int int,p_bigint bigint," +
        " p_float float, p_double double, p_decimal decimal(20,10), p_timestamp timestamp, p_date date, p_varchar varchar(40)," +
        " p_string string, p_boolean boolean)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(p_tinyint=10, p_smallint=120, " +
        " p_int=65532, p_bigint=4543534, p_float=22.123123, p_double=4512.435423, p_decimal=124.23431231, " +
        "p_timestamp='2020-02-02 11:12:20.1', p_date='2021-05-05', p_varchar='varchar part', " +
        "p_string='string part1', p_boolean=true) VALUES(1)";

      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      dataGenerator.executeDDL(insertCmd1);
      runPartialRefresh(tableName, "(\"p_tinyint\" = '10', \"p_smallint\" = '120', \"p_int\"='65532'," +
        " \"p_bigint\"='4543534', \"p_float\"='22.123123', \"p_double\" = '4512.435423', \"p_decimal\" = '124.23431231'," +
        " \"p_timestamp\" = '2020-02-02 11:12:20.1', \"p_date\" = '2021-05-05'," +
        "\"p_varchar\" = 'varchar part', \"p_string\" = 'string part1', \"p_boolean\" = 'true')");

      String selectQuery = "SELECT id from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("id")
        .baselineValues(1)
        .baselineValues(1)
        .go();
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testPartialRefreshFailWithNoValue() throws Exception {
    final String tableName = "parrefresh_v2_fail_partition_exist_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=10, month='JAN') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=30, month='JAN') VALUES(1)";
      dataGenerator.executeDDL(insertCmd2);

      assertThatThrownBy(() -> runPartialRefresh(tableName, "(\"year\" = '20', \"month\" = 'FEB')"))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("VALIDATION ERROR: Partition 'year=20/month=FEB' does not exist in default." + tableName);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testFixedWidthVarcharCol() throws Exception {
    final String tableName = "incrrefresh_v2_test_fix_width_varchar_col_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, c_char char(20)) PARTITIONED BY (p_char char(40))");
      dataGenerator.executeDDL("INSERT INTO " + tableName + " PARTITION(p_char = 'x yyy z') values (1, 'a b cdef')");
      runFullRefresh(tableName);

      dataGenerator.executeDDL("INSERT INTO " + tableName + " PARTITION(p_char = 'o pqr k') values (1, 'a b cdef')");
      runPartialRefresh(tableName, "(\"p_char\" = 'o pqr k')");

      String selectQuery = "SELECT * from " + HIVE + tableName;
      testBuilder()
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns("col1", "c_char", "p_char")
        .baselineValues(1, "a b cdef", "x yyy z")
        .baselineValues(1, "a b cdef", "o pqr k")
        .go();

      verifyIcebergExecution(EXPLAIN_PLAN + selectQuery, finalIcebergMetadataLocation);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  @Test
  public void testPartialRefreshWithRandomOrdering() throws Exception {
    final String tableName = "parrefresh_v2_test_random_partition_order_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Jan') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      icebergTable.refresh();
      long oldSnapshotId = icebergTable.currentSnapshot().snapshotId();

      // inserting single row in a new partition
      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(2)";
      dataGenerator.executeDDL(insertCmd2);

      // Do partial refresh. File addition is expected for this partition refresh
      runPartialRefresh(tableName, "(\"month\" = 'Feb', \"year\" = '2020')");

      //Refresh the same iceberg table again
      icebergTable.refresh();
      //New snapshot created
      Assert.assertNotEquals(oldSnapshotId, icebergTable.currentSnapshot().snapshotId());

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, new Schema(Arrays.asList(
        Types.NestedField.optional(1, "id", new Types.IntegerType()),
        Types.NestedField.optional(2, "year", new Types.IntegerType()),
        Types.NestedField.optional(3, "month", new Types.StringType()))), Sets.newHashSet("year", "month"), 2);
    }
    finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  protected String getFileFormat() {
    return "PARQUET";
  }

  protected String getFileFormatLowerCase() {
    return getFileFormat().toLowerCase(Locale.ROOT);
  }

  protected void createTable(String tableName, String queryDef) throws IOException {
    String query = "CREATE TABLE IF NOT EXISTS " + tableName + queryDef + " STORED AS " + getFileFormat();
    System.out.println("query = " + query);
    dataGenerator.executeDDL(query);
  }

  protected void dropTable(String tableName) throws IOException {
    dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
  }

  protected Table getIcebergTable() {
    return RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
  }

  protected void verifyMetadata(Schema expectedSchema, int expectedFileCount) {
    verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, new HashSet<>(), expectedFileCount);
  }

  protected void forgetMetadata(String tableName) throws Exception {
    runSQL(String.format(FORGET_DATASET, HIVE + tableName));
  }

  protected void runFullRefresh(String tableName) throws Exception {
    runRefresh(String.format(ALTER_PDS_REFRESH, HIVE + tableName), String.format(REFRESH_DONE, HIVE + tableName));
  }

  protected void runFullRefreshExpectingNoChange(String tableName) throws Exception {
    runRefresh(String.format(ALTER_PDS_REFRESH, HIVE + tableName), String.format(REFRESH_NOT_DONE, HIVE + tableName));
  }

  protected void runPartialRefresh(String tableName, String partitionColumnFilters) throws Exception {
    runRefresh(String.format(ALTER_PDS_REFRESH + " FOR PARTITIONS %s", HIVE + tableName, partitionColumnFilters), String.format(REFRESH_DONE, HIVE + tableName));
  }

  private void runRefresh(String query, String expectedMsg) throws Exception {
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, expectedMsg)
      .build()
      .run();
  }
}
