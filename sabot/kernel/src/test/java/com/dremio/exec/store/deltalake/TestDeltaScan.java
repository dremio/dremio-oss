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

package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertTrue;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.SabotContext;
import com.google.common.io.Resources;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestDeltaScan extends PlanTestBase {

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(80, TimeUnit.SECONDS);

  static FileSystem fs;
  static String testRootPath = "/tmp/deltalake/";
  static Configuration conf;
  static SabotContext sabotContext;

  @BeforeClass
  public static void initFs() throws Exception {
    sabotContext = getSabotContext();
    conf = new Configuration();
    conf.set("fs.default.name", "local");
    fs = FileSystem.get(conf);
    Path p = new Path(testRootPath);

    if (fs.exists(p)) {
      fs.delete(p, true);
    }

    fs.mkdirs(p);
    copyFromJar("deltalake/testDataset", java.nio.file.Paths.get(testRootPath + "/testDataset"));
    copyFromJar("deltalake/JsonDataset", java.nio.file.Paths.get(testRootPath + "/JsonDataset"));
    copyFromJar(
        "deltalake/lastCheckpointDataset",
        java.nio.file.Paths.get(testRootPath + "/lastCheckpointDataset"));
    copyFromJar(
        "deltalake/emptyDataFilesNoStatsParsed",
        java.nio.file.Paths.get(testRootPath + "/emptyDataFilesNoStatsParsed"));
    copyFromJar(
        "deltalake/extraAttrsRemovePath",
        java.nio.file.Paths.get(testRootPath + "/extraAttrsRemovePath"));
    copyFromJar(
        "deltalake/multibatchCheckpointWithRemove",
        java.nio.file.Paths.get(testRootPath + "/multibatchCheckpointWithRemove"));
    copyFromJar(
        "deltalake/multiPartCheckpoint",
        java.nio.file.Paths.get(testRootPath + "/multiPartCheckpoint"));
    copyFromJar(
        "deltalake/multi_partitioned_remove_only_checkpoint",
        java.nio.file.Paths.get(testRootPath + "/multi_partitioned_remove_only_checkpoint"));
    copyFromJar(
        "deltalake/repartitioned", java.nio.file.Paths.get(testRootPath + "/repartitioned"));
    copyFromJar(
        "deltalake/schema_change_partition",
        java.nio.file.Paths.get(testRootPath + "/schema_change_partition"));
    copyFromJar("deltalake/newPlanDataset", java.nio.file.Paths.get(testRootPath + "/newDataset"));
    copyFromJar(
        "deltalake/paritionenedNewPlan",
        java.nio.file.Paths.get(testRootPath + "/paritionenedNewPlan"));
    copyFromJar(
        "deltalake/commitInfoAtOnlyJson",
        java.nio.file.Paths.get(testRootPath + "/commitInfoAtOnlyJson"));
    copyFromJar(
        "deltalake/deltaMixCharsName",
        java.nio.file.Paths.get(testRootPath + "/deltaMixCharsName"));
    copyFromJar(
        "deltalake/test_2k_cols_checkpoint",
        java.nio.file.Paths.get(testRootPath + "/test_2k_cols_checkpoint"));
    copyFromJar(
        "deltalake/test_2k_cols_json",
        java.nio.file.Paths.get(testRootPath + "/test_2k_cols_json"));
    copyFromJar(
        "deltalake/test_long_cols_checkpoint",
        java.nio.file.Paths.get(testRootPath + "/test_long_cols_checkpoint"));
    copyFromJar(
        "deltalake/test_long_cols_json",
        java.nio.file.Paths.get(testRootPath + "/test_long_cols_json"));
    copyFromJar(
        "deltalake/columnMapping", java.nio.file.Paths.get(testRootPath + "/columnMapping"));
    copyFromJar(
        "deltalake/columnMappingComplexTypes",
        java.nio.file.Paths.get(testRootPath + "/columnMappingComplexTypes"));
    copyFromJar(
        "deltalake/columnMappingNestedTypes",
        java.nio.file.Paths.get(testRootPath + "/columnMappingNestedTypes"));
    copyFromJar(
        "deltalake/columnMappingConvertedIceberg",
        java.nio.file.Paths.get(testRootPath + "/columnMappingConvertedIceberg"));
    copyFromJar(
        "deltalake/columnMappingPartitions",
        java.nio.file.Paths.get(testRootPath + "/columnMappingPartitions"));
  }

  @AfterClass
  public static void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
  }

  @Test
  public void testDeltaScanCount() throws Exception {
    final String sql = "select count(*) as cnt from dfs.tmp.deltalake.testDataset";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(499L)
        .unOrdered()
        .build()
        .run();
  }

  @Test
  public void testDeltaLakeSelect() throws Exception {
    final String sql =
        "SELECT id, iso_code, continent FROM dfs.tmp.deltalake.testDataset order by id limit 2;";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id", "iso_code", "continent")
        .baselineValues(1L, "AFG", "Asia")
        .baselineValues(2L, "AFG", "Asia")
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaLakeGroupBy() throws Exception {
    final String sql =
        "SELECT SUM(cast(new_cases as DECIMAL)) as cases FROM dfs.tmp.deltalake.testDataset group by continent;";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cases")
        .baselineValues(new BigDecimal(45140))
        .baselineValues(new BigDecimal(23433))
        .baselineValues(new BigDecimal(25674))
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaLakeSnapshot() throws Exception {
    testDeltaLakeSnapshot("testDataset");
    testDeltaLakeSnapshot("JsonDataset");
  }

  private void testDeltaLakeSnapshot(String tableName) throws Exception {
    String testTablePath = testRootPath + "/" + tableName;
    DeltaTableIntegrationTestUtils testUtils =
        new DeltaTableIntegrationTestUtils(conf, testTablePath);

    promoteTable(tableName);

    final String sql = createTableFunctionQuery(tableName, "table_snapshot");
    TestBuilder testBuilder =
        testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns(
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary");
    testUtils
        .getHistory()
        .forEach(
            commit ->
                testBuilder.baselineValues(
                    new LocalDateTime(commit.getTimestamp().getTime(), DateTimeZone.UTC),
                    commit.getVersion().orElse(0L),
                    null,
                    commit.getOperation(),
                    null,
                    null));
    testBuilder.go();
  }

  private void promoteTable(String tableName) throws Exception {
    test("SELECT count(*) from dfs.tmp.deltalake." + tableName);
  }

  private String createTableFunctionQuery(String tableName, String tableFunction) {
    return "SELECT * FROM TABLE(" + tableFunction + "('dfs.tmp.deltalake." + tableName + "'));";
  }

  @Test
  public void testDeltaLakeHistory() throws Exception {
    testDeltaLakeHistory("testDataset");
    testDeltaLakeHistory("JsonDataset");
  }

  private void testDeltaLakeHistory(String tableName) throws Exception {
    String testTablePath = testRootPath + "/" + tableName;
    DeltaTableIntegrationTestUtils testUtils =
        new DeltaTableIntegrationTestUtils(conf, testTablePath);

    promoteTable(tableName);

    final String sql = createTableFunctionQuery(tableName, "table_history");
    TestBuilder testBuilder =
        testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("made_current_at", "snapshot_id", "parent_id", "is_current_ancestor");
    testUtils
        .getHistory()
        .forEach(
            commit ->
                testBuilder.baselineValues(
                    new LocalDateTime(commit.getTimestamp().getTime(), DateTimeZone.UTC),
                    commit.getVersion().orElse(0L),
                    null,
                    null));
    testBuilder.go();
  }

  @Test
  public void testDeltalakeFunctionsDistributionPlan() throws Exception {
    try (AutoCloseable ac =
        withSystemOption(PlannerSettings.DELTALAKE_HISTORY_SCAN_FILES_PER_THREAD, 5L)) {
      testDeltalakeFunctionsDistributionPlan("testDataset", "table_snapshot");
      testDeltalakeFunctionsDistributionPlan("testDataset", "table_history");
      testDeltalakeFunctionsDistributionPlan("JsonDataset", "table_snapshot");
      testDeltalakeFunctionsDistributionPlan("JsonDataset", "table_history");
    }
  }

  private void testDeltalakeFunctionsDistributionPlan(String tableName, String tableFunction)
      throws Exception {
    promoteTable(tableName);

    final String query = createTableFunctionQuery(tableName, tableFunction);
    testPlanMatchingPatterns(
        query,
        new String[] {
          "(?s)"
              + "UnionExchange.*"
              + "Project.*"
              + "Project.*"
              + "TableFunction.*"
              + "RoundRobinExchange.*"
              + "DirListingScan.*"
        });
  }

  @Test
  public void testDeltalakeJsonDataset() throws Exception {
    final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.JsonDataset;";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(89L)
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltalakeJsonDatasetSelect() throws Exception {
    testDeltalakeJsonCommitsDataset();
  }

  @Test
  public void testDeltalakeJsonReadNumbersAsDoubleOptionEnabled() throws Exception {
    try (AutoCloseable ac = enableJsonReadNumbersAsDouble()) {
      testDeltalakeJsonCommitsDataset();
    }
  }

  @Test
  public void testDeltalakeJsonReadAllAsTextEnabled() throws Exception {
    try (AutoCloseable ac = enableJsonAllStrings()) {
      testDeltalakeJsonCommitsDataset();
    }
  }

  private void testDeltalakeJsonCommitsDataset() throws Exception {
    final String sql = "SELECT id, new_cases FROM dfs.tmp.deltalake.JsonDataset limit 3";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id", "new_cases")
        .baselineValues(71L, "45.0")
        .baselineValues(72L, "150.0")
        .baselineValues(73L, "116.0")
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaLakeCheckpointDatasetCount() throws Exception {
    final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.lastCheckpointDataset";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(109L)
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaLakeCheckpointDatasetSelect() throws Exception {
    final String sql =
        "SELECT id, total_cases FROM dfs.tmp.deltalake.lastCheckpointDataset order by total_cases limit 2";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id", "total_cases")
        .baselineValues(11L, "1027.0")
        .baselineValues(54L, "1050.0")
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaLakeEmptyDataFilesNoStatsParsed() throws Exception {
    /*
     * 1. The data files within the dataset doesn't even have a rowgroup written.
     * 2. The checkpoint parquet contains "stats" but doesn't contain "stats_parsed" field.
     */
    final String sql = "SELECT * FROM dfs.tmp.deltalake.emptyDataFilesNoStatsParsed";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id", "total_cases")
        .expectsEmptyResultSet()
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaLakeRemoveEntryExtraAttrs() throws Exception {
    /*
     * "remove" contains extra attributes such as extendedFileMetadata,partitionValues,size. Dremio should run irrespective
     * of any unrecognizable attributes
     */
    final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.extraAttrsRemovePath";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(2L)
        .unOrdered()
        .go();
  }

  @Test
  public void testMultiBatchCheckpointWithRemoveEntries() throws Exception {
    /*
     * The dataset contains only a checkpoint reference, containing 700 add paths, which cross batch size 500, followed
     * by 500 remove paths and metaData and protocolVersion only in the end.
     */
    final String sql =
        "SELECT intcol, longcol FROM dfs.tmp.deltalake.multibatchCheckpointWithRemove limit 1";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("intcol", "longcol")
        .baselineValues(6097811, 6097811L)
        .unOrdered()
        .go();
  }

  @Test
  public void testMultiRGCheckpointParquet() throws Exception {
    /*
     * The dataset contains a checkpoint with multiple rowgroups. The data files referenced include added as well as
     * remove entries.
     * Since number of entries referenced from the checkpoint parquet are large, this test-case duplicates the same
     * data file.
     */
    // setup
    final String deltaDirectory = testRootPath + "/testMultiRGCheckpointParquetDeltaDataset";
    final String deltaLogDirectory = deltaDirectory + "/_delta_log";
    fs.mkdirs(new Path(deltaLogDirectory));

    final BiConsumer<String, String> cp =
        (src, dest) -> {
          try {
            final String srcBase = "deltalake/checkpoint_multi_rowgroups_with_remove/";
            Files.copy(Paths.get(Resources.getResource(srcBase + src).toURI()), Paths.get(dest));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    cp.accept(
        "checkpoint_multi_rg.parquet",
        deltaLogDirectory + "/00000000000000000010.checkpoint.parquet");
    cp.accept("_last_checkpoint", deltaLogDirectory + "/_last_checkpoint");
    IntStream.range(0, 700)
        .forEach(i -> cp.accept("datafile.parquet", deltaDirectory + "/testme" + i + ".parquet"));

    // promote and execute
    final String sql =
        "SELECT count(*) as cnt FROM dfs.tmp.deltalake.testMultiRGCheckpointParquetDeltaDataset";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(7000L)
        .unOrdered()
        .go();
  }

  /*Tests weather dedup happens if there multiple add commits for the same file on a
  non-paritioned delta dataset
  * */
  @Test
  public void testMultipleAddCommitsForSameFile() throws Exception {
    String sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.newDataset";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(170L)
        .unOrdered()
        .go();

    // Added once and removed once
    sql = "SELECT id from dfs.tmp.deltalake.newDataset where id % 10 = 0";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id")
        .expectsEmptyResultSet()
        .unOrdered()
        .go();

    // Added multiple times.
    sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.newDataset where id > 160 and id < 170";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(4L)
        .unOrdered()
        .go();

    // Added multiple times and then removed
    sql = "SELECT id from dfs.tmp.deltalake.newDataset where id > 180 and id < 190";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id")
        .expectsEmptyResultSet()
        .unOrdered()
        .go();
  }

  /*Tests weather dedup happens if there multiple add commits for the same file on a
   paritioned delta dataset
  * */
  @Test
  public void testMultipleAddCommitsForSameFilePartitioned() throws Exception {
    String sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(40L)
        .unOrdered()
        .go();

    // Ocenia is once added once removed
    sql =
        "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan where continent = 'Oceania'";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .unOrdered()
        .go();

    // Africa is added multiple times
    sql =
        "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan where continent = 'Africa'";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(10L)
        .unOrdered()
        .go();

    // South America is added mutiple times but then is removed
    sql =
        "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan where continent = 'South America'";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .unOrdered()
        .go();
  }

  @Test
  public void testMultiPartitionedCheckpointNoAddValsNoParsed() throws Exception {
    /*
     * Checkpoint contains only remove, metaData and protocol entries. No add entries.
     * Dataset is partitioned, using partitionValues field to read - no partitionValues_parsed.
     */
    final String sql = "SELECT * FROM dfs.tmp.deltalake.multi_partitioned_remove_only_checkpoint";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("col1", "pcol1", "pcol2")
        .expectsEmptyResultSet()
        .unOrdered()
        .go();
  }

  @Test
  public void testRepartitionedOnceAfterCheckpoint() throws Exception {
    final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.repartitioned";
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("cnt").baselineValues(10L).go();
  }

  @Test
  public void testDatasetWithPartitionAndSchemaChange() throws Exception {
    final String sql = "SELECT count(*) cnt FROM dfs.tmp.deltalake.schema_change_partition";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(2L)
        .unOrdered()
        .go();
  }

  @Test
  public void testDatasetWithMultiPartCheckpointParquet() throws Exception {
    final String sql = "SELECT intcol, longcol FROM dfs.tmp.deltalake.multiPartCheckpoint limit 1";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("intcol", "longcol")
        .baselineValues(2450811, 2450811L)
        .unOrdered()
        .go();
  }

  @Test
  @Ignore("DX-50441")
  public void testWithCommitInfoAtEndOnlyJson() throws Exception {
    final String sql = "SELECT * FROM dfs.tmp.deltalake.commitInfoAtOnlyJson order by id limit 3";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id")
        .baselineValues(0L)
        .baselineValues(1L)
        .baselineValues(2L)
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaFileWithPlusSign() throws Exception {
    final String sql = "SELECT \"c1+c2/c3\" as col1 FROM dfs.tmp.deltalake.deltaMixCharsName";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("a b+c")
        .baselineValues("a=b")
        .baselineValues("a?b%c")
        .unOrdered()
        .go();
  }

  @Test
  public void testDeltaWithLongSchemaCheckpoint() throws Exception {
    testDeltaWithLongSchemaCheckpoint("test_2k_cols_checkpoint");
    testDeltaWithLongSchemaCheckpoint("test_long_cols_checkpoint");
  }

  private void testDeltaWithLongSchemaCheckpoint(String tableName) throws Exception {
    com.dremio.io.file.Path checkpointPath =
        com.dremio.io.file.Path.of(testRootPath)
            .resolve(tableName)
            .resolve(DeltaConstants.DELTA_LOG_DIR)
            .resolve("00000000000000000010.checkpoint.parquet");
    com.dremio.io.file.FileSystem dremioFs = HadoopFileSystem.get(fs);
    DeltaLogCheckpointParquetReader reader = new DeltaLogCheckpointParquetReader();
    DeltaLogSnapshot snapshot =
        reader.parseMetadata(
            null,
            sabotContext,
            dremioFs,
            Collections.singletonList(dremioFs.getFileAttributes(checkpointPath)),
            10);
    assertTrue(
        snapshot.getSchema().length()
            > sabotContext.getOptionManager().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));

    final String sql = "select count(*) cnt from dfs.tmp.deltalake." + tableName;
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("cnt").baselineValues(23L).go();
  }

  @Test
  public void testDeltaWithLongSchemaJsonCommit() throws Exception {
    testDeltaWithLongSchemaJsonCommit("test_2k_cols_json");
    testDeltaWithLongSchemaJsonCommit("test_long_cols_json");
  }

  private void testDeltaWithLongSchemaJsonCommit(String tableName) throws Exception {
    com.dremio.io.file.Path commitPath =
        com.dremio.io.file.Path.of(testRootPath)
            .resolve(tableName)
            .resolve(DeltaConstants.DELTA_LOG_DIR)
            .resolve("00000000000000000000.json");
    com.dremio.io.file.FileSystem dremioFs = HadoopFileSystem.get(fs);
    DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
    DeltaLogSnapshot snapshot =
        jsonReader.parseMetadata(
            null,
            sabotContext,
            dremioFs,
            Collections.singletonList(dremioFs.getFileAttributes(commitPath)),
            0);
    assertTrue(
        snapshot.getSchema().length()
            > sabotContext.getOptionManager().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));

    final String sql = "select count(*) cnt from dfs.tmp.deltalake." + tableName;
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("cnt").baselineValues(5L).go();
  }

  @Test
  public void testColumnMapping() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      final String sql = "SELECT * FROM dfs.tmp.deltalake.columnMapping";
      testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("c_int", "c_str2")
          .baselineValues(1, "a")
          .baselineValues(2, "b")
          .baselineValues(3, "c")
          .baselineValues(4, "d")
          .go();
    }
  }

  @Test
  public void testColumnMappingComplexTypes() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      // id                  int
      // c_array             array<string>
      // c_map               map<string,int>
      // c_struct            struct<i:int,s:string>
      final String sql =
          "SELECT id, c_array[0] as a0, c_array[1] as a1, c_map['a'] as ma, c_map['b'] as mb, c_map['c'] as mc, c_struct['i'] as si, c_struct['s'] as ss FROM dfs.tmp.deltalake.columnMappingComplexTypes";
      testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("id", "a0", "a1", "ma", "mb", "mc", "si", "ss")
          .baselineValues(1, "a", "b", 11, 12, null, 1, "a")
          .baselineValues(2, "c", "d", 21, 22, null, 2, "c")
          .baselineValues(3, "e", "f", 31, 32, null, 3, "e")
          .baselineValues(4, "g", "h", 41, null, 42, 4, "g")
          .go();
    }
  }

  @Test
  public void testColumnMappingNestedTypes() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      // id                  int
      // c_array             array<map<string,int>>
      // c_map               map<string,int>
      // c_struct            array<struct<i:int,s:string>>
      final String sql =
          "SELECT id, c_array[0]['b'] as c_array, c_map['a'] as c_map, c_struct[0]['i'] as c_struct FROM dfs.tmp.deltalake.columnMappingNestedTypes";
      testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("id", "c_array", "c_map", "c_struct")
          .baselineValues(1, 12, 11, 1)
          .baselineValues(2, 22, 21, 2)
          .go();
    }
  }

  @Test
  public void testColumnMappingConvertedIceberg() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      // converted table has id-based column mapping
      final String sql = "SELECT * FROM dfs.tmp.deltalake.columnMappingConvertedIceberg";
      testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("id", "str")
          .baselineValues(1, "ab")
          .baselineValues(2, "cd")
          .baselineValues(3, "ef")
          .baselineValues(4, "gh")
          .go();
    }
  }

  @Test
  public void testColumnMappingWithPartitions() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      final String sql = "SELECT * FROM dfs.tmp.deltalake.columnMappingPartitions";
      testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("id", "str", "key")
          .baselineValues(1, "a", 10)
          .baselineValues(2, "b", 10)
          .baselineValues(3, "c", 20)
          .baselineValues(4, "d", 20)
          .go();
    }
  }
}
