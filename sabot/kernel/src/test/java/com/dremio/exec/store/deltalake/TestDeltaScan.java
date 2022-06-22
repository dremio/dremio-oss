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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.google.common.io.Resources;

public class TestDeltaScan extends BaseTestQuery {

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(80, TimeUnit.SECONDS);

  FileSystem fs;
  static String testRootPath = "/tmp/deltalake/";
  static Configuration conf;

  @Before
  public void initFs() throws Exception {
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
    copyFromJar("deltalake/lastCheckpointDataset", java.nio.file.Paths.get(testRootPath + "/lastCheckpointDataset"));
    copyFromJar("deltalake/emptyDataFilesNoStatsParsed", java.nio.file.Paths.get(testRootPath + "/emptyDataFilesNoStatsParsed"));
    copyFromJar("deltalake/extraAttrsRemovePath", java.nio.file.Paths.get(testRootPath + "/extraAttrsRemovePath"));
    copyFromJar("deltalake/multibatchCheckpointWithRemove", java.nio.file.Paths.get(testRootPath + "/multibatchCheckpointWithRemove"));
    copyFromJar("deltalake/multiPartCheckpoint", java.nio.file.Paths.get((testRootPath + "/multiPartCheckpoint")));
    copyFromJar("deltalake/multi_partitioned_remove_only_checkpoint", java.nio.file.Paths.get(testRootPath + "/multi_partitioned_remove_only_checkpoint"));
    copyFromJar("deltalake/repartitioned", java.nio.file.Paths.get(testRootPath + "/repartitioned"));
    copyFromJar("deltalake/schema_change_partition", java.nio.file.Paths.get(testRootPath + "/schema_change_partition"));
    copyFromJar("deltalake/newPlanDataset", java.nio.file.Paths.get((testRootPath + "/newDataset")));
    copyFromJar("deltalake/paritionenedNewPlan", java.nio.file.Paths.get((testRootPath + "/paritionenedNewPlan")));
    copyFromJar("deltalake/commitInfoAtOnlyJson", java.nio.file.Paths.get((testRootPath + "/commitInfoAtOnlyJson")));
  }

  @After
  public void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
  }

  private void copy(java.nio.file.Path source, java.nio.file.Path dest) {
    try {
      Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private void copyFromJar(String sourceElement, final java.nio.file.Path target) throws URISyntaxException, IOException {
    URI resource = Resources.getResource(sourceElement).toURI();
    java.nio.file.Path srcDir = java.nio.file.Paths.get(resource);
    Files.walk(srcDir)
            .forEach(source -> copy(source, target.resolve(srcDir.relativize(source))));
  }

  @Test
  public void testDeltaScanCount() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
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
  }

  @Test
  public void testDeltaLakeSelect() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT id, iso_code, continent FROM dfs.tmp.deltalake.testDataset order by id limit 2;";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("id", "iso_code", "continent")
              .baselineValues(1L, "AFG", "Asia")
              .baselineValues(2L, "AFG", "Asia")
              .unOrdered().go();
    }
  }

  @Test
  public void testDeltaLakeGroupBy() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT SUM(cast(new_cases as DECIMAL)) as cases FROM dfs.tmp.deltalake.testDataset group by continent;";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("cases")
              .baselineValues(new BigDecimal(45140))
              .baselineValues(new BigDecimal(23433))
              .baselineValues(new BigDecimal(25674))
              .unOrdered().go();
    }
  }

  @Test
  public void testDeltalakeJsonDataset() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.JsonDataset;";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("cnt")
              .baselineValues(89L)
              .unOrdered().go();
    }
  }

  @Test
  public void testDeltalakeJsonDatasetSelect() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      testDeltalakeJsonCommitsDataset();
    }
  }

  @Test
  public void testDeltalakeJsonReadNumbersAsDoubleOptionEnabled() throws Exception {
    try (AutoCloseable c = enableDeltaLake();
         AutoCloseable c2 = enableJsonReadNumbersAsDouble()) {
      testDeltalakeJsonCommitsDataset();
    }
  }

  @Test
  public void testDeltalakeJsonReadAllAsTextEnabled() throws Exception {
    try (AutoCloseable c = enableDeltaLake();
         AutoCloseable c2 = enableJsonAllStrings()) {
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
            .unOrdered().go();
  }

  @Test
  public void testDeltaLakeCheckpointDatasetCount() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.lastCheckpointDataset";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("cnt")
              .baselineValues(109L)
              .unOrdered().go();
    }
  }

  @Test
  public void testDeltaLakeCheckpointDatasetSelect() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT id, total_cases FROM dfs.tmp.deltalake.lastCheckpointDataset order by total_cases limit 2";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("id", "total_cases")
              .baselineValues(11L, "1027.0")
              .baselineValues(54L, "1050.0")
              .unOrdered().go();
    }
  }

  @Test
  public void testDeltaLakeEmptyDataFilesNoStatsParsed() throws Exception {
    /*
     * 1. The data files within the dataset doesn't even have a rowgroup written.
     * 2. The checkpoint parquet contains "stats" but doesn't contain "stats_parsed" field.
     */
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT * FROM dfs.tmp.deltalake.emptyDataFilesNoStatsParsed";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("id", "total_cases")
              .expectsEmptyResultSet()
              .unOrdered().go();
    }
  }

  @Test
  public void testDeltaLakeRemoveEntryExtraAttrs() throws Exception {
    /*
     * "remove" contains extra attributes such as extendedFileMetadata,partitionValues,size. Dremio should run irrespective
     * of any unrecognizable attributes
     */
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.extraAttrsRemovePath";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("cnt")
              .baselineValues(2L)
              .unOrdered().go();
    }
  }

  @Test
  public void testMultiBatchCheckpointWithRemoveEntries() throws Exception {
    /*
     * The dataset contains only a checkpoint reference, containing 700 add paths, which cross batch size 500, followed
     * by 500 remove paths and metaData and protocolVersion only in the end.
     */
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT intcol, longcol FROM dfs.tmp.deltalake.multibatchCheckpointWithRemove limit 1";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("intcol", "longcol")
              .baselineValues(6097811, 6097811L)
              .unOrdered().go();
    }
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

    final BiConsumer<String, String> cp = (src, dest) -> {
      try {
        final String srcBase = "deltalake/checkpoint_multi_rowgroups_with_remove/";
        final java.nio.file.Path srcPath = Paths.get(Resources.getResource(srcBase + src).toURI());
        Files.copy(srcPath, Paths.get(dest));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    cp.accept("checkpoint_multi_rg.parquet", deltaLogDirectory + "/00000000000000000010.checkpoint.parquet");
    cp.accept("_last_checkpoint", deltaLogDirectory + "/_last_checkpoint");
    IntStream.range(0, 700).forEach(i -> cp.accept("datafile.parquet", deltaDirectory + "/testme" + i + ".parquet"));

    // promote and execute
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.testMultiRGCheckpointParquetDeltaDataset";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("cnt")
              .baselineValues(7000L)
              .unOrdered().go();
    }
  }

  /*Tests weather dedup happens if there multiple add commits for the same file on a
  non-paritioned delta dataset
  * */
  @Test
  public void testMultipleAddCommitsForSameFile() throws Exception{
    try (AutoCloseable c = enableDeltaLake()) {
      String sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.newDataset";
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(170L)
        .unOrdered().go();

      //Added once and removed once
      sql = "SELECT id from dfs.tmp.deltalake.newDataset where id % 10 = 0";

      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id")
        .expectsEmptyResultSet()
        .unOrdered().go();

      //Added multiple times.
      sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.newDataset where id > 160 and id < 170";

      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(4L)
        .unOrdered().go();

      //Added multiple times and then removed
      sql = "SELECT id from dfs.tmp.deltalake.newDataset where id > 180 and id < 190";

      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id")
        .expectsEmptyResultSet()
        .unOrdered().go();
    }
  }

  /*Tests weather dedup happens if there multiple add commits for the same file on a
   paritioned delta dataset
  * */
  @Test
  public void testMultipleAddCommitsForSameFilePartitioned() throws Exception{
    try (AutoCloseable c = enableDeltaLake()) {
      String sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan";
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(40L)
        .unOrdered().go();

      //Ocenia is once added once removed
      sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan where continent = 'Oceania'";

      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .unOrdered().go();

      //Africa is added multiple times
      sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan where continent = 'Africa'";

      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(10L)
        .unOrdered().go();

      //South America is added mutiple times but then is removed
      sql = "SELECT count(*) as cnt from dfs.tmp.deltalake.paritionenedNewPlan where continent = 'South America'";

      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .unOrdered().go();
    }
  }
  @Test
  public void testMultiPartitionedCheckpointNoAddValsNoParsed() throws Exception {
    /*
     * Checkpoint contains only remove, metaData and protocol entries. No add entries.
     * Dataset is partitioned, using partitionValues field to read - no partitionValues_parsed.
     */
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT * FROM dfs.tmp.deltalake.multi_partitioned_remove_only_checkpoint";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("col1", "pcol1", "pcol2")
              .expectsEmptyResultSet()
              .unOrdered().go();
    }
  }

  @Test
  public void testRepartitionedOnceAfterCheckpoint() throws Exception {
    // Partition columns are different in checkpoint and in later commit log json.
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT count(*) as cnt FROM dfs.tmp.deltalake.repartitioned";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("cnt")
              .baselineValues(10L)
              .unOrdered().go();
      fail("Expected to fail");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Different partitions detected across the commits. Dremio doesn't support scan on a repartitioned table."));
    }
  }

  @Test
  public void testDatasetWithPartitionAndSchemaChange() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT count(*) cnt FROM dfs.tmp.deltalake.schema_change_partition";
      testBuilder()
              .sqlQuery(sql)
              .unOrdered()
              .baselineColumns("cnt")
              .baselineValues(2L)
              .unOrdered().go();
    }
  }

  @Test
  public void testDatasetWithMultiPartCheckpointParquet() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT intcol, longcol FROM dfs.tmp.deltalake.multiPartCheckpoint limit 1";
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("intcol","longcol")
        .baselineValues(2450811, 2450811L)
        .unOrdered().go();
    }
  }


  @Test
  @Ignore("DX-50441")
  public void testWithCommitInfoAtEndOnlyJson() throws Exception {
    try (AutoCloseable c = enableDeltaLake()) {
      final String sql = "SELECT * FROM dfs.tmp.deltalake.commitInfoAtOnlyJson order by id limit 3";
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id")
        .baselineValues(0l)
        .baselineValues(1l)
        .baselineValues(2l)
        .unOrdered().go();
    }
  }
}
