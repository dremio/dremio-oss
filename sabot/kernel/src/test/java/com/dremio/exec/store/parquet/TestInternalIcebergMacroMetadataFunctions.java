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

package com.dremio.exec.store.parquet;

import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.fsDelete;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * functions defined in MetadataFunctionsMacro.java are no longer supported for Internal Iceberg
 * Tables Examples of all Table Functions No longer Supported:
 *
 * <p>SELECT * FROM TABLE(TABLE_HISTORY('_path_'));
 *
 * <p>SELECT * FROM TABLE(TABLE_MANIFESTS('_path_'));
 *
 * <p>SELECT * FROM TABLE(TABLE_SNAPSHOT('_path_'));
 *
 * <p>SELECT * FROM TABLE(TABLE_FILES('_path_'));
 *
 * <p>SELECT * FROM TABLE(TABLE_PARTITIONS('_path_'));
 *
 * <p>All methods expect an exception. Proof of query success - if feature is ever turned back on -
 * is not guaranteed
 */
public class TestInternalIcebergMacroMetadataFunctions extends BaseTestQuery {

  private static FileSystem fs;
  static String testRootPath = "/tmp/metadatarefresh/";
  static String finalIcebergMetadataLocation;

  @BeforeClass
  public static void setupIcebergMetadataLocation() {
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
  }

  @Before
  public void initFs() throws Exception {
    fs = setupLocalFS();
    Path p = new Path(testRootPath);

    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    fs.mkdirs(p);

    copyFromJar("metadatarefresh/onlyFull", java.nio.file.Paths.get(testRootPath + "/onlyFull"));
    copyFromJar(
        "metadatarefresh/incrementalRefresh",
        java.nio.file.Paths.get(testRootPath + "/incrementalRefresh"));
  }

  @After
  public void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);

    // resets KV store to prevent collision of methods using the same metadata
    final Properties properties = cloneDefaultTestConfigProperties();
    updateClient(properties);
  }

  @AfterClass
  public static void reset() throws IOException {
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
  }

  /** ensures the "table_partition" function is not supported on internal Iceberg tables */
  @Test
  public void testPartitionFunction() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull refresh metadata";

    runSQL(sql);
    Table icebergTable = loadIcebergTable(finalIcebergMetadataLocation);

    assertThatThrownBy(
            () -> {
              testBuilder()
                  .ordered()
                  .sqlQuery(
                      "select count(*) as cnt from table(table_partitions('dfs.tmp.metadatarefresh.onlyFull'))")
                  .baselineColumns("cnt")
                  .baselineValues(Long.valueOf(icebergTable.history().size()))
                  .go();
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining(
            "Metadata function ('table_partitions') is not supported on table '[dfs, tmp, metadatarefresh, onlyFull]'");
  }

  /** ensures the "table_files" function is not supported on internal Iceberg tables */
  @Test
  public void testTableFilesFunction() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.incrementalRefresh refresh metadata";

    runSQL(sql);
    String selectTableFilesQuery =
        String.format(
            "SELECT count(file_path) as cnt FROM table(table_files('dfs.tmp.metadatarefresh.incrementalRefresh'))");

    assertThatThrownBy(
            () -> {
              testBuilder()
                  .sqlQuery(selectTableFilesQuery)
                  .unOrdered()
                  .baselineColumns("cnt")
                  .baselineValues(1L)
                  .build()
                  .run();
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining(
            "Metadata function ('table_files') is not supported on table '[dfs, tmp, metadatarefresh, incrementalRefresh]'");
  }

  /** ensures the "table_history" function is not supported on internal Iceberg tables */
  @Test
  public void testHistoryFunction() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull refresh metadata";

    runSQL(sql);
    Table icebergTable = loadIcebergTable(finalIcebergMetadataLocation);
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(ImmutableMap.of("`snapshot_id`", icebergTable.history().get(0).snapshotId()));

    assertThatThrownBy(
            () -> {
              testBuilder()
                  .ordered()
                  .sqlQuery(
                      "select count(*) as cnt from table(table_history('dfs.tmp.metadatarefresh.onlyFull'))")
                  .baselineColumns("cnt")
                  .baselineValues(Long.valueOf(icebergTable.history().size()))
                  .go();
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining(
            "Metadata function ('table_history') is not supported on table '[dfs, tmp, metadatarefresh, onlyFull]'");
  }

  /** ensures the "table_snapshot" function is not supported on internal Iceberg tables */
  @Test
  public void testSnapshotFunction() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull refresh metadata";

    runSQL(sql);
    Table icebergTable = loadIcebergTable(finalIcebergMetadataLocation);
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    Iterable<Snapshot> snapshots = icebergTable.snapshots();
    Snapshot snapshot1 = snapshots.iterator().next();

    Map<String, Object> mutableMap = new HashMap<>();
    mutableMap.put("`snapshot_id`", snapshot1.snapshotId());
    mutableMap.put("`parent_id`", snapshot1.parentId());
    mutableMap.put("`operation`", snapshot1.operation());
    mutableMap.put("`manifest_list`", snapshot1.manifestListLocation());

    Map<String, Object> unmodifiableMap = Collections.unmodifiableMap(mutableMap);

    recordBuilder.add(new Map[] {unmodifiableMap});

    assertThatThrownBy(
            () -> {
              testBuilder()
                  .ordered()
                  .sqlQuery(
                      "SELECT parent_id,snapshot_id,operation,manifest_list FROM TABLE(table_snapshot('dfs.tmp.metadatarefresh.onlyFull'))")
                  .baselineColumns(
                      "committed_at", "parent_id", "snapshot_id", "operation", "manifest_list")
                  .baselineRecords(recordBuilder.build())
                  .go();
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining(
            "Metadata function ('table_snapshot') is not supported on table '[dfs, tmp, metadatarefresh, onlyFull]'");
  }

  /** ensures the "table_manifests" function is not supported on internal Iceberg tables */
  @Test
  public void testManifestsFunction() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull refresh metadata";

    runSQL(sql);
    Table icebergTable = loadIcebergTable(finalIcebergMetadataLocation);

    assertThatThrownBy(
            () -> {
              testBuilder()
                  .ordered()
                  .sqlQuery(
                      "select * from table(table_manifests('dfs.tmp.metadatarefresh.onlyFull'))")
                  .baselineColumns("cnt")
                  .baselineValues(Long.valueOf(icebergTable.history().size()))
                  .go();
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining(
            "Metadata function ('table_manifests') is not supported on table '[dfs, tmp, metadatarefresh, onlyFull]'");
  }

  private static Table loadIcebergTable(String tableFolderPath) {
    File tableFolder = new File(tableFolderPath);
    Assert.assertTrue(tableFolder.exists());
    File tablePath = tableFolder.listFiles()[0];
    return BaseTestQuery.getIcebergTable(tablePath);
  }
}
