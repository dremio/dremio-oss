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
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.setupLocalFS;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.verifyIcebergMetadata;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;


/**
 * Test class for iceberg snapshot functions select * from table(table_files('table')) at snapshot snapshotId
 */
public class TestInternalIcebergTableFilesFunction extends BaseTestQuery {

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

    copyFromJar("metadatarefresh/incrementalRefresh", java.nio.file.Paths.get(testRootPath + "/incrementalRefresh"));

  }

  @After
  public void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
  }

  public static void copy(java.nio.file.Path source, java.nio.file.Path dest) {
    try {
      Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  protected static void copyFromJar(String sourceElement, final java.nio.file.Path target) throws URISyntaxException, IOException {
    URI resource = Resources.getResource(sourceElement).toURI();
    java.nio.file.Path srcDir = java.nio.file.Paths.get(resource);
    try (Stream<java.nio.file.Path> stream = java.nio.file.Files.walk(srcDir)) {
      stream.forEach(source -> copy(source, target.resolve(srcDir.relativize(source))));
    }
  }

  @Test
  public void testTableFiles() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.incrementalRefresh refresh metadata";

      //this will do a full refresh first
      runSQL(sql);
      Thread.sleep(1001L);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema , Sets.newHashSet("dir0"), 1);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

      Thread.sleep(1001L);

      String selectTableFilesQuery = String.format("SELECT count(file_path) as cnt FROM table(table_files('dfs.tmp.metadatarefresh.incrementalRefresh')) AT SNAPSHOT '%s'",
        icebergTable.currentSnapshot().snapshotId());
      testBuilder()
        .sqlQuery(selectTableFilesQuery)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(1L)
        .build()
        .run();


      copyFromJar("metadatarefresh/float.parquet", java.nio.file.Paths.get(testRootPath + "/incrementalRefresh/float.parquet"));
      //here one table_file will increase

      runSQL(sql);
      Thread.sleep(2001L);

      icebergTable.refresh();

      Thread.sleep(1001L);


      String selectTableFilesQuery2 = String.format("SELECT count(file_path) as cnt FROM table(table_files('dfs.tmp.metadatarefresh.incrementalRefresh')) AT SNAPSHOT '%s'",
        icebergTable.currentSnapshot().snapshotId());
      testBuilder()
        .sqlQuery(selectTableFilesQuery2)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(2L)
        .build()
        .run();

    }


  }

}
