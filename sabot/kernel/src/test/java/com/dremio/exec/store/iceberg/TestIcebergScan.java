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
package com.dremio.exec.store.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.google.common.io.Resources;

public class TestIcebergScan extends BaseTestQuery {

  static FileSystem fs;
  static String testRootPath = "/tmp/iceberg";
  static Configuration conf;

  @BeforeClass
  public static void initFs() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "local");

    fs = FileSystem.get(conf);
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
  public void testSuccessFile() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      Path p = new Path(testRootPath);
      if (fs.exists(p)) {
        fs.delete(p, true);
      }

      fs.mkdirs(p);
      copyFromJar("iceberg/nation", java.nio.file.Paths.get(testRootPath));

      testBuilder()
          .sqlQuery("select count(*) c from dfs.tmp.iceberg where 1 = 1")
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(25L)
          .build()
          .run();
    }
  }

  @Test
  public void testPartitionMismatchSpecSchema() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      Path p = new Path(testRootPath);
      if (fs.exists(p)) {
        fs.delete(p, true);
      }

      fs.mkdirs(p);
      copyFromJar("iceberg/partitionednation", java.nio.file.Paths.get(testRootPath));

      HadoopTables tables = new HadoopTables(conf);
      Table table = tables.load(testRootPath);

      // n_regionkey was renamed to regionkey
      assertNull(table.schema().findField("n_regionkey"));
      assertNotNull(table.schema().findField("regionkey"));

      assertEquals(1, table.spec().fields().size());
      // no change in partition spec
      assertEquals("n_regionkey", table.spec().fields().get(0).name());

      IcebergTableInfo tableInfo = new IcebergTableWrapper(getSabotContext(),
          HadoopFileSystem.get(fs), conf, new File(testRootPath).getAbsolutePath()).getTableInfo();
      assertEquals(1, tableInfo.getPartitionColumns().size());
      // partition column matches new column name
      assertEquals("regionkey", tableInfo.getPartitionColumns().get(0));
    }
  }
}
