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

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.google.common.io.Resources;

public class TestDeltaScan extends BaseTestQuery {

  FileSystem fs;
  static String testRootPath = "/tmp/deltalake/";
  static Configuration conf;

  @Before
  public void initFs() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "local");
    fs = FileSystem.get(conf);
    Path p = new Path(testRootPath);

    if(fs.exists(p)) {
      fs.delete(p, true);
    }

    fs.mkdirs(p);
    copyFromJar("deltalake/testDataset", java.nio.file.Paths.get(testRootPath + "/testDataset"));
    copyFromJar("deltalake/JsonDataset", java.nio.file.Paths.get(testRootPath + "/JsonDataset"));
    copyFromJar("deltalake/lastCheckpointDataset", java.nio.file.Paths.get(testRootPath + "/lastCheckpointDataset"));
  }

  @After
  public void cleanup()  throws Exception {
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
      final String sql = "SELECT id, new_cases FROM dfs.tmp.deltalake.JsonDataset limit 3";
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id","new_cases")
        .baselineValues(71L, "45.0")
        .baselineValues(72L, "150.0")
        .baselineValues(73L, "116.0")
        .unOrdered().go();
    }
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
}
