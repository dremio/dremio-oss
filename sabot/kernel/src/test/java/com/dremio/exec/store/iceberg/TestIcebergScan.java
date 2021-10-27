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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.google.common.io.Resources;

public class TestIcebergScan extends BaseTestQuery {
  private static FileSystem fs;
  private String testRootPath;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
    fs = FileSystem.get(conf);
    setSystemOption(ENABLE_ICEBERG, "true");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    setSystemOption(ENABLE_ICEBERG, "false");
  }

  @After
  public void tearDown() throws Exception {
    if (testRootPath != null) {
      fs.delete(new Path(testRootPath), true);
    }
  }

  @Test
  public void testScanEmptyTable() throws Exception {
    testRootPath = "/tmp/empty_iceberg";
    copyFromJar("iceberg/empty_table", testRootPath);
    runQueryExpectingRecordCount("select count(*) c from dfs_hadoop.tmp.empty_iceberg", 0L);
  }

  @Test
  public void testScanNonIcebergTableWithColumnIds() throws Exception {
    testRootPath = "/tmp/non_iceberg_with_column_ids";
    copyFromJar("iceberg/non_iceberg_with_column_ids", testRootPath);
    testBuilder()
      .sqlQuery("select * from dfs_hadoop.tmp.non_iceberg_with_column_ids")
      .unOrdered()
      .baselineColumns("value")
      .baselineValues(new BigDecimal("1.00"))
      .baselineValues(new BigDecimal("2.00"))
      .baselineValues(new BigDecimal("3.00"))
      .baselineValues(new BigDecimal("4.00"))
      .baselineValues(new BigDecimal("5.00"))
      .baselineValues(new BigDecimal("6.00"))
      .baselineValues(new BigDecimal("7.00"))
      .baselineValues(new BigDecimal("8.00"))
      .baselineValues(new BigDecimal("9.00"))
      .baselineValues(new BigDecimal("10.00"))
      .baselineValues(new BigDecimal("11.00"))
      .baselineValues(new BigDecimal("12.00"))
      .baselineValues(new BigDecimal("13.00"))
      .baselineValues(new BigDecimal("14.00"))
      .baselineValues(new BigDecimal("15.00"))
      .baselineValues(new BigDecimal("16.00"))
      .baselineValues(new BigDecimal("17.00"))
      .baselineValues(new BigDecimal("18.00"))
      .baselineValues(new BigDecimal("19.00"))
      .baselineValues(new BigDecimal("20.00"))
      .baselineValues(new BigDecimal("21.00"))
      .baselineValues(new BigDecimal("22.00"))
      .baselineValues(new BigDecimal("23.00"))
      .baselineValues(new BigDecimal("24.00"))
      .build()
      .run();
  }

  @Test
  public void testSuccessFile() throws Exception {
    testRootPath = "/tmp/iceberg";
    copyFromJar("iceberg/nation", testRootPath);
    runQueryExpectingRecordCount("select count(*) c from dfs_hadoop.tmp.iceberg where 1 = 1", 25L);
  }

  @Test
  public void testPartitionMismatchSpecSchema() throws Exception {
    testRootPath = "/tmp/iceberg";
    copyFromJar("iceberg/partitionednation", testRootPath);

    File tableRoot = new File(testRootPath);
    IcebergModel icebergModel = getIcebergModel(tableRoot, IcebergCatalogType.HADOOP);
    Table table = icebergModel.getIcebergTable(icebergModel.getTableIdentifier(tableRoot.getPath()));

    // n_regionkey was renamed to regionkey
    assertNull(table.schema().findField("n_regionkey"));
    assertNotNull(table.schema().findField("regionkey"));

    assertEquals(1, table.spec().fields().size());
    // no change in partition spec
    assertEquals("n_regionkey", table.spec().fields().get(0).name());

    IcebergTableInfo tableInfo = new IcebergTableWrapper(getSabotContext(), HadoopFileSystem.get(fs), icebergModel, testRootPath).getTableInfo();
    assertEquals(1, tableInfo.getPartitionColumns().size());
    // partition column matches new column name
    assertEquals("regionkey", tableInfo.getPartitionColumns().get(0));
  }

  private void runQueryExpectingRecordCount(String query, long recordCount) throws Exception {
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("c")
      .baselineValues(recordCount)
      .build()
      .run();
  }

  private void copyFromJar(String src, String testRoot) throws IOException, URISyntaxException {
    Path path = new Path(testRoot);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    fs.mkdirs(path);

    URI resource = Resources.getResource(src).toURI();
    java.nio.file.Path srcDir = Paths.get(resource);
    try (Stream<java.nio.file.Path> stream = Files.walk(srcDir)) {
      stream.forEach(source -> copy(source, Paths.get(testRoot).resolve(srcDir.relativize(source))));
    }
  }

  private void copy(java.nio.file.Path source, java.nio.file.Path dest) {
    try {
      Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
