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

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.google.common.io.Resources;

/**
 * Verify that iceberg tables don't treat columns named 'dir0' as anything special.
 */
public class TestDirColumns extends BaseTestQuery {

  private static FileSystem fs;
  private static Configuration conf;

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "local");

    fs = FileSystem.get(conf);
  }

  private void createDataFile(File dir, String fileName) throws Exception {
    File dataFile = new File(dir, fileName);
    URI resource = Resources.getResource(
      "iceberg/nation/data/00000-1-a9e8d979-a183-40c5-af3d-a338ab62be8b-00000.parquet").toURI();
    Files.copy(Paths.get(resource), dataFile.toPath());
  }

  @Test
  public void testDirs() throws Exception {
    String srcTableName = "testDirs_src";
    String dstTableName = "testDirs_dst";

    File srcFolder = new File(getDfsTestTmpSchemaLocation(), srcTableName);
    srcFolder.mkdir();
    File srcFolderD0 = new File(srcFolder, "d0");
    srcFolderD0.mkdir();
    createDataFile(srcFolderD0, "file0");

    try (AutoCloseable ac = enableIcebergTables()) {
      final String ctasQuery =
        String.format(
          "CREATE TABLE %s.%s  "
            + " AS SELECT * from %s.%s",
          TEMP_SCHEMA,
          dstTableName,
          TEMP_SCHEMA,
          srcTableName);

      test(ctasQuery);

      testBuilder()
        .sqlQuery(String.format("select count(dir0) c from %s.%s", TEMP_SCHEMA, dstTableName))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(25L)
        .build()
        .run();

      testBuilder()
        .sqlQuery(String.format("select dir0 from %s.%s order by dir0 limit 1", TEMP_SCHEMA, dstTableName))
        .unOrdered()
        .baselineColumns("dir0")
        .baselineValues("d0")
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), srcTableName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dstTableName));
    }
  }

}
