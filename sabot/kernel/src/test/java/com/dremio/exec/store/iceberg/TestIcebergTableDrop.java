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

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.google.common.io.Resources;

public class TestIcebergTableDrop extends BaseTestQuery {
  private static FileSystem fs;
  private static Configuration conf;
  private Schema schema;

  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "local");

    fs = FileSystem.get(conf);
  }

  @Before
  public void setUp() {
    schema = new Schema(
      NestedField.optional(1, "n_nationkey", Types.IntegerType.get()),
      NestedField.optional(2, "n_name", Types.StringType.get()),
      NestedField.optional(3, "n_regionkey", Types.IntegerType.get()),
      NestedField.optional(4, "n_comment", Types.StringType.get())
    );
  }

  private DataFile createDataFile(File dir, String fileName) throws Exception {
    File dataFile = new File(dir, fileName);
    URI resource = Resources.getResource(
      "iceberg/nation/data/00000-1-a9e8d979-a183-40c5-af3d-a338ab62be8b-00000.parquet").toURI();
    Files.copy(Paths.get(resource), dataFile.toPath());

    return DataFiles.builder(PartitionSpec.builderFor(schema).build())
      .withInputFile(org.apache.iceberg.Files.localInput(dataFile))
      .withRecordCount(25)
      .withFormat(FileFormat.PARQUET)
      .build();
  }

  @Test
  public void testDropTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      Path rootPath = Paths.get(getDfsTestTmpSchemaLocation(), "iceberg", "nation");
      File tableRoot = rootPath.toFile();
      IcebergModel icebergModel = getIcebergModel(tableRoot);
      Files.createDirectories(rootPath);
      String root = rootPath.toString();

      String tableName = "dfs_test.iceberg.nation";

      HadoopTables tables = new HadoopTables(conf);
      Table table = tables.create(schema, null, root);
      IcebergTableInfo tableInfo =
          new IcebergTableWrapper(getSabotContext(), HadoopFileSystem.get(fs), icebergModel, root)
              .getTableInfo();
      assertEquals(tableInfo.getRecordCount(), 0);

      // Append some data files.
      Transaction transaction = table.newTransaction();
      AppendFiles appendFiles = transaction.newAppend();
      appendFiles.appendFile(createDataFile(rootPath.toFile(), "d1"));
      appendFiles.commit();
      transaction.commitTransaction();

      testBuilder()
          .sqlQuery("select count(*) c from " + tableName)
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(25L)
          .build()
          .run();

      testBuilder()
          .sqlQuery("DROP TABLE " + tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Table [%s] dropped", tableName))
          .build()
          .run();

      errorMsgTestHelper(
          "select count(*) c from " + tableName, "Table '" + tableName + "' not found");
    }
  }
}
