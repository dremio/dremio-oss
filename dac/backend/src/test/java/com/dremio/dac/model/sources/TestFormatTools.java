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
package com.dremio.dac.model.sources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import com.dremio.common.util.FileUtils;
import com.dremio.dac.explore.model.FileFormatUI;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import java.io.IOException;
import javax.ws.rs.client.Entity;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test FormatTools via REST api explored by {@SourceResource} */
public class TestFormatTools extends BaseTestServer {
  private static final ch.qos.logback.classic.Logger rootLogger =
      ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("com.dremio"));
  private static Level originalLogLevel;

  @BeforeClass
  public static void initLogLevel() {
    originalLogLevel = rootLogger.getLevel();
    rootLogger.setLevel(Level.DEBUG);
  }

  @AfterClass
  public static void restoreLogLevel() throws Exception {
    rootLogger.setLevel(originalLogLevel);
  }

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    final SourceService sourceService = getSourceService();
    {
      final NASConf nas = new NASConf();
      nas.path = "/";
      SourceUI source = new SourceUI();
      source.setName("dfs_static_test_hadoop");
      source.setConfig(nas);
      source.setMetadataPolicy(
          UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
      sourceService.registerSourceWithRuntime(source);
    }
  }

  private static String getUrlPath(String file) throws IOException {
    return Path.of(FileUtils.getResourceAsFile(file).getAbsolutePath()).toString();
  }

  @Test
  public void testParquetFormat() throws Exception {
    String fileUrlPath = getUrlPath("/singlefile_parquet_dir/0_0_0.parquet");
    FileFormatUI fileFormat =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dfs_static_test_hadoop/file_format/" + fileUrlPath))
                .buildGet(),
            FileFormatUI.class);
    assertTrue(fileFormat.getFileFormat() instanceof ParquetFileConfig);
    assertEquals(FileType.PARQUET, fileFormat.getFileFormat().getFileType());
  }

  @Test
  public void testIcebergFormat() throws Exception {
    String tableUrlPath = getUrlPath("/datasets/iceberg/empty_table");
    FileFormatUI fileFormat =
        expectSuccess(
            getBuilder(
                    getAPIv2().path("/source/dfs_static_test_hadoop/file_format/" + tableUrlPath))
                .buildGet(),
            FileFormatUI.class);
    assertTrue(fileFormat.getFileFormat() instanceof IcebergFileConfig);
    assertEquals(FileType.ICEBERG, fileFormat.getFileFormat().getFileType());
  }

  @Test
  public void testIcebergTablePreviewData() throws Exception {
    setSystemOption("dac.format.preview.batch_size", "1000");
    IcebergTestTables.Table icebergTable =
        IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
    try {
      String tableUrlPath = icebergTable.getLocation();
      FileFormatUI fileFormat =
          expectSuccess(
              getBuilder(
                      getAPIv2().path("/source/dfs_static_test_hadoop/file_format/" + tableUrlPath))
                  .buildGet(),
              FileFormatUI.class);
      assertTrue(fileFormat.getFileFormat() instanceof IcebergFileConfig);
      JobDataFragment data =
          expectSuccess(
              getBuilder(
                      getAPIv2()
                          .path("/source/dfs_static_test_hadoop/file_preview/" + tableUrlPath))
                  .buildPost(Entity.json(fileFormat.getFileFormat())),
              JobDataFragment.class);
      assertEquals(551, data.getReturnedRowCount());
      assertEquals(6, data.getColumns().size());
      assertEquals("order_id", data.getColumns().get(0).getName());
      assertEquals("order_year", data.getColumns().get(1).getName());
      assertEquals("order_date", data.getColumns().get(2).getName());
      assertEquals("source_id", data.getColumns().get(3).getName());
      assertEquals("product_name", data.getColumns().get(4).getName());
      assertEquals("amount", data.getColumns().get(5).getName());
    } finally {
      resetSystemOption("dac.format.preview.batch_size");
      if (icebergTable != null) {
        icebergTable.close();
      }
    }
  }

  @Test
  public void testPreviewMetastoreIcebergTable() throws Exception {
    String tableUrlPath = getUrlPath("/datasets/iceberg/metastore_table");
    IcebergFileConfig icebergFileConfig = new IcebergFileConfig();

    doc("preview data for metastore iceberg table");
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getAPIv2().path("/source/dfs_static_test_hadoop/file_preview" + tableUrlPath))
                .buildPost(Entity.json(icebergFileConfig)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error
            .getErrorMessage()
            .contains(
                "This folder does not contain a filesystem-based Iceberg table. If the table in this folder is managed via a catalog "
                    + "such as Hive, Glue, or Nessie, please use a data source configured for that catalog to connect to this table."));
  }
}
