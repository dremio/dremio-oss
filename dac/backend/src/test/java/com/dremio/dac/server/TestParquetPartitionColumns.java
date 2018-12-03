/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.server;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;


/**
 * Tests that partition columns for a parquet file stay within limit.
 */
public class TestParquetPartitionColumns extends BaseTestServer {

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testGroupScan() throws Exception {
    // create vds with decimal types
    final String sName = "s1" + Math.random();

    URL stream = (TestParquetPartitionColumns.class.getClassLoader()
      .getResource("datasets/parquet"));

    File fileDir = new File(stream.getFile());
    DatasetPath parquet = new DatasetPath(ImmutableList.of("dfs", fileDir.getAbsolutePath()));

    DatasetConfig config = addDataSet(parquet);

    DremioTable table = p(CatalogService.class).get().getCatalog(SchemaConfig.newBuilder
      (DEFAULT_USERNAME)
      .build())
      .getTable(config.getId().getId());

    List<String> partitionColumnList = table.getDatasetConfig().getReadDefinition()
      .getPartitionColumnsList();

    Assert.assertEquals(25, partitionColumnList.size());
    Assert.assertEquals( "row1", partitionColumnList.get(1));

  }

  private NamespaceService getNamespaceService() {
    return p(NamespaceService.class).get();
  }

  protected DatasetConfig addDataSet(DatasetPath path) throws Exception {
    final DatasetConfig dataset = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.PARQUET).setCtime(1L).setOwner
          (DEFAULT_USERNAME))
      );
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);
    return nsService.getDataset(path.toNamespaceKey());
  }
}
