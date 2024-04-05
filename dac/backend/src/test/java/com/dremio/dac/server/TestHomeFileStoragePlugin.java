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
package com.dremio.dac.server;

import com.dremio.connector.metadata.EntityPath;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.homefiles.HomeFileSystemStoragePlugin;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test home file storage plugin. */
public class TestHomeFileStoragePlugin extends BaseTestServer {

  @ClassRule public static final TemporaryFolder temp = new TemporaryFolder();
  private static String complexPdsPath;

  @BeforeClass
  public static void setup() throws Exception {

    complexPdsPath =
        addJsonTable(
            "complex_schema.json",
            "{"
                + "\"custID\": 100,"
                + "\"cName\": \"customer\","
                + "\"cItems\": [{ \"iName\": \"laptop\", \"weight_in_pounds\": 2.5 }],"
                + "\"cProperties\": {\"a\": 1000,\"b\": 0.25}"
                + "}");
  }

  @Test
  public void testGetDatasetHandle() throws Exception {
    final String spaceName = "vds_space";
    final String vdsName = "vds";
    try {
      getNamespaceService().getSpace(new NamespaceKey(spaceName));
    } catch (NamespaceNotFoundException e) {
      expectSuccess(
          getBuilder(getPublicAPI(3).path("/catalog/"))
              .buildPost(
                  Entity.json(new com.dremio.dac.api.Space(null, spaceName, null, null, null))),
          new GenericType<com.dremio.dac.api.Space>() {});
    }

    final DatasetPath vdsPath = new DatasetPath(spaceName + "." + vdsName);
    List<String> components = new ArrayList<>();
    components.add(spaceName);
    components.add(vdsName);
    EntityPath entityPath = new EntityPath(components);
    final String vdsSql = String.format("SELECT * FROM dfs.\"%s\"", complexPdsPath);
    createDatasetFromSQLAndSave(vdsPath, vdsSql, Collections.emptyList());

    DatasetConfig currentConfig =
        new DatasetConfig().setTag("0").setType(DatasetType.VIRTUAL_DATASET);
    final CatalogServiceImpl catalog = (CatalogServiceImpl) l(CatalogService.class);
    StoragePlugin plugin = catalog.getSource(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME);
    HomeFileSystemStoragePlugin homePlugin = (HomeFileSystemStoragePlugin) plugin;

    UserExceptionAssert.assertThatThrownBy(
            () ->
                homePlugin.getDatasetHandle(
                    entityPath,
                    DatasetRetrievalOptions.DEFAULT.toBuilder()
                        .build()
                        .asGetDatasetOptions(currentConfig)))
        .hasMessageContaining("not a valid physical dataset");
  }

  private static NamespaceService getNamespaceService() {
    return p(NamespaceService.class).get();
  }

  private static String addJsonTable(String tableName, String... jsonData) throws Exception {
    final File file = temp.newFile(tableName);
    final String dataFile = file.getAbsolutePath();
    // TODO write each record in a separate file, so we can cause a union type for example
    try (PrintWriter writer = new PrintWriter(file)) {
      for (String record : jsonData) {
        writer.println(record);
      }
    }
    final DatasetPath path = new DatasetPath(ImmutableList.of("dfs", dataFile));
    final DatasetConfig dataset =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
            .setFullPathList(path.toPathList())
            .setName(path.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);
    return dataFile;
  }
}
