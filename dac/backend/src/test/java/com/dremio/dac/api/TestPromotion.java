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
package com.dremio.dac.api;

import static com.dremio.dac.api.TestCatalogResource.getFolderIdByName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test dataset promotion. */
public class TestPromotion extends BaseTestServer {
  private static final String CATALOG_PATH = "/catalog/";

  private Source source;

  @Before
  public void createSource() {
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    source = new Source();
    source.setName("catalog-test-promotion");
    source.setType("NAS");
    source.setConfig(nasConf);
    source.setMetadataPolicy(new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY));

    // create the source
    source =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH))
                .buildPost(Entity.json(source)),
            new GenericType<Source>() {});

    assertFalse(
        "check auto-promotion is disabled", source.getMetadataPolicy().isAutoPromoteDatasets());
  }

  @After
  public void deleteSource() throws NamespaceException {
    deleteSource(source.getName());
  }

  private Dataset createPDS(List<String> path, FileFormat format) {
    return new Dataset(
        null,
        Dataset.DatasetType.PHYSICAL_DATASET,
        path,
        null,
        null,
        null,
        null,
        null,
        null,
        format,
        null);
  }

  @Test
  public void file() throws Exception {
    doc("browse to the json directory");
    String id = getFolderIdByName(source.getChildren(), "json");
    assertNotNull(id, "Failed to find json directory");

    doc("load the json dir");
    Folder folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    String fileId = null;

    for (CatalogItem item : folder.getChildren()) {
      List<String> path = item.getPath();
      // get the numbers.json file
      if (item.getType() == CatalogItem.CatalogItemType.FILE
          && path.get(path.size() - 1).equals("numbers.json")) {
        fileId = item.getId();
        break;
      }
    }

    assertNotNull(fileId, "Failed to find numbers.json file");

    doc("load the file");
    final File file =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(fileId)))
                .buildGet(),
            new GenericType<File>() {});

    doc("promote the file (dac/backend/src/test/resources/json/numbers.json)");
    Dataset dataset =
        createPDS(CatalogServiceHelper.getPathFromInternalId(file.getId()), new JsonFileConfig());

    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(fileId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    doc("load the dataset");
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
                .buildGet(),
            new GenericType<Dataset>() {});

    doc("verify listing");
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    doc("unpromote file");
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
            .buildDelete());

    doc("dataset should no longer exist");
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId())).buildGet());

    doc("verify listing");
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);
  }

  @Test
  public void mapFile() throws Exception {
    setSystemOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
    doc("browse to the datasets directory");
    String id = getFolderIdByName(source.getChildren(), "datasets");
    assertNotNull(id, "Failed to find datasets directory");

    doc("load the datasets dir");
    Folder folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 22);

    String fileId = null;

    for (CatalogItem item : folder.getChildren()) {
      List<String> path = item.getPath();
      // get the numbers.json file
      if (item.getType() == CatalogItem.CatalogItemType.FILE
          && path.get(path.size() - 1).equals("map.parquet")) {
        fileId = item.getId();
        break;
      }
    }

    assertNotNull(fileId, "Failed to find map.parquet file");

    doc("load the file");
    final File file =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(fileId)))
                .buildGet(),
            new GenericType<File>() {});

    doc("promote the file");
    Dataset dataset =
        createPDS(
            CatalogServiceHelper.getPathFromInternalId(file.getId()), new ParquetFileConfig());

    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(fileId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    final Response response =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
                .buildGet());
    final String body = response.readEntity(String.class);
    assertTrue(
        body.contains(
            "\"fields\":[{\"name\":\"f1\",\"type\":{\"name\":\"MAP\",\"subSchema\":[{\"type\":{\"name\":\"STRUCT\",\"subSchema\":[{\"name\":\"key\",\"type\":{\"name\":\"VARCHAR\"}},{\"name\":\"value\",\"type\":{\"name\":\"INTEGER\"}}]}}]}}]"));

    doc("load the dataset");
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
                .buildGet(),
            new GenericType<Dataset>() {});

    doc("verify listing");
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 22);

    doc("unpromote file");
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
            .buildDelete());

    doc("dataset should no longer exist");
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId())).buildGet());
    resetSystemOption(ExecConstants.ENABLE_MAP_DATA_TYPE);
  }

  @Test
  public void folder() {
    doc("browse to the json directory");
    String id = getFolderIdByName(source.getChildren(), "json");
    assertNotNull(id, "Failed to find json directory");

    doc("load the json dir");
    Folder folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    doc(
        "promote a folder that contains several csv files (dac/backend/src/test/resources/datasets/folderdataset)");
    String folderId = getFolderIdByName(source.getChildren(), "datasets");
    assertNotNull(folderId, "Failed to find datasets directory");

    Folder dsFolder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(folderId)))
                .buildGet(),
            new GenericType<Folder>() {});

    String folderDatasetId = getFolderIdByName(dsFolder.getChildren(), "folderdataset");
    assertNotNull(folderDatasetId, "Failed to find folderdataset directory");

    doc("we want to use the path that the backend gives us so fetch the full folder");
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(folderDatasetId)))
                .buildGet(),
            new GenericType<Folder>() {});

    ParquetFileConfig parquetFileConfig = new ParquetFileConfig();
    Dataset dataset = createPDS(folder.getPath(), parquetFileConfig);

    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(folderDatasetId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    doc("load the promoted dataset");
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
                .buildGet(),
            new GenericType<Dataset>() {});

    doc("unpromote the folder");
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
            .buildDelete());

    doc("dataset should no longer exist");
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId())).buildGet());
  }

  @Test
  public void folderWithFileNameFiltering() {
    doc(
        "promote a folder that contains directory structure with parquet and other files as well (dac/backend/src/test/resources/datasets/parquet_2p_4s_mixed)");
    String folderId = getFolderIdByName(source.getChildren(), "datasets");
    assertNotNull(folderId, "Failed to find datasets directory");

    Folder dsFolder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(folderId)))
                .buildGet(),
            new GenericType<Folder>() {});

    String folderDatasetId = getFolderIdByName(dsFolder.getChildren(), "parquet_2p_4s_mixed");
    assertNotNull(folderDatasetId, "Failed to find parquet_2p_4s_mixed directory");

    doc("we want to use the path that the backend gives us so fetch the full folder");
    Folder folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(folderDatasetId)))
                .buildGet(),
            new GenericType<Folder>() {});

    ParquetFileConfig parquetFileConfig = new ParquetFileConfig();
    parquetFileConfig.setIgnoreOtherFileFormats(true);
    Dataset dataset = createPDS(folder.getPath(), parquetFileConfig);

    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(CATALOG_PATH)
                        .path(PathUtils.encodeURIComponent(folderDatasetId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    doc("load the promoted dataset");
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
                .buildGet(),
            new GenericType<Dataset>() {});

    doc("unpromote the folder");
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId()))
            .buildDelete());

    doc("dataset should no longer exist");
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getAPIv3().path(CATALOG_PATH).path(dataset.getId())).buildGet());
  }
}
