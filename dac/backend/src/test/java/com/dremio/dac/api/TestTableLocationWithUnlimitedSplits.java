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

import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test catalog API with Unlimited splits */
public class TestTableLocationWithUnlimitedSplits extends BaseTestServer {
  private static final String CATALOG_PATH = "/catalog/";

  private static final String SOURCE_PATH = TestTools.getWorkingPath() + "/src/test/resources/";

  private Source source;

  @Before
  public void createSource() {
    NASConf nasConf = new NASConf();
    nasConf.path = SOURCE_PATH;

    source = new Source();
    source.setName("catalog-test-unlimited");
    source.setType("NAS");
    source.setConfig(nasConf);
    source.setMetadataPolicy(new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY));

    // create the source
    source =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(source)),
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
    doc("browse to the parquet directory");
    String id = getFolderIdByName(source.getChildren(), "singlefile_parquet_dir");
    assertNotNull(id, "Failed to find singlefile_parquet_dir directory");

    doc("load the singlefile_parquet_dir dir");
    Folder folder =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 1);

    String fileId = null;

    for (CatalogItem item : folder.getChildren()) {
      List<String> path = item.getPath();
      // get the numbers.json file
      if (item.getType() == CatalogItem.CatalogItemType.FILE
          && path.get(path.size() - 1).equals("0_0_0.parquet")) {
        fileId = item.getId();
        break;
      }
    }

    assertNotNull(fileId, "Failed to find 0_0_0.parquet file");

    doc("load the file");
    final File file =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(fileId)))
                .buildGet(),
            new GenericType<File>() {});

    doc("promote the file (dac/backend/src/test/resources/singlefile_parquet_dir/0_0_0.parquet)");
    Dataset dataset =
        createPDS(
            CatalogServiceHelper.getPathFromInternalId(file.getId()), new ParquetFileConfig());

    dataset =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(fileId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    doc("load the dataset");
    dataset =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet(),
            new GenericType<Dataset>() {});

    assertEquals(
        SOURCE_PATH + "singlefile_parquet_dir/0_0_0.parquet", dataset.getFormat().getLocation());

    doc("unpromote file");
    expectSuccess(
        getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildDelete());
  }

  @Test
  public void folder() {
    doc("browse to the singlefile_parquet_dir directory");
    String id = getFolderIdByName(source.getChildren(), "singlefile_parquet_dir");
    assertNotNull(id, "Failed to find singlefile_parquet_dir directory");

    doc("load singlefile_parquet_dir dir");
    Folder folder =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});

    Dataset dataset = createPDS(folder.getPath(), new ParquetFileConfig());
    dataset =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(id)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    doc("load the promoted dataset");
    dataset =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet(),
            new GenericType<Dataset>() {});

    assertEquals(SOURCE_PATH + "singlefile_parquet_dir", dataset.getFormat().getLocation());

    doc("unpromote the folder");
    expectSuccess(
        getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildDelete());
  }
}
