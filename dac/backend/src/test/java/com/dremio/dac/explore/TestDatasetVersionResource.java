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
package com.dremio.dac.explore;

import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.api.Dataset;
import com.dremio.dac.explore.model.DatasetUIWithHistory;
import com.dremio.dac.explore.model.HistoryItem;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Tests for DatasetVersionResource
 */
public class TestDatasetVersionResource extends BaseTestServer {
  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();

    // setup space
    NamespaceKey key = new NamespaceKey("dsvTest");
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("dsvTest");
    newNamespaceService().addOrUpdateSpace(key, spaceConfig);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    // setup space
    NamespaceKey key = new NamespaceKey("dsvTest");
    SpaceConfig space = newNamespaceService().getSpace(key);
    newNamespaceService().deleteSpace(key, space.getTag());
  }

  @Test
  public void testVersionHistory() throws Exception {
    // Test for DX-12601

    // create a VDS in the space
    Dataset newVDS = createVDS(Arrays.asList("dsvTest", "myVDS"),"select * from sys.version");
    Dataset vds = expectSuccess(getBuilder(getPublicAPI(3).path("catalog")).buildPost(Entity.json(newVDS)), new GenericType<Dataset>() {});

    // create a derivation of the VDS
    String parentDataset = String.join(".", vds.getPath());
    DatasetVersion datasetVersion = DatasetVersion.newVersion();
    WebTarget target = getAPIv2()
      .path("datasets")
      .path("new_untitled")
      .queryParam("parentDataset", parentDataset)
      .queryParam("newVersion", datasetVersion)
      .queryParam("limit", 120);
    InitialPreviewResponse initialPreviewResponse = expectSuccess(getBuilder(target).buildPost(Entity.json(null)), new GenericType<InitialPreviewResponse>() {});

    // save the derivation a new VDS
    target = getAPIv2()
      .path("dataset")
      .path("tmp.UNTITLED")
      .path("version")
      .path(datasetVersion.getVersion())
      .path("save")
      .queryParam("as", "dsvTest.myVDS2");
    DatasetUIWithHistory dswh = expectSuccess(getBuilder(target).buildPost(Entity.json(null)), new GenericType<DatasetUIWithHistory>() {});

    // modify the sql of the new VDS by doing a transform
    DatasetVersion datasetVersion2 = DatasetVersion.newVersion();
    String dsPath = String.join(".", dswh.getDataset().getFullPath());

    target = getAPIv2()
      .path("dataset")
      .path(dsPath)
      .path("version")
      .path(dswh.getDataset().getDatasetVersion().getVersion())
      .path("transformAndPreview")
      .queryParam("newVersion", datasetVersion2);

    TransformUpdateSQL transformSql = new TransformUpdateSQL();
    transformSql.setSql("SELECT \"version\" FROM dsvTest.myVDS");

    initialPreviewResponse = expectSuccess(getBuilder(target).buildPost(Entity.json(transformSql)), new GenericType<InitialPreviewResponse>() {});

    // save the transform as a third VDS
    target = getAPIv2()
      .path("dataset")
      .path(dsPath)
      .path("version")
      .path(initialPreviewResponse.getDataset().getDatasetVersion().getVersion())
      .path("save")
      .queryParam("as", "dsvTest.myVDS3");

    DatasetUIWithHistory dswh2 = expectSuccess(getBuilder(target).buildPost(Entity.json(null)), new GenericType<DatasetUIWithHistory>() {});

    // preview the last history item
    HistoryItem historyItem = dswh.getHistory().getItems().get(0);
    String dsPath2 = String.join(".", dswh2.getDataset().getFullPath());

    target = getAPIv2()
      .path("dataset")
      .path(dsPath2)
      .path("version")
      .path(historyItem.getDatasetVersion().getVersion())
      .path("preview")
      .queryParam("view", "explore")
      .queryParam("tipVersion", dswh2.getDataset().getDatasetVersion());

    expectSuccess(getBuilder(target).buildGet(), new GenericType<InitialPreviewResponse>() {});
  }

  private Dataset createVDS(List<String> path, String sql) {
    return new Dataset(
      null,
      Dataset.DatasetType.VIRTUAL_DATASET,
      path,
      null,
      null,
      null,
      null,
      sql,
      null,
      null,
      null
    );
  }
}
