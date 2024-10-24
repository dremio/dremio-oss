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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDropView extends BaseTestServer {

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();

    // setup a space
    NamespaceKey key = new NamespaceKey("space");
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("space");
    getNamespaceService().addOrUpdateSpace(key, spaceConfig);
  }

  @Test
  public void testDropView() throws NamespaceException {
    DatasetConfig dsConfig = createView("space", "vds1");
    dropView(dsConfig.getFullPathList());
    verifyViewDeleted(dsConfig.getFullPathList());
  }

  @Test
  public void testDeleteView() throws NamespaceException {
    DatasetConfig dsConfig = createView("space", "vds2");
    deleteView(dsConfig.getFullPathList(), dsConfig.getTag());
    verifyViewDeleted(dsConfig.getFullPathList());
  }

  @Test
  public void testDropViewWithMissingHistory() throws NamespaceException {
    DatasetConfig dsConfig = createView("space", "vds3");
    final LegacyKVStoreProvider provider = l(LegacyKVStoreProvider.class);
    DatasetVersionMutator.deleteDatasetVersion(
        provider,
        dsConfig.getFullPathList(),
        dsConfig.getVirtualDataset().getVersion().getVersion());
    dropView(dsConfig.getFullPathList());
    verifyViewDeleted(dsConfig.getFullPathList());
  }

  @Test
  public void testDeleteViewWithMissingHistory() throws NamespaceException {
    DatasetConfig dsConfig = createView("space", "vds4");
    final LegacyKVStoreProvider provider = l(LegacyKVStoreProvider.class);
    DatasetVersionMutator.deleteDatasetVersion(
        provider,
        dsConfig.getFullPathList(),
        dsConfig.getVirtualDataset().getVersion().getVersion());
    deleteView(dsConfig.getFullPathList(), dsConfig.getTag());
    verifyViewDeleted(dsConfig.getFullPathList());
  }

  private DatasetConfig createView(String space, String name) throws NamespaceException {
    NamespaceKey datasetPath = new NamespaceKey(Arrays.asList(space, name));
    final String query =
        "CREATE VIEW "
            + datasetPath.getSchemaPath()
            + " AS SELECT * FROM INFORMATION_SCHEMA.\"tables\"";
    submitJob(query);

    return getNamespaceService().getDataset(datasetPath);
  }

  private void dropView(List<String> path) {
    final String query = "DROP VDS " + String.join(".", path);
    submitJob(query);
  }

  private void submitJob(String query) {
    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
            .setQueryType(QueryType.UI_PREVIEW)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }

  private void deleteView(List<String> path, String tag) {
    WebTarget target =
        getHttpClient()
            .getAPIv2()
            .path("dataset")
            .path(String.join(".", path))
            .queryParam("savedTag", tag);
    expectSuccess(getBuilder(target).buildDelete(), new GenericType<DatasetUI>() {});
  }

  private void verifyViewDeleted(List<String> path) {
    NamespaceKey datasetPath = new NamespaceKey(path);
    assertThatThrownBy(() -> getNamespaceService().getDataset(datasetPath))
        .isExactlyInstanceOf(NamespaceNotFoundException.class);
  }
}
