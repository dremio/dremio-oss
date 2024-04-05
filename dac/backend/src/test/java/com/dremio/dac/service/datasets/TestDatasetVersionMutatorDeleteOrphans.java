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
package com.dremio.dac.service.datasets;

import static org.junit.Assert.assertEquals;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.datasets.DatasetVersionMutator.VersionDatasetKey;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.TestJobService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.inject.Provider;
import org.junit.Before;
import org.junit.Test;

public class TestDatasetVersionMutatorDeleteOrphans extends BaseTestServer {

  private static final String SPACE_NAME = "space";
  private static final String VIEW_NAME = "view";

  private Provider<OptionManager> optionManagerProvider;

  private KVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersionsStore;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    optionManagerProvider = p(OptionManager.class);

    KVStoreProvider provider = l(KVStoreProvider.class);
    datasetVersionsStore = provider.getStore(DatasetVersionMutator.VersionStoreCreator.class);

    NamespaceService namespaceService = newNamespaceService();
    SpaceConfig config = new SpaceConfig();
    config.setName(SPACE_NAME);
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
  }

  @Test
  public void testDeleteOrphans() throws NamespaceException {
    testDeleteOrphans(5);
  }

  private void testDeleteOrphans(int numberOfVds) throws NamespaceException {
    // Every new call to "CREATE VDS ... AS SELECT *" results in
    // two dataset versions: one with tmp.UNTITLED name and one with the actual
    // name. When the view is identical no new dataset version is created.
    createVDS("vds1", true);
    createVDS("vds1", true);
    for (int i = 0; i < numberOfVds; i++) {
      createVDS();
      dropVDS();
    }
    createVDS();

    // 2 versions are created for every call to createVDS, except the
    // second call with the same name.
    int expectedNumberOfDatasetVersions = 2 + numberOfVds * 2 + 2;
    assertEquals(expectedNumberOfDatasetVersions, toStream(datasetVersionsStore.find()).count());

    // delete all the jobs, this will make the tmp.UNTITLED be an orphan
    TestJobService.cleanJobs();

    // Under test
    DatasetVersionMutator.deleteOrphans(optionManagerProvider, datasetVersionsStore, 0, true);

    // It is expected to have all tmp.UNTITLED versions are deleted.
    // It is expected to have (_numberOfVds_ + 1) a.vds dataset versions.
    // It is expected to have one dataset versions of a.ds1 as second is identical.
    int expected = numberOfVds + 1 + 1;
    assertEquals(expected, toStream(datasetVersionsStore.find()).count());
  }

  private void createVDS(String name, boolean replace) {
    // This is the easiest way I found to also create a tmp.UNTITLED dataset version.
    // This job execution creates two dataset versions, one with the provided
    // dataset path and another one tmp.UNTITLED.
    final String query =
        "CREATE "
            + (replace ? "OR REPLACE" : "")
            + " VDS "
            + String.format("%s.%s", SPACE_NAME, name)
            + " AS SELECT * FROM INFORMATION_SCHEMA.\"tables\"";
    submitJob(query);
  }

  private void createVDS() {
    createVDS(VIEW_NAME, false);
  }

  private void dropVDS() {
    final String query = String.format("DROP VDS %s.%s", SPACE_NAME, VIEW_NAME);
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

  private <T> Stream<T> toStream(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}
