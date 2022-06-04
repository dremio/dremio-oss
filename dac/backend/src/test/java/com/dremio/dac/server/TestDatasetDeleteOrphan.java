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

import static org.junit.Assert.assertEquals;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.datasets.DatasetVersionMutator.VersionDatasetKey;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.TestJobService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.SpaceConfig;

public class TestDatasetDeleteOrphan extends BaseTestServer {

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  private Provider<OptionManager> optionManagerProvider;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    optionManagerProvider = p(OptionManager.class);
  }

  @Test
  public void testDeleteOrphan() throws NamespaceException {
    testDeleteOrphan(5);
  }

  // This is a very slow test. Uncomment only when needed
  @Ignore
  @Test
  public void testDeleteOrphanRepeatedVDS() throws NamespaceException {
    testDeleteOrphan(10_000);
  }

  @SuppressWarnings("deprecation")
  private void testDeleteOrphan(int numberOfVds) throws NamespaceException {
    LegacyKVStoreProvider provider = p(LegacyKVStoreProvider.class).get();
    LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> datasetStore =
      provider.getStore(DatasetVersionMutator.VersionStoreCreator.class);

    NamespaceService namespaceService = newNamespaceService();

    SpaceConfig config = new SpaceConfig();
    config.setName("a");
    namespaceService.addOrUpdateSpace(new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);

    // The next statements are going to create 15 dataset versions.
    // It looks odd, but each time a job is submitted with the 'CREATE VDS'
    // statement, 2 dataset versions are created by the backend.
    createVDS("vds1", true);
    createVDS("vds1", true);
    for (int i = 0; i < numberOfVds; i++) {
      createVDS();
      dropVDS();
    }
    createVDS();

    int expectedNumberOfDatasetVersions = numberOfVds * 2 + 2 * 2 + 1;
    assertEquals(expectedNumberOfDatasetVersions, toStream(datasetStore.find()).count());

    // delete all the jobs, this will make the tmp.UNTITLED be an orphan
    TestJobService.cleanJobs();

    // Under test
    DatasetVersionMutator.deleteOrphans(optionManagerProvider, datasetStore, 0, true);

    // It is expected to have all tmp.UNTITLED versions are deleted.
    // It is expected to have only ONE a.vds dataset version.
    // It is expected to have two dataset versions of a.ds1.
    assertEquals(3, toStream(datasetStore.find()).count());
  }

  private void createVDS(String name, boolean replace) {
    // This is the easiest way I found to also create a tmp.UNTITLED dataset version.
    // This job execution creates two dataset versions, one with the provided
    // dataset path and another one tmp.UNTITLED.
    final String query = "CREATE " + (replace ? "OR REPLACE" : "") + " VDS a." + name + " AS SELECT * FROM INFORMATION_SCHEMA.\"tables\"";
    submitJob(query);
  }

  private void createVDS() {
    createVDS("vds", false);
  }

  private void dropVDS() {
    final String query = "DROP VDS a.vds";
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
