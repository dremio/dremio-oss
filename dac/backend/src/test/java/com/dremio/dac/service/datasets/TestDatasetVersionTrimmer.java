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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link DatasetVersionTrimmer}. */
public class TestDatasetVersionTrimmer extends BaseTestServer {
  private static final String SPACE_NAME = "space";
  private static final String VIEW_NAME = "view";
  private static final String FULL_VIEW_NAME = SPACE_NAME + "." + VIEW_NAME;
  private static final DatasetPath VIEW_PATH = new DatasetPath(FULL_VIEW_NAME);

  private KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
      datasetVersionsStore;

  private DatasetVersionMutator mutator;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();

    KVStoreProvider provider = l(KVStoreProvider.class);
    datasetVersionsStore = provider.getStore(DatasetVersionMutator.VersionStoreCreator.class);

    NamespaceService namespaceService = newNamespaceService();
    SpaceConfig config = new SpaceConfig();
    config.setName(SPACE_NAME);
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);

    mutator =
        new DatasetVersionMutator(
            getSabotContext().getKVStoreProvider(),
            l(JobsService.class),
            l(CatalogService.class),
            l(OptionManager.class),
            l(ContextService.class));

    createView();
  }

  @Test
  public void testTrimVersions_deleteOne() throws Exception {
    trimVersionsTest(1);
  }

  @Test
  public void testTrimVersions_deleteMany() throws Exception {
    trimVersionsTest(50);
  }

  @Test
  public void testTrimVersions_noChanges() throws Exception {
    addVersions(VIEW_PATH, 9);
    List<VirtualDatasetUI> versionsBefore = getAllVersions(VIEW_PATH);

    DatasetVersionTrimmer.trimHistory(Clock.systemUTC(), datasetVersionsStore, 10, 1);

    // Verify versions are the same.
    List<VirtualDatasetUI> versionsAfter = getAllVersions(VIEW_PATH);
    assertEquals(versionsBefore, versionsAfter);
  }

  @Test
  public void testTrimVersions_lastModifiedTime() throws Exception {
    // Initial version from createView and this one are created with system clock.
    DatasetVersion latestVersion = addVersion(VIEW_PATH).getLeft();

    // Put current time in the future, 2 weeks from now.
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(Instant.now().plus(14, ChronoUnit.DAYS));

    // Should not trim with 30 days limit.
    List<VirtualDatasetUI> versionsBefore = getAllVersions(VIEW_PATH);
    DatasetVersionTrimmer.trimHistory(clock, datasetVersionsStore, 1, 30);
    assertEquals(versionsBefore.size(), getAllVersions(VIEW_PATH).size());

    // Should trim with 5 days limit.
    DatasetVersionTrimmer.trimHistory(clock, datasetVersionsStore, 1, 5);
    List<VirtualDatasetUI> versionsAfter = getAllVersions(VIEW_PATH);
    assertEquals(1, versionsAfter.size());
    assertEquals(latestVersion, versionsAfter.get(0).getVersion());
  }

  private void trimVersionsTest(int numVersionsToAdd) throws Exception {
    Pair<DatasetVersion, DatasetVersion> versionsPair = addVersions(VIEW_PATH, numVersionsToAdd);
    DatasetVersion latestVersion = versionsPair.getLeft();
    DatasetVersion previousVersion = versionsPair.getRight();

    // Verify there are N + 1 versions and latest points to the previous one.
    List<VirtualDatasetUI> versions = getAllVersions(VIEW_PATH);
    assertEquals(numVersionsToAdd + 1, versions.size());
    VirtualDatasetUI latestVersionVds =
        versions.stream().filter(v -> v.getVersion().equals(latestVersion)).findFirst().get();
    assertEquals(
        previousVersion.getVersion(), latestVersionVds.getPreviousVersion().getDatasetVersion());

    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(Instant.now().plus(14, ChronoUnit.DAYS));
    DatasetVersionTrimmer.trimHistory(clock, datasetVersionsStore, 1, 1);

    // Verify there is one remaining version pointing to null.
    versions = getAllVersions(VIEW_PATH);
    assertEquals(1, versions.size());
    assertEquals(latestVersion, versions.get(0).getVersion());
    assertNull(versions.get(0).getPreviousVersion());
  }

  private List<VirtualDatasetUI> getAllVersions(DatasetPath path) throws Exception {
    return ImmutableList.copyOf(mutator.getAllVersions(path));
  }

  private Pair<DatasetVersion, DatasetVersion> addVersions(DatasetPath path, int numVersionsToAdd)
      throws Exception {
    Pair<DatasetVersion, DatasetVersion> pair;
    do {
      pair = addVersion(path);
    } while (--numVersionsToAdd > 0);
    return pair;
  }

  private Pair<DatasetVersion, DatasetVersion> addVersion(DatasetPath path) throws Exception {
    DatasetVersion latestVersion;
    DatasetVersion previousVersion;
    VirtualDatasetUI vds = mutator.get(path);
    // The version is based on system millis, make sure there is at least 1ms between versions.
    Thread.sleep(1);
    latestVersion = DatasetVersion.newVersion();
    previousVersion = vds.getVersion();
    vds.setVersion(latestVersion);
    vds.setPreviousVersion(
        new NameDatasetRef(DatasetPath.defaultImpl(vds.getFullPathList()).toString())
            .setDatasetVersion(previousVersion.getVersion()));
    mutator.putVersion(vds);
    return Pair.of(latestVersion, previousVersion);
  }

  private void createView() {
    final String query =
        "CREATE  VDS " + FULL_VIEW_NAME + " AS SELECT * FROM INFORMATION_SCHEMA.\"tables\"";

    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
            .setQueryType(QueryType.UI_PREVIEW)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }
}
