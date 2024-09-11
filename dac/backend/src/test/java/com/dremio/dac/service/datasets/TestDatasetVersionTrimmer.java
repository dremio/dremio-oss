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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.server.BaseTestServerJunit5;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link DatasetVersionTrimmer}. */
public class TestDatasetVersionTrimmer extends BaseTestServerJunit5 {
  private static final String SPACE_NAME1 = "space1";
  private static final String VIEW_NAME = "view";
  private static final String FULL_VIEW_NAME1 = SPACE_NAME1 + "." + VIEW_NAME;

  private static final String SPACE_NAME2 = "space2";
  private static final String FULL_VIEW_NAME2 = SPACE_NAME2 + "." + VIEW_NAME;
  private static final String FULL_VIEW_NAME2_UPPER = SPACE_NAME2.toUpperCase() + "." + VIEW_NAME;

  private KVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
      datasetVersionsStore;

  private DatasetVersionMutator mutator;
  private NamespaceService namespaceService;

  @BeforeEach
  public void setup() throws Exception {
    clearAllDataExceptUser();

    KVStoreProvider provider = l(KVStoreProvider.class);
    datasetVersionsStore = provider.getStore(DatasetVersionMutator.VersionStoreCreator.class);

    namespaceService = getNamespaceService();

    mutator =
        new DatasetVersionMutator(
            getSabotContext().getKVStoreProvider(),
            getJobsService(),
            getCatalogService(),
            getOptionManager(),
            getSabotContext());

    // Create two spaces in lower case.
    createSpace(SPACE_NAME1);
    createSpace(SPACE_NAME2);

    // Create two views: one in lower case space1, one with upper SPACE2.
    createView(FULL_VIEW_NAME1);
    createView(FULL_VIEW_NAME2_UPPER);
  }

  @ParameterizedTest
  @ValueSource(strings = {FULL_VIEW_NAME1, FULL_VIEW_NAME2})
  public void testTrimVersions_deleteOne(String fullViewName) throws Exception {
    trimVersionsTest(new DatasetPath(fullViewName), 1);
  }

  @ParameterizedTest
  @ValueSource(strings = {FULL_VIEW_NAME1, FULL_VIEW_NAME2})
  public void testTrimVersions_deleteMany(String fullViewName) throws Exception {
    trimVersionsTest(new DatasetPath(fullViewName), 50);
  }

  @ParameterizedTest
  @ValueSource(strings = {FULL_VIEW_NAME1, FULL_VIEW_NAME2})
  public void testTrimVersions_noChanges(String fullViewName) throws Exception {
    DatasetPath viewPath = new DatasetPath(fullViewName);

    addVersions(viewPath, 9);
    List<VirtualDatasetUI> versionsBefore = getAllVersions(viewPath);

    DatasetVersionTrimmer.trimHistory(
        Clock.systemUTC(), datasetVersionsStore, namespaceService, 10, 30);

    // Verify versions are the same.
    List<VirtualDatasetUI> versionsAfter = getAllVersions(viewPath);
    assertEquals(versionsBefore, versionsAfter);
  }

  @ParameterizedTest
  @ValueSource(strings = {FULL_VIEW_NAME1, FULL_VIEW_NAME2})
  public void testTrimVersions_maxVersionsAreKept(String fullViewName) throws Exception {
    DatasetPath viewPath = new DatasetPath(fullViewName);

    addVersion(viewPath);
    addVersion(viewPath);
    DatasetVersion latestVersion = addVersion(viewPath).getLeft();

    // Should not trim with system clock as age is under 30 days.
    DatasetVersionTrimmer.trimHistory(
        Clock.systemUTC(), datasetVersionsStore, namespaceService, 3, 30);
    List<VirtualDatasetUI> versionsAfter = getAllVersions(viewPath);
    assertEquals(4, versionsAfter.size());
    assertEquals(latestVersion, versionsAfter.get(3).getVersion());

    // Should trim to 3 with clock in the future as age of last version is greater than 30 days.
    Clock clock = Clock.fixed(Instant.now().plus(100, ChronoUnit.DAYS), ZoneId.of("UTC"));
    DatasetVersionTrimmer.trimHistory(clock, datasetVersionsStore, namespaceService, 3, 30);
    versionsAfter = getAllVersions(viewPath);
    assertEquals(3, versionsAfter.size());
    assertEquals(latestVersion, versionsAfter.get(2).getVersion());
  }

  @ParameterizedTest
  @ValueSource(strings = {FULL_VIEW_NAME1, FULL_VIEW_NAME2})
  public void testTrimVersions_loopIgnored(String fullViewName) throws Exception {
    DatasetPath viewPath = new DatasetPath(fullViewName);

    // There are 4 versions after these calls.
    DatasetVersion versionBeforeLoop = addVersion(viewPath).getLeft();
    addVersionLoop(viewPath);
    addVersion(viewPath);

    // Should not trim because of the loop, should remove the loop and point dataset to the
    // version before the loop.
    DatasetVersionTrimmer.trimHistory(
        Clock.systemUTC(), datasetVersionsStore, namespaceService, 1, 30);
    List<VirtualDatasetUI> versionsAfter = getAllVersions(viewPath);
    assertEquals(2, versionsAfter.size());
    DatasetConfig datasetConfig = namespaceService.getDataset(viewPath.toNamespaceKey());
    assertEquals(versionBeforeLoop, datasetConfig.getVirtualDataset().getVersion());
  }

  @ParameterizedTest
  @ValueSource(strings = {FULL_VIEW_NAME1, FULL_VIEW_NAME2})
  public void testTrimVersions_corruptedListIgnored(String fullViewName) throws Exception {
    DatasetPath viewPath = new DatasetPath(fullViewName);

    // There are 4 versions after these calls.
    addVersion(viewPath);
    addVersion(viewPath);
    addVersion(viewPath);

    // Corrupt latest version.
    VirtualDatasetUI vds = mutator.get(viewPath);
    vds.setPreviousVersion(
        new NameDatasetRef(DatasetPath.defaultImpl(vds.getFullPathList()).toString())
            .setDatasetVersion(DatasetVersion.newVersion().getVersion()));
    mutator.put(vds);

    // Should not trim because of the corruption.
    List<VirtualDatasetUI> versionsBefore = getAllVersions(viewPath);
    DatasetVersionTrimmer.trimHistory(
        Clock.systemUTC(), datasetVersionsStore, namespaceService, 1, 30);
    List<VirtualDatasetUI> versionsAfter = getAllVersions(viewPath);
    assertEquals(versionsBefore.size(), versionsAfter.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {FULL_VIEW_NAME2})
  public void testTrimVersions_corruptedHeadVersion(String fullViewName) throws Exception {
    DatasetPath viewPath = new DatasetPath(fullViewName);

    // There are 4 versions after these calls.
    addVersion(viewPath);
    addVersion(viewPath);
    DatasetVersion latestVersion = addVersion(viewPath).getLeft();

    // Corrupt pointer in the dataset.
    DatasetConfig datasetConfig = namespaceService.getDataset(viewPath.toNamespaceKey());
    DatasetVersion invalidVersion = DatasetVersion.newVersion();
    datasetConfig.getVirtualDataset().setVersion(invalidVersion);
    namespaceService.addOrUpdateDataset(viewPath.toNamespaceKey(), datasetConfig);

    // Should not trim the list and should update dataset to point to latest version.
    List<VirtualDatasetUI> versionsBefore = getAllVersions(viewPath);
    DatasetVersionTrimmer.trimHistory(
        Clock.systemUTC(), datasetVersionsStore, namespaceService, 1, 30);
    List<VirtualDatasetUI> versionsAfter = getAllVersions(viewPath);
    assertThat(versionsAfter).hasSize(versionsBefore.size());
    assertThat(
            namespaceService.getDataset(viewPath.toNamespaceKey()).getVirtualDataset().getVersion())
        .isEqualTo(latestVersion);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0_" + FULL_VIEW_NAME1,
        "0_" + FULL_VIEW_NAME2,
        "1_" + FULL_VIEW_NAME1,
        "1_" + FULL_VIEW_NAME2
      })
  public void testTrimVersions_removeOrphanedVersions(String orphansAreOldAndfullViewName)
      throws Exception {
    String[] parts = orphansAreOldAndfullViewName.split("_");
    boolean orphansAreOld = parts[0].equals("1");
    DatasetPath viewPath = new DatasetPath(parts[1]);

    // There are two versions after this call.
    addVersion(viewPath);
    assertThat(getAllVersions(viewPath)).hasSize(2);

    // Delete dataset.
    DatasetConfig datasetConfig = namespaceService.getDataset(viewPath.toNamespaceKey());
    namespaceService.deleteDataset(viewPath.toNamespaceKey(), datasetConfig.getTag());
    assertThat(getAllVersions(viewPath)).hasSize(2);

    // Trim with the clock in the future so that the orhpaned versions were old enough to delete.
    Clock clock =
        Clock.fixed(Instant.now().plus(orphansAreOld ? 14 : 0, ChronoUnit.DAYS), ZoneId.of("UTC"));
    DatasetVersionTrimmer.trimHistory(clock, datasetVersionsStore, namespaceService, 1, 30);

    // Verify versions were deleted.
    if (orphansAreOld) {
      assertThat(getAllVersions(viewPath)).isEmpty();
    } else {
      assertThat(getAllVersions(viewPath)).hasSize(2);
    }
  }

  private void trimVersionsTest(DatasetPath viewPath, int numVersionsToAdd) throws Exception {
    Pair<DatasetVersion, DatasetVersion> versionsPair = addVersions(viewPath, numVersionsToAdd);
    DatasetVersion latestVersion = versionsPair.getLeft();
    DatasetVersion previousVersion = versionsPair.getRight();

    // Verify there are N + 1 versions and latest points to the previous one.
    List<VirtualDatasetUI> versions = getAllVersions(viewPath);
    assertEquals(numVersionsToAdd + 1, versions.size());
    VirtualDatasetUI latestVersionVds =
        versions.stream().filter(v -> v.getVersion().equals(latestVersion)).findFirst().get();
    assertEquals(
        previousVersion.getVersion(), latestVersionVds.getPreviousVersion().getDatasetVersion());

    Clock clock = Clock.fixed(Instant.now().plus(100, ChronoUnit.DAYS), ZoneId.of("UTC"));
    DatasetVersionTrimmer.trimHistory(clock, datasetVersionsStore, namespaceService, 1, 30);

    // Verify there is one remaining version pointing to null.
    versions = getAllVersions(viewPath);
    assertEquals(1, versions.size());
    assertEquals(latestVersion, versions.get(0).getVersion());
    assertNull(versions.get(0).getPreviousVersion());
  }

  private List<VirtualDatasetUI> getAllVersions(DatasetPath path) throws Exception {
    if (path.equals(new DatasetPath(FULL_VIEW_NAME2))) {
      // Convert to "canonical" case as mutator will not do it if the dataset was deleted.
      path = new DatasetPath(FULL_VIEW_NAME2_UPPER);
    }
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

    // Update dataset so that it points to the latest version.
    DatasetConfig updatedDatasetConfig = DatasetsUtil.toVirtualDatasetVersion(vds).getDataset();
    namespaceService.addOrUpdateDataset(
        new NamespaceKey(updatedDatasetConfig.getFullPathList()), updatedDatasetConfig);

    return Pair.of(latestVersion, previousVersion);
  }

  private Pair<DatasetVersion, DatasetVersion> addVersionLoop(DatasetPath path) throws Exception {
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
            // Loop back to itself.
            .setDatasetVersion(latestVersion.getVersion()));
    mutator.putVersion(vds);

    // Update dataset so that it points to the latest version.
    DatasetConfig updatedDatasetConfig = DatasetsUtil.toVirtualDatasetVersion(vds).getDataset();
    namespaceService.addOrUpdateDataset(
        new NamespaceKey(updatedDatasetConfig.getFullPathList()), updatedDatasetConfig);

    return Pair.of(latestVersion, previousVersion);
  }

  private void createSpace(String name) throws Exception {
    SpaceConfig config = new SpaceConfig().setName(name);
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
  }

  private void createView(String fullViewName) {
    final String query =
        "CREATE  VDS " + fullViewName + " AS SELECT * FROM INFORMATION_SCHEMA.\"tables\"";

    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
            .setQueryType(QueryType.UI_PREVIEW)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }
}
