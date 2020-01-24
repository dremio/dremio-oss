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
package com.dremio.dac.cmd;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.dremio.common.perf.Timer;
import com.dremio.common.util.FileUtils;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.homefiles.HomeFileSystemStoragePlugin;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.TestHomeFiles;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.dac.util.BackupRestoreUtil.BackupOptions;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SearchJobsRequest;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.User;
import com.dremio.service.users.UserService;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Test backup and restore.
 */
@RunWith(Parameterized.class)
public class TestBackupManager extends BaseTestServer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBackupManager.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static DACConfig dacConfig =  DACConfig
    .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
    .autoPort(true)
    .allowTestApis(true)
    .serveUI(false)
    .inMemoryStorage(false)
    .clusterMode(DACDaemon.ClusterMode.LOCAL);

  @BeforeClass
  public static void init() throws Exception {
    Assume.assumeFalse(BaseTestServer.isMultinode());
    try (Timer.TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      logger.info("Running tests in local mode ");
      dacConfig = dacConfig.writePath(folder1.newFolder().getAbsolutePath());
      startDaemon();
    }
  }

  private static void startDaemon() throws Exception {
    setCurrentDremioDaemon(DACDaemon.newDremioDaemon(dacConfig, DremioTest.CLASSPATH_SCAN_RESULT));
    setMasterDremioDaemon(null);
    getCurrentDremioDaemon().init();
    initClient();
    setBinder(createBinder(getCurrentDremioDaemon().getBindingProvider()));
    TestUtilities.addClasspathSourceIf(l(CatalogService.class));
    setPopulator(new SampleDataPopulator(
      l(SabotContext.class),
      newSourceService(),
      newDatasetVersionMutator(),
      l(UserService.class),
      newNamespaceService(),
      DEFAULT_USERNAME
    ));
  }

  private final String mode;
  public TestBackupManager(String mode) {
    this.mode = mode;
  }

  @Parameters(name="mode={0}")
  public static Iterable<? extends Object> data() {
      return Arrays.asList("json", "binary");
  }

  @Test
  public void testBackup() throws Exception {
    boolean binary = "binary".equals(mode);
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    populateInitialData();
    Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    dacConfig = dacConfig.httpPort(httpPort);

    LocalKVStoreProvider localKVStoreProvider = (LocalKVStoreProvider) l(KVStoreProvider.class);
    HomeFileConf homeFileStore = ((CatalogServiceImpl) l(CatalogService.class)).getManagedSource(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME).getId().getConnectionConf();

    // take backup 1
    CheckPoint cp1 = checkPoint();
    Path backupDir1 = Path.of(BackupRestoreUtil.createBackup(
      fs, new BackupOptions(BaseTestServer.folder1.newFolder().getAbsolutePath(), binary, false), localKVStoreProvider, homeFileStore).getBackupPath());

    // add dataset, delete dataset, upload file
    getPopulator().putDS("DG", "dsg11", new FromSQL("select * from DG.dsg9 t1 left join DG.dsg8 t2 on t1.A=t2.age").wrap());
    DatasetPath datasetPath = new DatasetPath("DG.dsg10");
    newDatasetVersionMutator().deleteDataset(datasetPath, NamespaceUtils.getVersion(datasetPath.toNamespaceKey(), newNamespaceService()));

    File tmpFile = TEMP_FOLDER.newFile();
    Files.write(FileUtils.getResourceAsString("/datasets/text/comma.txt"), tmpFile, UTF_8);
    Path textFile = Path.of(tmpFile.getAbsolutePath());
    TestHomeFiles.uploadFile(homeFileStore, textFile, "comma", "txt", new TextFileConfig().setFieldDelimiter(","), null);

    TestHomeFiles.runQuery("comma", 4, 3, null);
    CheckPoint cp2 = checkPoint();

    // take backup 2 using rest api
    final URI backupPath = BaseTestServer.folder1.newFolder().getAbsoluteFile().toURI();
    Path backupDir2 = Path.of(
      Backup.createBackup(dacConfig, DEFAULT_USERNAME, DEFAULT_PASSWORD, false, backupPath, binary, false)
      .getBackupPath());

    // destroy everything
    l(HomeFileTool.class).clearUploads();
    localKVStoreProvider.deleteEverything();
    getCurrentDremioDaemon().close();

    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);

    // restore
    BackupRestoreUtil.restore(fs, backupDir2, dacConfig);

    // restart
    startDaemon();

    localKVStoreProvider = (LocalKVStoreProvider) l(KVStoreProvider.class);
    cp2.checkEquals(checkPoint());
    DatasetConfig dsg11 = newNamespaceService().getDataset(new DatasetPath("DG.dsg11").toNamespaceKey());
    assertNotNull(dsg11);
    assertEquals("dsg11", dsg11.getName());

    try {
      newNamespaceService().getDataset(datasetPath.toNamespaceKey());
      fail("DG.dsg10 should have been deleted in backup 2");
    } catch (NamespaceNotFoundException e) {
    }

    // query uploaded file
    TestHomeFiles.runQuery("comma", 4, 3, null);

    // destroy everything
    l(HomeFileTool.class).clearUploads();
    localKVStoreProvider.deleteEverything();
    getCurrentDremioDaemon().close();

    // recreate dirs
    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);

    // restore
    BackupRestoreUtil.restore(fs, backupDir1, dacConfig);
    // restart
    startDaemon();

    cp1.checkEquals(checkPoint());
    DatasetConfig dsg10 = newNamespaceService().getDataset(datasetPath.toNamespaceKey());
    assertNotNull(dsg10);
    assertEquals("dsg10", dsg10.getName());

    try {
      newNamespaceService().getDataset(new DatasetPath("DG.dsg11").toNamespaceKey());
      fail("DG.dsg11 should not be present in backup 1");
    } catch (NamespaceNotFoundException e) {
    }

    try {
      newNamespaceService().getDataset(new DatasetPath("@tshiran.comma").toNamespaceKey());
      fail("@tshiran.comma should not be present in backup1");
    } catch (NamespaceNotFoundException e) {
    }
  }

  private CheckPoint checkPoint() throws Exception {
    CheckPoint checkPoint = new CheckPoint();
    NamespaceService namespaceService = newNamespaceService();
    UserService userService = l(UserService.class);
    JobsService jobsService = l(JobsService.class);

    checkPoint.sources = namespaceService.getSources();
    checkPoint.spaces = namespaceService.getSpaces();
    checkPoint.homes = namespaceService.getHomeSpaces();
    checkPoint.users = Arrays.asList(Iterables.toArray(userService.getAllUsers(10000), User.class));
    checkPoint.datasets = Lists.newArrayList();
    checkPoint.virtualDatasetVersions = Lists.newArrayList();

    /** DX-4498
    for (NamespaceKey ds : namespaceService.getAllDatasets(new NamespaceKey(""))) {
      DatasetConfig datasetConfig =  namespaceService.getDataset(ds);
      checkPoint.datasets.add(datasetConfig);
      checkPoint.virtualDatasetVersions.addAll(
        Arrays.asList(Iterables.toArray(datasetService.getAllVersions(new DatasetPath(ds.getPathComponents())), VirtualDatasetUI.class)));
      if (datasetConfig.getAccelerationId() != null) {
        checkPoint.accelerations.add(accelerationService.getAccelerationById(new AccelerationId(datasetConfig.getAccelerationId())));
      }
    }
     */
    final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setFilterString("")
        .setUsername("tshiran")
        .build();

    checkPoint.jobs = ImmutableList.copyOf(jobsService.searchJobs(request));
    return checkPoint;
  }

  private static final class CheckPoint {
    private List<SourceConfig> sources;
    private List<SpaceConfig> spaces;
    private List<HomeConfig> homes;
    private List<DatasetConfig> datasets;
    private List<? extends User> users;
    private List<VirtualDatasetUI> virtualDatasetVersions;
    private List<Job> jobs;

    private void checkEquals(CheckPoint o) {
      assertTrue(CollectionUtils.isEqualCollection(sources, o.sources));
      assertTrue(CollectionUtils.isEqualCollection(spaces, o.spaces));
      assertTrue(CollectionUtils.isEqualCollection(homes, o.homes));
      assertTrue(CollectionUtils.isEqualCollection(datasets, o.datasets));
      assertTrue(CollectionUtils.isEqualCollection(users, o.users));
      assertTrue(CollectionUtils.isEqualCollection(virtualDatasetVersions, o.virtualDatasetVersions));
      assertTrue(CollectionUtils.isEqualCollection(jobs, o.jobs));
    }
  }

}
