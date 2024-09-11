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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.common.VM;
import com.dremio.common.perf.Timer;
import com.dremio.common.utils.ProtostuffUtil;
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
import com.dremio.datastore.CheckpointInfo;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.jobs.JobsService;
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
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Test backup and restore. */
@RunWith(Parameterized.class)
public class ITBackupManager extends BaseTestServer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ITBackupManager.class);

  private static final boolean FLAG_BACKUP_JSON = false;

  @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static DACConfig dacConfig =
      DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
          .autoPort(true)
          .allowTestApis(true)
          .serveUI(false)
          .inMemoryStorage(false)
          .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
          .clusterMode(DACDaemon.ClusterMode.LOCAL);

  private static FileSystem fs;

  @BeforeClass
  public static void init() throws Exception {
    Assume.assumeFalse(BaseTestServer.isMultinode());
    fs = HadoopFileSystem.getLocal(new Configuration());
    try (Timer.TimedBlock b = Timer.time("ITBackupManager.init")) {
      logger.info("Running tests in local mode ");
      dacConfig = dacConfig.writePath(folder1.newFolder().getAbsolutePath());
      startDaemon();
    }
    populateInitialData();
  }

  private static void startDaemon() throws Exception {
    startDaemon(dacConfig);
  }

  private static void startDaemon(DACConfig dacConfig) throws Exception {
    setCurrentDremioDaemon(DACDaemon.newDremioDaemon(dacConfig, DremioTest.CLASSPATH_SCAN_RESULT));
    getCurrentDremioDaemon().init();
    initClient();
    TestUtilities.addClasspathSourceIf(l(CatalogService.class));
    setPopulator(
        new SampleDataPopulator(
            getSabotContext(),
            getSourceService(),
            getDatasetVersionMutator(),
            getUserService(),
            getNamespaceService(),
            DEFAULT_USERNAME,
            getCollaborationHelper()));
  }

  private final String mode;

  public ITBackupManager(String mode) {
    this.mode = mode;
  }

  @Parameters(name = "mode={0}")
  public static Iterable<? extends Object> data() {
    return Arrays.asList("json", "binary");
  }

  @Test
  public void testBackupTableKey() throws Exception {
    testBackup1table1key("configuration", "clusterId");
  }

  @Test
  public void testBackupTable() throws Exception {
    testBackup1table1key("configuration", "");
  }

  @Test
  public void testBackupTableJobs() throws Exception {
    testBackup1table1key("jobs", "");
  }

  @Test
  public void testBackupTableDataset() throws Exception {
    testBackup1table1key("datasetVersions", "");
  }

  @Test
  public void testBackupWrongKey() throws Exception {
    testBackup1table1key("configuration", "test");
  }

  private void testBackup1table1key(String table, String key) throws Exception {
    if ("binary".equals(mode)) {
      return; // only test/run on 'json' mode
    }

    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);
    Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    LocalKVStoreProvider localKVStoreProvider =
        l(LegacyKVStoreProvider.class).unwrap(LocalKVStoreProvider.class);
    HomeFileConf homeFileStore =
        getCatalogService()
            .getManagedSource(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME)
            .getId()
            .getConnectionConf();

    // take backup 1
    BackupOptions backupOptions =
        new BackupOptions(
            BaseTestServer.folder1.newFolder().getAbsolutePath(),
            FLAG_BACKUP_JSON,
            false,
            "",
            table,
            key);
    Path backupDir =
        Path.of(
            BackupRestoreUtil.createBackup(
                    fs,
                    backupOptions,
                    localKVStoreProvider,
                    homeFileStore,
                    dacConfig.getConfig(),
                    null,
                    true)
                .getBackupPath());

    // Verify the table "configuration" and the key are in the backup
    Path path = backupDir.resolve(table + "_backup.json");

    File file = new File(path.toString());
    Map<String, ConfigurationEntry> map = new HashMap<>();
    String keyInDataset = null;
    try (final BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      final ObjectMapper objectMapper = new ObjectMapper();
      String line;
      while ((line = reader.readLine()) != null) {
        JsonNode jsonNode = objectMapper.readTree(line);
        String key1 = jsonNode.get("key").asText();
        ConfigurationEntry entry =
            ProtostuffUtil.fromJSON(
                jsonNode.get("value").asText(), ConfigurationEntry.getSchema(), false);
        map.put(key1, entry);
        keyInDataset = key1;
      }
    }

    if ("jobs".equals(table) || "test".equals(key)) { // empty data/table
      assertThat(map).hasSize(0);
    } else if (!key.isEmpty()) {
      assertThat(map).hasSize(1);
      assertThat(map).containsKey(key);
    } else {
      assertTrue(map.size() > 1);
      if ("datasetVersions".equals(table)) { // for 'datasetVersions', keep testing with 1 key
        testBackup1table1key(table, keyInDataset);
      }
    }
  }

  private void testBackup(String compression) throws Exception {
    boolean binary = "binary".equals(mode);
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);
    Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    LocalKVStoreProvider localKVStoreProvider =
        l(LegacyKVStoreProvider.class).unwrap(LocalKVStoreProvider.class);
    HomeFileConf homeFileStore =
        getCatalogService()
            .getManagedSource(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME)
            .getId()
            .getConnectionConf();

    // take backup 1
    CheckPoint cp1 = checkPoint();
    Path backupDir1 =
        Path.of(
            BackupRestoreUtil.createBackup(
                    fs,
                    new BackupOptions(
                        BaseTestServer.folder1.newFolder().getAbsolutePath(),
                        binary,
                        false,
                        compression,
                        "",
                        ""),
                    localKVStoreProvider,
                    homeFileStore,
                    dacConfig.getConfig(),
                    null,
                    true)
                .getBackupPath());

    // add dataset, delete dataset, upload file
    getPopulator()
        .putDS(
            "DG",
            "dsg11",
            new FromSQL("select * from DG.dsg9 t1 left join DG.dsg8 t2 on t1.A=t2.age").wrap());
    DatasetPath datasetPath = new DatasetPath("DG.dsg10");
    getDatasetVersionMutator()
        .deleteDataset(
            datasetPath,
            NamespaceUtils.getVersion(datasetPath.toNamespaceKey(), getNamespaceService()));

    File tmpFile = TEMP_FOLDER.newFile();
    Files.write(readResourceAsString("/datasets/text/comma.txt"), tmpFile, UTF_8);
    Path textFile = Path.of(tmpFile.getAbsolutePath());
    TestHomeFiles.uploadFile(
        homeFileStore, textFile, "comma", "txt", new TextFileConfig().setFieldDelimiter(","), null);

    try (BufferAllocator allocator =
        getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE)) {
      runQuery(l(JobsService.class), "comma", 4, 3, null, allocator);
    }
    CheckPoint cp2 = checkPoint();

    // take backup 2 using rest api
    final URI backupPath = BaseTestServer.folder1.newFolder().getAbsoluteFile().toURI();
    Path backupDir2 =
        Path.of(
            Backup.createBackup(
                    dacConfig,
                    () -> null,
                    DEFAULT_USERNAME,
                    DEFAULT_PASSWORD,
                    false,
                    backupPath,
                    binary,
                    false,
                    compression,
                    "",
                    "")
                .getBackupPath());

    // destroy everything
    l(HomeFileTool.class).clearUploads();
    localKVStoreProvider.deleteEverything();
    closeCurrentDremioDaemon();

    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);

    // restore
    BackupRestoreUtil.restore(fs, backupDir2, dacConfig);

    // restart
    startDaemon(dacConfig);

    localKVStoreProvider = l(LegacyKVStoreProvider.class).unwrap(LocalKVStoreProvider.class);
    cp2.checkEquals(checkPoint());
    DatasetConfig dsg11 =
        getNamespaceService().getDataset(new DatasetPath("DG.dsg11").toNamespaceKey());
    assertNotNull(dsg11);
    assertEquals("dsg11", dsg11.getName());

    try {
      getNamespaceService().getDataset(datasetPath.toNamespaceKey());
      fail("DG.dsg10 should have been deleted in backup 2");
    } catch (NamespaceNotFoundException e) {
    }

    // query uploaded file
    try (BufferAllocator allocator =
        getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE)) {
      runQuery(l(JobsService.class), "comma", 4, 3, null, allocator);
    }

    // destroy everything
    l(HomeFileTool.class).clearUploads();
    localKVStoreProvider.deleteEverything();
    closeCurrentDremioDaemon();

    // recreate dirs
    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);

    // restore
    BackupRestoreUtil.restore(fs, backupDir1, dacConfig);
    // restart
    startDaemon(dacConfig);

    cp1.checkEquals(checkPoint());
    DatasetConfig dsg10 = getNamespaceService().getDataset(datasetPath.toNamespaceKey());
    assertNotNull(dsg10);
    assertEquals("dsg10", dsg10.getName());

    try {
      getNamespaceService().getDataset(new DatasetPath("DG.dsg11").toNamespaceKey());
      fail("DG.dsg11 should not be present in backup 1");
    } catch (NamespaceNotFoundException e) {
    }

    try {
      getNamespaceService().getDataset(new DatasetPath("@tshiran.comma").toNamespaceKey());
      fail("@tshiran.comma should not be present in backup1");
    } catch (NamespaceNotFoundException e) {
    }
  }

  /** Test backup and restore for all the compression methods available. */
  @Ignore("DX-89937 fix and re-enable")
  @Test
  public void testBackup() throws Exception {
    testBackup("");
  }

  @Ignore("DX-89937 fix and re-enable")
  @Test
  public void testSnappyCompressionBackup() throws Exception {
    testBackup("snappy");
  }

  @Ignore("DX-89937 fix and re-enable")
  @Test
  public void testLZ4CompressionBackup() throws Exception {
    testBackup("lz4");
  }

  @Ignore("DX-89937 fix and re-enable")
  @Test
  public void testNullCompressionBackup() throws Exception {
    testBackup(null);
  }

  /**
   * Test backup and restore with large sql (exceeding SimpleDocumentWriter.MAX_STRING_LENGTH)
   * containing only ascii characters.
   */
  @Test
  public void testLargeSqlQueryWithOnlyAscii() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 3500; i++) {
      sb.append("0123456789");
    }
    backupRestoreTestHelper("dsg16", "dsg17", "select '" + sb.toString() + "' as text");
  }

  /**
   * Test backup and restore with large sql (exceeding SimpleDocumentWriter.MAX_STRING_LENGTH)
   * containing mix of ascii and non-ascii (2-byte) characters.
   */
  @Test
  public void testLargeSqlQueryWithNonAscii() throws Exception {
    // Prepare a large sql, such that truncation happens at middle of two-byte char,
    // resulting in incorrect last char. With this, test backup and restore should have no issues.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 27000 / 26; i++) {
      sb.append("abcdefghijklmnopqrstuvwxyz");
    }
    sb.append("123");
    for (int i = 0; i < 400; i++) {
      sb.append("\u00E4\u00FC\u00F6\u00E4\u00FC\u00F6\u00E4\u00FC\u00F6\u00E4");
    }
    backupRestoreTestHelper("dsg14", "dsg15", "select '" + sb.toString() + "' as text");
  }

  @Ignore("DX-89937 fix and re-enable")
  @Test
  public void testLocalAttach() throws Exception {
    backupRestoreTestHelper(
        "dsg12", "dsg13", "select * from DG.dsg9 t1 left join DG.dsg8 t2 on t1.A=t2.age");
  }

  @Test
  public void testBackupRestoreOnCli() throws Exception {
    boolean binary = "binary".equals(mode);
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);
    final Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    String username = DEFAULT_USERNAME;
    String password = DEFAULT_PASSWORD;
    File backupDir = TEMP_FOLDER.newFolder();
    String[] args;
    if (binary) {
      args = new String[] {"-u", username, "-p", password, "-d", backupDir.getAbsolutePath(), "-s"};
    } else {
      args =
          new String[] {
            "-u", username, "-p", password, "-d", backupDir.getAbsolutePath(), "-s", "-j"
          };
    }

    // Create an initial checkpoint with the current populated data
    final CheckPoint cpBeforeBackup = checkPoint();

    Backup.BackupResult backupResult = Backup.doMain(args, dacConfig);
    assertEquals("backup must finish with exit code 0", 0, backupResult.getExitStatus());

    // Restore
    closeCurrentDremioDaemon();
    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);
    BackupRestoreUtil.restore(
        fs, Path.of(backupResult.getBackupStats().get().getBackupPath()), dacConfig);
    startDaemon(dacConfig);

    final CheckPoint cpAfterRestore = checkPoint();
    cpAfterRestore.checkEquals(cpBeforeBackup);
  }

  @Test
  public void testBackupRestoreOnCliWithCheckpointParameter() throws Exception {
    boolean binary = "binary".equals(mode);
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);
    final Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    File backupDir = TEMP_FOLDER.newFolder();
    // Create an initial real checkpoint that we pass and reuse
    CheckpointInfo checkpoint =
        Backup.createCheckpoint(
            dacConfig,
            getCurrentDremioDaemon().getProvider(CredentialsService.class),
            DEFAULT_USERNAME,
            DEFAULT_PASSWORD,
            false,
            backupDir.toURI(),
            binary,
            false);

    // Keep this checkpoint for test after restore
    CheckPoint cpBeforeBackup = checkPoint();
    String[] args;
    if (binary) {
      args =
          new String[] {
            "-d", backupDir.getAbsolutePath(), "-s", "--checkpoint", checkpoint.getCheckpointPath()
          };
    } else {
      args =
          new String[] {
            "-d",
            backupDir.getAbsolutePath(),
            "-s",
            "-j",
            "--checkpoint",
            checkpoint.getCheckpointPath()
          };
    }

    Backup.BackupResult backupResult = Backup.doMain(args, dacConfig);
    assertEquals("backup must finish with exit code 0", 0, backupResult.getExitStatus());

    // Restore
    closeCurrentDremioDaemon();
    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);
    BackupRestoreUtil.restore(
        fs, Path.of(backupResult.getBackupStats().get().getBackupPath()), dacConfig);
    startDaemon(dacConfig);

    final CheckPoint cpAfterRestore = checkPoint();
    cpAfterRestore.checkEquals(cpBeforeBackup);
  }

  @Test
  public void testBackupRestoreOnCliWithNotFoundCheckpointParameter() throws Exception {
    boolean binary = "binary".equals(mode);
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);

    File backupDir = TEMP_FOLDER.newFolder();

    String invalidCheckpointPath = "///abcdef////";

    String[] args;
    if (binary) {
      args =
          new String[] {
            "-d", backupDir.getAbsolutePath(), "-s", "--checkpoint", invalidCheckpointPath
          };
    } else {
      args =
          new String[] {
            "-d", backupDir.getAbsolutePath(), "-s", "-j", "--checkpoint", invalidCheckpointPath
          };
    }

    Backup.BackupResult backupResult = Backup.doMain(args, dacConfig);
    assertEquals("backup must finish with exit code 1", 1, backupResult.getExitStatus());
  }

  @Test
  public void testBackupTableKeyOnCli() throws Exception {
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);
    final Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    String username = DEFAULT_USERNAME;
    String password = DEFAULT_PASSWORD;
    File backupDir = TEMP_FOLDER.newFolder();
    String table = "configuration";
    String key = "clusterId";
    String[] args;
    args =
        new String[] {
          "-u",
          username,
          "-p",
          password,
          "-d",
          backupDir.getAbsolutePath(),
          "-s",
          "-j",
          "-t",
          table,
          "-k",
          key
        };

    Backup.BackupResult backupResult = Backup.doMain(args, dacConfig);
    assertEquals("backup must finish with exit code 0", 0, backupResult.getExitStatus());

    // Verify the table "configuration" and the key are in the backup
    String[] names = backupDir.list();
    String path = backupDir.getPath() + "/" + names[0] + "/" + table + "_backup.json";

    File file = new File(path);
    Map<String, ConfigurationEntry> map = new HashMap<>();
    try (final BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      final ObjectMapper objectMapper = new ObjectMapper();
      String line;
      while ((line = reader.readLine()) != null) {
        JsonNode jsonNode = objectMapper.readTree(line);
        String key1 = jsonNode.get("key").asText();
        ConfigurationEntry entry =
            ProtostuffUtil.fromJSON(
                jsonNode.get("value").asText(), ConfigurationEntry.getSchema(), false);
        map.put(key1, entry);
      }
    }

    assertThat(map).hasSize(1);
    assertThat(map).containsKey(key);
  }

  @Test
  public void testBackupBinaryTableKeyOnCli() throws Exception {
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);
    final Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    String username = DEFAULT_USERNAME;
    String password = DEFAULT_PASSWORD;
    File backupDir = TEMP_FOLDER.newFolder();
    String table = "configuration";
    String key = "clusterId";
    String[] args;
    args =
        new String[] {
          "-u",
          username,
          "-p",
          password,
          "-d",
          backupDir.getAbsolutePath(),
          "-s",
          "-t",
          table,
          "-k",
          key
        };

    Backup.BackupResult backupResult = Backup.doMain(args, dacConfig);
    assertEquals("backup must finish with exit code 1", 1, backupResult.getExitStatus());
  }

  @Test
  public void testBackupEmptyTableKeyOnCli() throws Exception {
    int httpPort = getCurrentDremioDaemon().getWebServer().getPort();
    DACConfig dacConfig = ITBackupManager.dacConfig.httpPort(httpPort);
    final Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    String username = DEFAULT_USERNAME;
    String password = DEFAULT_PASSWORD;
    File backupDir = TEMP_FOLDER.newFolder();
    String table = "";
    String key = "clusterId";
    String[] args;
    args =
        new String[] {
          "-u",
          username,
          "-p",
          password,
          "-d",
          backupDir.getAbsolutePath(),
          "-s",
          "-t",
          table,
          "-k",
          key
        };

    Backup.BackupResult backupResult = Backup.doMain(args, dacConfig);
    assertEquals("backup must finish with exit code 1", 1, backupResult.getExitStatus());
  }

  private void backupRestoreTestHelper(String dsName1, String dsName2, String sql)
      throws Exception {
    boolean binary = "binary".equals(mode);
    Path dbDir = Path.of(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));
    LocalKVStoreProvider localKVStoreProvider =
        l(LegacyKVStoreProvider.class).unwrap(LocalKVStoreProvider.class);
    HomeFileConf homeFileStore =
        getCatalogService()
            .getManagedSource(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME)
            .getId()
            .getConnectionConf();

    final String tempPath = TEMP_FOLDER.getRoot().getAbsolutePath();

    Path backupDir1 =
        Path.of(
            BackupRestoreUtil.createBackup(
                    fs,
                    new BackupOptions(
                        BaseTestServer.folder1.newFolder().getAbsolutePath(),
                        binary,
                        false,
                        "",
                        "",
                        ""),
                    localKVStoreProvider,
                    homeFileStore,
                    dacConfig.getConfig(),
                    null,
                    true)
                .getBackupPath());

    // Do some things
    getPopulator().putDS("DG", dsName1, new FromSQL(sql).wrap());
    CheckPoint cp = checkPoint();

    // Backup
    final String[] backupArgs;
    if (binary) {
      backupArgs = new String[] {"backup", tempPath, "true", "false"};
    } else {
      backupArgs = new String[] {"backup", tempPath, "false", "false"};
    }
    final String vmid = VM.getProcessId();
    DremioAttach.main(vmid, backupArgs);
    // verify that repeated backup calls don't cause an exception
    DremioAttach.main(vmid, backupArgs);

    // Destroy everything
    l(HomeFileTool.class).clearUploads();
    localKVStoreProvider.deleteEverything();
    closeCurrentDremioDaemon();

    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);

    // Get last modified backup file
    final Optional<java.nio.file.Path> restorePath = findLastModifiedBackup(tempPath);

    // verify that backup in default(binary) format has some number of
    // files with .pb extension
    final File[] files = new File(restorePath.get().toString()).listFiles();
    final int binaryFilesCount =
        (int)
            Arrays.stream(files)
                .filter(file -> FilenameUtils.getExtension(file.getName()).equals("pb"))
                .count();

    if (binary) {
      assertNotEquals(0, binaryFilesCount);
    } else {
      assertEquals(0, binaryFilesCount);
    }
    // restore
    if (!restorePath.isPresent()) {
      throw new AssertionError("Could not find restore directory.");
    }
    BackupRestoreUtil.restore(fs, Path.of(restorePath.get().toString()), dacConfig);

    // restart
    startDaemon();

    // verify
    cp.checkEquals(checkPoint());

    // try adding something else, should not match checkpoint
    getPopulator().putDS("DG", dsName2, new FromSQL(sql).wrap());
    try {
      cp.checkEquals(checkPoint());
      throw new AssertionError();
    } catch (AssertionError ignored) {
    }

    // destroy everything
    l(HomeFileTool.class).clearUploads();
    localKVStoreProvider.deleteEverything();
    closeCurrentDremioDaemon();

    // recreate dirs
    fs.delete(dbDir, true);
    fs.mkdirs(dbDir);

    // restore
    BackupRestoreUtil.restore(fs, backupDir1, dacConfig);
    // restart
    startDaemon(dacConfig);
  }

  @Nonnull
  private static Optional<java.nio.file.Path> findLastModifiedBackup(String workingDir)
      throws IOException {
    try (Stream<java.nio.file.Path> stream = java.nio.file.Files.list(Paths.get(workingDir))) {
      return stream
          .filter(java.nio.file.Files::isDirectory)
          .filter(f -> f.getFileName().toString().startsWith("dremio_backup"))
          .max(Comparator.comparingLong(f -> f.toFile().lastModified()));
    }
  }

  private CheckPoint checkPoint() throws Exception {
    CheckPoint checkPoint = new CheckPoint();
    NamespaceService namespaceService = getNamespaceService();
    UserService userService = getUserService();
    JobsService jobsService = getJobsService();

    checkPoint.sources = namespaceService.getSources();
    checkPoint.spaces = namespaceService.getSpaces();
    checkPoint.homes = namespaceService.getHomeSpaces();
    checkPoint.users = Arrays.asList(Iterables.toArray(userService.getAllUsers(10000), User.class));
    checkPoint.datasets = Lists.newArrayList();
    checkPoint.virtualDatasetVersions = Lists.newArrayList();

    /**
     * DX-4498 for (NamespaceKey ds : namespaceService.getAllDatasets(new NamespaceKey(""))) {
     * DatasetConfig datasetConfig = namespaceService.getDataset(ds);
     * checkPoint.datasets.add(datasetConfig); checkPoint.virtualDatasetVersions.addAll(
     * Arrays.asList(Iterables.toArray(datasetService.getAllVersions(new
     * DatasetPath(ds.getPathComponents())), VirtualDatasetUI.class))); if
     * (datasetConfig.getAccelerationId() != null) {
     * checkPoint.accelerations.add(accelerationService.getAccelerationById(new
     * AccelerationId(datasetConfig.getAccelerationId()))); } }
     */
    final SearchJobsRequest request =
        SearchJobsRequest.newBuilder().setFilterString("").setUserName("tshiran").build();

    checkPoint.jobs = ImmutableList.copyOf(jobsService.searchJobs(request));
    return checkPoint;
  }

  private static final class CheckPoint {
    private List<SourceConfig> sources;
    private List<SpaceConfig> spaces;
    private List<HomeConfig> homes;
    private List<DatasetConfig> datasets;
    private List<User> users;
    private List<VirtualDatasetUI> virtualDatasetVersions;
    private List<JobSummary> jobs;

    private void checkEquals(CheckPoint o) {
      assertThat(sources).containsExactlyInAnyOrderElementsOf(o.sources);
      assertThat(spaces).containsExactlyInAnyOrderElementsOf(o.spaces);
      assertThat(homes).containsExactlyInAnyOrderElementsOf(o.homes);
      assertThat(datasets).containsExactlyInAnyOrderElementsOf(o.datasets);
      assertThat(users).containsExactlyInAnyOrderElementsOf(o.users);
      assertThat(virtualDatasetVersions)
          .containsExactlyInAnyOrderElementsOf(o.virtualDatasetVersions);
      assertThat(jobs).containsExactlyInAnyOrderElementsOf(o.jobs);
    }
  }
}
