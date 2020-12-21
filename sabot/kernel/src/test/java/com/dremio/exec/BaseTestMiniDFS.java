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
package com.dremio.exec;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.dremio.PlanTestBase;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.dotfile.DotFileType;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.exec.store.dfs.WorkspaceConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class BaseTestMiniDFS extends PlanTestBase {
  protected static final String MINIDFS_STORAGE_PLUGIN_NAME = "miniDfsPlugin";
  protected static final String processUser = System.getProperty("user.name");

  protected static MiniDFSCluster dfsCluster;
  protected static Configuration dfsConf;
  protected static FileSystem fs;
  protected static String miniDfsStoragePath;

  /**
   * Start a MiniDFS cluster backed SabotNode cluster
   * @param testClass
   * @throws Exception
   */
  protected static Configuration startMiniDfsCluster(String testClass) throws Exception {
    Configuration configuration = new Configuration();
    startMiniDfsCluster(testClass, configuration);
    return configuration;
  }

  protected static void startMiniDfsCluster(final String testClass, Configuration configuration) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(testClass), "Expected a non-null and non-empty test class name");
    dfsConf = Preconditions.checkNotNull(configuration);

    // Set the MiniDfs base dir to be the temp directory of the test, so that all files created within the MiniDfs
    // are properly cleanup when test exits.
    miniDfsStoragePath = Files.createTempDirectory(testClass).toString();
    dfsConf.set("hdfs.minidfs.basedir", miniDfsStoragePath);
    // HDFS-8880 and HDFS-8953 introduce metrics logging that requires log4j, but log4j is explicitly
    // excluded in build. So disable logging to avoid NoClassDefFoundError for Log4JLogger.
    dfsConf.set("dfs.namenode.metrics.logger.period.seconds", "0");
    dfsConf.set("dfs.datanode.metrics.logger.period.seconds", "0");

    // Start the MiniDfs cluster
    dfsCluster = new MiniDFSCluster.Builder(dfsConf)
        .numDataNodes(3)
        .format(true)
        .build();

    fs = dfsCluster.getFileSystem();
  }

  protected static void addMiniDfsBasedStorage(boolean impersonationEnabled) throws Exception {
    // Create a HDFS based storage plugin (connection string for mini dfs is varies for each run).

    final CatalogService catalogService = getSabotContext().getCatalogService();
    final Path dirPath = new Path("/");
    FileSystem.mkdirs(fs, dirPath, new FsPermission((short)0777));
    fs.setOwner(dirPath, processUser, processUser);

    final InternalFileConf conf = new InternalFileConf();
    conf.connection = fs.getUri().toString();
    conf.path = "/";
    conf.enableImpersonation = impersonationEnabled;
    conf.mutability = SchemaMutability.ALL;

    final SourceConfig config = new SourceConfig();
    config.setName(MINIDFS_STORAGE_PLUGIN_NAME);
    config.setConnectionConf(conf);
    config.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE);
    ((CatalogServiceImpl) catalogService).getSystemUserCatalog().createSource(config);
  }

  protected static void createAndAddWorkspace(String name, String path, short permissions, String owner,
      String group, final Map<String, WorkspaceConfig> workspaces) throws Exception {
    final Path dirPath = new Path(path);
    FileSystem.mkdirs(fs, dirPath, new FsPermission(permissions));
    fs.setOwner(dirPath, owner, group);
    final WorkspaceConfig ws = new WorkspaceConfig(path, true, "parquet");
    workspaces.put(name, ws);
  }

  protected static void stopMiniDfsCluster() {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }

    if (miniDfsStoragePath != null) {
      FileUtils.deleteQuietly(new File(miniDfsStoragePath));
    }
  }

  // Return the user workspace for given user.
  protected static String getWSSchema(String user) {
    return getUserHome(user);
  }

  protected static String getUserHome(String user) {
    return "/user/" + user;
  }

  protected static void createView(final String viewOwner, final String viewGroup, final short viewPerms,
                                 final String newViewName, final String fromPath) throws Exception {
    updateClient(viewOwner);
    test(String.format("ALTER SESSION SET \"%s\"='%o';", ExecConstants.NEW_VIEW_DEFAULT_PERMS_KEY, viewPerms));
    test(String.format("CREATE VIEW %s.\"/user/%s/%s\" AS SELECT * FROM %s.\"%s\";",
      MINIDFS_STORAGE_PLUGIN_NAME, viewOwner, newViewName, MINIDFS_STORAGE_PLUGIN_NAME, fromPath));

    // Verify the view file created has the expected permissions and ownership
    Path viewFilePath = new Path(getUserHome(viewOwner), newViewName + DotFileType.VIEW.getEnding());
    FileStatus status = fs.getFileStatus(viewFilePath);
    assertEquals(viewGroup, status.getGroup());
    assertEquals(viewOwner, status.getOwner());
    assertEquals(viewPerms, status.getPermission().toShort());
  }

  protected static void createView(final String viewOwner, final String viewGroup, final String viewName,
                                 final String viewDef) throws Exception {
    updateClient(viewOwner);
    test(String.format("ALTER SESSION SET \"%s\"='%o';", ExecConstants.NEW_VIEW_DEFAULT_PERMS_KEY, (short) 0750));
    test("CREATE VIEW %s.%s.%s AS %s", MINIDFS_STORAGE_PLUGIN_NAME, "tmp", viewName, viewDef);
    final Path viewFilePath = new Path("/tmp/", viewName + DotFileType.VIEW.getEnding());
    fs.setOwner(viewFilePath, viewOwner, viewGroup);
  }
}
