/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.util;

import java.io.File;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.google.common.io.Files;

/**
 * This class contains utility methods to speed up tests. Some of the production code currently calls this method
 * when the production code is executed as part of the test runs. That's the reason why this code has to be in
 * production module.
 */
public class TestUtilities {

  public static final String DFS_TEST_PLUGIN_NAME = "dfs_test";

  /**
   * Create and removes a temporary folder
   *
   * @return absolute path to temporary folder
   */
  public static String createTempDir() {
    final File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    return tmpDir.getAbsolutePath();
  }

  /**
   * Update the location of dfs_test.tmp location. Get the "dfs_test.tmp" workspace and update the location with an
   * exclusive temp directory just for use in the current test jvm.
   *
   * @param pluginRegistry
   * @return JVM exclusive temporary directory location.
   */
  public static void updateDfsTestTmpSchemaLocation(final StoragePluginRegistry pluginRegistry,
                                                      final String tmpDirPath)
      throws ExecutionSetupException {
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin(DFS_TEST_PLUGIN_NAME);
    final FileSystemConfig pluginConfig = plugin.getConfig();

    final FileSystemConfig newPluginConfig = new FileSystemConfig(
        pluginConfig.getConnection(),
        tmpDirPath,
        pluginConfig.getConfig(),
        pluginConfig.getFormats(),
        pluginConfig.isImpersonationEnabled(),
        SchemaMutability.ALL);

    pluginRegistry.createOrUpdate(DFS_TEST_PLUGIN_NAME, newPluginConfig, true);
  }
}
