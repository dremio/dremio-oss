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
package com.dremio.exec.store.dfs;

import java.io.File;
import java.io.PrintWriter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.StoragePluginRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

/**
 * Test various operations (accessing table (file/folder), create table, create view, drop table, drop view) in
 * a {@link FileSystemPlugin} based storage where the root of the source is a subpath in FileSystem, not the root.
 */
public class TestSubPathFileSystemPlugin extends BaseTestQuery {
  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  private static File storageBase;

  @BeforeClass
  public static void setup() throws Exception {
    generateTestData();
    addSubPathDfsPlugin();
  }

  private static void generateTestData() throws Exception {
    storageBase = testFolder.newFolder("base");

    // put some test data under the base
    generateTestDataFile(new File(storageBase, "tblInside.csv"));
    Files.createParentDirs(new File(storageBase, "dirInside/tbl.csv"));
    generateTestDataFile(new File(storageBase, "dirInside/tbl.csv"));

    // generate data outside the storage base
    generateTestDataFile(new File(testFolder.getRoot(), "tblOutside.csv"));
  }

  private static String generateTestDataFile(File file) throws Exception {
    PrintWriter printWriter = new PrintWriter(file);
    for (int i = 1; i <= 5; i++) {
      printWriter.println (String.format("%d,key_%d", i, i));
    }
    printWriter.close();

    return file.getPath();
  }

  private static void addSubPathDfsPlugin() throws Exception {
    final StoragePluginRegistry pluginRegistry = getSabotContext().getStorage();
    final FileSystemConfig lfsPluginConfig = pluginRegistry.getPlugin("dfs_test").getId().getConfig();

    final FileSystemConfig subPathPluginConfig =
        new FileSystemConfig("file:///", storageBase.getPath(), ImmutableMap.<String, String>of(),
            lfsPluginConfig.getFormats(), false, SchemaMutability.ALL);

    pluginRegistry.createOrUpdate("subPathDfs", subPathPluginConfig, true);
  }

  @Test
  public void queryValidPath() throws Exception {
    test("SELECT * FROM subPathDfs.`tblInside.csv`");
    test("SELECT * FROM subPathDfs.`dirInside/tbl.csv`");
  }

  @Test
  public void queryInvalidPath() throws Exception {
    errorMsgTestHelper("SELECT * FROM subPathDfs.`../tblOutside.csv`",
        "PERMISSION ERROR: Not allowed to access files outside of the source root");
  }

  @Test
  public void ctasAndDropTableValidPath() throws Exception {
    test("CREATE TABLE subPathDfs.ctas AS SELECT * FROM cp.`region.json`");
    test("DROP TABLE subPathDfs.ctas");
  }

  @Test
  public void ctasInvalidPath() throws Exception {
    errorMsgTestHelper("CREATE TABLE subPathDfs.`../ctas` AS SELECT * FROM cp.`region.json`",
        "PERMISSION ERROR: Not allowed to access files outside of the source root");
  }

  @Test
  public void dropTableInvalidPath() throws Exception {
    errorMsgTestHelper("DROP TABLE subPathDfs.`../tblOutside.csv`",
        "PERMISSION ERROR: Not allowed to access files outside of the source root");
  }

  @Test
  public void createAndDropViewValidPath() throws Exception {
    test("CREATE VIEW subPathDfs.`view` AS SELECT * FROM cp.`region.json`");
    test("DROP VIEW subPathDfs.`view`");
  }

  @Test
  public void createViewInvalidPath() throws Exception {
    errorMsgTestHelper("CREATE VIEW subPathDfs.`../view` AS SELECT * FROM cp.`region.json`",
        "PERMISSION ERROR: Not allowed to access files outside of the source root");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    getSabotContext().getStorage().deletePlugin("subPathDfs");
  }
}
