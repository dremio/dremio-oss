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
package com.dremio.exec.store.dfs;

import com.dremio.BaseTestQuery;
import com.dremio.config.DremioConfig;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.io.Files;
import java.io.File;
import java.io.PrintWriter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test various operations (accessing table (file/folder), create table, create view, drop table,
 * drop view) in a {@link FileSystemPlugin} based storage where the root of the source is a subpath
 * in FileSystem, not the root.
 */
public class TestSubPathFileSystemPlugin extends BaseTestQuery {
  @ClassRule public static TemporaryFolder testFolder = new TemporaryFolder();

  protected static File storageBase;

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @BeforeClass
  public static void setup() throws Exception {
    generateTestData();
    addSubPathDfsPlugin();
  }

  protected static void generateTestData() throws Exception {
    storageBase = testFolder.newFolder("base");

    // put some test data under the base
    generateTestDataFile(new File(storageBase, "tblInside.csv"), "%d,key_%d");
    Files.createParentDirs(new File(storageBase, "dirInside/tbl.csv"));
    generateTestDataFile(new File(storageBase, "dirInside/tbl.csv"), "%d,key_%d");

    // generate data outside the storage base
    generateTestDataFile(new File(testFolder.getRoot(), "tblOutside.csv"), "%d,key_%d");

    // generate data for a dataset that has too many files
    Files.createParentDirs(new File(storageBase, "largeDir/tbl1.csv"));
    generateTestDataFile(new File(storageBase, "largeDir/tbl1.csv"), "%d,key_%d");
    generateTestDataFile(new File(storageBase, "largeDir/tbl2.csv"), "%d,key_%d");

    // generate data for a mutable dataset
    Files.createParentDirs(new File(storageBase, "largeDir2/tbl1.csv"));
    generateTestDataFile(new File(storageBase, "largeDir2/tbl1.csv"), "%d,key_%d");

    // generate data for a json dataset that has too many files
    Files.createParentDirs(new File(storageBase, "largeJsonDir/json_file1.json"));
    generateTestDataFile(
        new File(storageBase, "largeJsonDir/json_file1.json"),
        "{\"rownum\":%d, \"value\": \"val_%d\"}");
    generateTestDataFile(
        new File(storageBase, "largeJsonDir/json_file2.json"),
        "{\"rownum\":%d, \"value\": \"val_%d\"}");
    generateTestDataFile(
        new File(storageBase, "largeJsonDir/json_file3.json"),
        "{\"rownum\":%d, \"value\": \"val_%d\"}");
    generateTestDataFile(
        new File(storageBase, "largeJsonDir/json_file4.json"),
        "{\"rownum\":%d, \"value\": \"val_%d\"}");
  }

  protected static String generateTestDataFile(File file, String format_string) throws Exception {
    PrintWriter printWriter = new PrintWriter(file);
    for (int i = 1; i <= 5; i++) {
      printWriter.println(String.format(format_string, i, i));
    }
    printWriter.close();

    return file.getPath();
  }

  private static void addSubPathDfsPlugin() throws Exception {
    final CatalogServiceImpl pluginRegistry =
        (CatalogServiceImpl) getSabotContext().getCatalogService();
    final ManagedStoragePlugin msp = pluginRegistry.getManagedSource("dfs_test");
    StoragePluginId pluginId = msp.getId();
    InternalFileConf nasConf = pluginId.getConnectionConf();
    nasConf.path = storageBase.getPath();
    nasConf.mutability = SchemaMutability.ALL;

    // Add one configuration for testing when internal is true
    nasConf.isInternal = true;

    SourceConfig config = pluginId.getConfig();
    config.setId(null);
    config.setTag(null);
    config.setConfigOrdinal(null);
    config.setName("subPathDfs");
    config.setConfig(nasConf.toBytesString());
    pluginRegistry.getSystemUserCatalog().createSource(config);
  }

  @Test
  public void queryValidPath() throws Exception {
    test("SELECT * FROM subPathDfs.\"tblInside.csv\"");
    test("SELECT * FROM subPathDfs.\"dirInside/tbl.csv\"");
  }

  @Test
  public void queryInvalidPath() {
    errorMsgTestHelper(
        "SELECT * FROM subPathDfs.\"../tblOutside.csv\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @Test
  public void queryInvalidPath2() {
    errorMsgTestHelper(
        "SELECT * FROM subPathDfs.subFolder.\"../tblOutside.csv\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @Test
  public void ctasAndDropTableValidPath() throws Exception {
    test("CREATE TABLE subPathDfs.ctas AS SELECT * FROM cp.\"region.json\"");
    test("DROP TABLE subPathDfs.ctas");
  }

  @Test
  public void ctasInvalidPath() {
    errorMsgTestHelper(
        "CREATE TABLE subPathDfs.\"../ctas\" AS SELECT * FROM cp.\"region.json\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @Test
  public void ctasInvalidPath2() {
    errorMsgTestHelper(
        "CREATE TABLE subPathDfs.subFolder.\"../ctas\" AS SELECT * FROM cp.\"region.json\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @Test
  public void dropTableInvalidPath() {
    errorMsgTestHelper(
        "DROP TABLE subPathDfs.\"../tblOutside.csv\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @Test
  public void dropTableInvalidPath2() {
    errorMsgTestHelper(
        "DROP TABLE subPathDfs.subFolder.\"../tblOutside.csv\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @Test
  public void createAndDropViewValidPath() throws Exception {
    test("CREATE VIEW subPathDfs.\"view\" AS SELECT * FROM cp.\"region.json\"");
    test("DROP VIEW subPathDfs.\"view\"");
  }

  @Test
  public void createViewInvalidPath() {
    errorMsgTestHelper(
        "CREATE VIEW subPathDfs.\"../view\" AS SELECT * FROM cp.\"region.json\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @Test
  public void createViewInvalidPath2() {
    errorMsgTestHelper(
        "CREATE VIEW subPathDfs.subFolder.\"../view\" AS SELECT * FROM cp.\"region.json\"",
        "PERMISSION ERROR: Not allowed to perform directory traversal");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    SourceConfig config =
        getSabotContext()
            .getNamespaceService(SystemUser.SYSTEM_USERNAME)
            .getSource(new NamespaceKey("subPathDfs"));
    ((CatalogServiceImpl) getSabotContext().getCatalogService())
        .getSystemUserCatalog()
        .deleteSource(config);
  }
}
