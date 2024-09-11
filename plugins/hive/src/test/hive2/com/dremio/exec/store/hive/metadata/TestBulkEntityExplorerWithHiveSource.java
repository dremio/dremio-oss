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
package com.dremio.exec.store.hive.metadata;

import com.dremio.dac.metadata.TestBulkEntityExplorer;
import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.impersonation.hive.BaseTestHiveImpersonation;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.hive.Hive2StoragePluginConfig;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.dremio.exec.store.hive.HiveTestDataGenerator;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.nio.file.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class TestBulkEntityExplorerWithHiveSource extends TestBulkEntityExplorer {

  @ClassRule public static TemporaryFolder storage = new TemporaryFolder();

  @BeforeClass
  public static void setupSources() throws Exception {
    File src1 = storage.newFolder("src1");
    File src2 = storage.newFolder("src2");

    HiveTestDataGenerator source1 =
        HiveTestDataGenerator.newInstanceWithoutTestData(
            createFolderPath(src1, "warehouse"), createFolderPath(src1, "metastore_db"), null);
    HiveTestDataGenerator source2 =
        HiveTestDataGenerator.newInstanceWithoutTestData(
            createFolderPath(src2, "warehouse"), createFolderPath(src2, "metastore_db"), null);

    createSource(SOURCE_1, source1);
    createSource(SOURCE_1_EXTERNAL, source1);
    createSource(SOURCE_2, source2);
    createSource(SOURCE_2_EXTERNAL, source2);
  }

  private static void createSource(String name, HiveTestDataGenerator source) throws Exception {
    SourceConfig sc = new SourceConfig();
    sc.setName(name);
    Hive2StoragePluginConfig conf =
        BaseTestHiveImpersonation.createHiveStoragePlugin(source.getConfig());
    conf.hostname = "localhost";
    conf.port = source.getPort();
    conf.defaultCtasFormat = DefaultCtasFormatSelection.ICEBERG;
    conf.propertyList.addAll(
        ImmutableList.of(
            new Property(
                HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:///" + source.getWhDir() + "/"),
            new Property(HiveConfFactory.ENABLE_DML_TESTS_WITHOUT_LOCKING, "true")));
    sc.setType(conf.getType());
    sc.setConfig(conf.toBytesString());
    sc.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
    getCatalogService().getSystemUserCatalog().createSource(sc);
  }

  private static String createFolderPath(File parent, String child) {
    return Path.of(parent.getPath(), child).toString();
  }
}
