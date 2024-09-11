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
package com.dremio.dac.metadata;

import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.source.proto.SourceConfig;
import java.io.File;
import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class TestBulkEntityExplorerWithFileSystemSource extends TestBulkEntityExplorer {

  @ClassRule public static TemporaryFolder storage = new TemporaryFolder();

  @BeforeClass
  public static void setupSources() throws Exception {
    String src1 = createFolderPath("src1");
    String src2 = createFolderPath("src2");

    createSource(SOURCE_1, src1);
    createSource(SOURCE_1_EXTERNAL, src1);
    createSource(SOURCE_2, src2);
    createSource(SOURCE_2_EXTERNAL, src2);
  }

  private static void createSource(String name, String path) {
    SourceConfig c = new SourceConfig();
    NASConf conf = new NASConf();
    conf.path = path;
    conf.defaultCtasFormat = DefaultCtasFormatSelection.ICEBERG;
    c.setConnectionConf(conf);
    c.setName(name);
    c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
    getCatalogService().getSystemUserCatalog().createSource(c);
  }

  private static String createFolderPath(String path) throws Exception {
    File folder = storage.newFolder(path);
    return Path.of(folder.getPath()).toString();
  }
}
