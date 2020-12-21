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
package com.dremio.exec.impersonation;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static com.dremio.common.TestProfileHelper.isMaprProfile;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.junit.Assert.assertEquals;

import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;

public class TestImpersonationMetadataRefresh extends BaseTestImpersonation {
  private static final String USER = "dremioTestUser1";
  private static final String GROUP = "dremioTestGrp1";
  private static Configuration configuration;

  static {
    UserGroupInformation.createUserForTesting(USER, new String[]{GROUP});
  }

  @BeforeClass
  public static void setup() throws Exception {
    assumeNonMaprProfile();
    configuration = startMiniDfsCluster(TestImpersonationMetadataRefresh.class.getSimpleName());
    addMiniDfsStorage();
  }

  @AfterClass
  public static void removeMiniDfsBasedStorage() throws Exception {
    /*
     JUnit assume() call results in AssumptionViolatedException, which is handled by JUnit with a goal to ignore
     the test having the assume() call. Multiple assume() calls, or other exceptions coupled with a single assume()
     call, result in multiple exceptions, which aren't handled by JUnit, leading to test deemed to be failed.
     We thus use isMaprProfile() check instead of assumeNonMaprProfile() here.
     */
    if (isMaprProfile()) {
      return;
    }

    SourceConfig config = getSabotContext().getNamespaceService(SYSTEM_USERNAME).getSource(new NamespaceKey(MINIDFS_STORAGE_PLUGIN_NAME));
    ((CatalogServiceImpl) getSabotContext().getCatalogService()).getSystemUserCatalog().deleteSource(config);
    stopMiniDfsCluster();
  }

  @Test
  public void testAutoMetadataRefreshUponFileSystemChange() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_REATTEMPTS, true)) {

      final String query = "SELECT * from " + MINIDFS_STORAGE_PLUGIN_NAME + ".data";

      // run simple query
      final int recordCount = getRecordCount(testSqlWithResults(query));

      // change underlying filesystem source
      FileSystem.get(configuration).delete(new Path("/data/2"), true);

      // wait for for cached metadata to expire
      Thread.sleep(10);

      // run simple query again - records should be half this size as one parquet file has been removed
      assertEquals(recordCount / 2, getRecordCount(testSqlWithResults(query)));
    }
  }

  private static void addMiniDfsStorage() throws Exception {
    CatalogService catalogService = getSabotContext().getCatalogService();
    Path dirPath1 = new Path("/data/1");
    FileSystem.mkdirs(fs, dirPath1, new FsPermission((short) 0777));
    fs.setOwner(dirPath1, processUser, processUser);
    FileSystem.get(configuration).copyFromLocalFile(getSampleParquetFilePath(), dirPath1);

    Path dirPath2 = new Path("/data/2");
    FileSystem.mkdirs(fs, dirPath2, new FsPermission((short) 0777));
    fs.setOwner(dirPath2, processUser, processUser);
    FileSystem.get(configuration).copyFromLocalFile(getSampleParquetFilePath(), dirPath2);

    InternalFileConf conf = new InternalFileConf();
    conf.connection = fs.getUri().toString();
    conf.path = "/";
    conf.enableImpersonation = true;
    conf.mutability = SchemaMutability.ALL;

    SourceConfig config = new SourceConfig();
    config.setName(MINIDFS_STORAGE_PLUGIN_NAME);
    config.setConnectionConf(conf);
    config.setMetadataPolicy(new MetadataPolicy()
        .setAuthTtlMs(5L)
        .setAutoPromoteDatasets(true)
        .setDatasetDefinitionExpireAfterMs(1_000_000L));
    ((CatalogServiceImpl) catalogService).getSystemUserCatalog().createSource(config);
  }

  private static Path getSampleParquetFilePath() throws URISyntaxException {
    return new Path(TestImpersonationMetadataRefresh.class.getResource("/parquet/singlets.parquet").toURI());
  }
}
