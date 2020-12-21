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
package com.dremio.dac.daemon;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static com.dremio.common.TestProfileHelper.isMaprProfile;
import static com.dremio.dac.server.JobsServiceTestUtils.submitJobAndGetData;
import static org.junit.Assert.assertEquals;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.perf.Timer;
import com.dremio.common.util.FileUtils;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.BaseTestMiniDFS;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.HDFSConf;
import com.dremio.exec.util.TestUtilities;
import com.dremio.service.Binder;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.UserService;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * HDFS tests.
 */
public class TestHdfsAsyncParquet extends BaseTestMiniDFS {

  private static DACDaemon dremioDaemon;
  private static Binder dremioBinder;
  private static Client client;
  private static String host;
  private static int port;
  private static final String SOURCE_NAME = "dachdfs_parquettests";
  private static final String SOURCE_ID = "12346";
  private static final String SOURCE_DESC = "TestHdfsAsyncParquet";
  private BufferAllocator allocator;

  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  private static void setupSchemaLearnTest() throws Exception{
    fs.mkdirs(new Path("/parquet/"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.copyFromLocalFile(false, true, new Path(FileUtils.getResourceAsFile("/schemalearn").getAbsolutePath()),
      new Path("/parquet/"));
    fs.setPermission(new Path("/parquet/schemalearn"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
  }

  private static void setupIntUnionTest() throws Exception {
    fs.mkdirs(new Path("/parquet/"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.copyFromLocalFile(false, true, new Path(FileUtils.getResourceAsFile("/intunionschemalearn").getAbsolutePath()),
      new Path("/parquet/"));
    fs.setPermission(new Path("/parquet/intunionschemalearn"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
  }

  private static void setupStructSchemaChangeTest() throws Exception {
    fs.mkdirs(new Path("/parquet/"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.copyFromLocalFile(false, true, new Path(FileUtils.getResourceAsFile("/schemachangestruct").getAbsolutePath()),
      new Path("/parquet/"));
    fs.setPermission(new Path("/parquet/schemachangestruct"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
  }

  private static void setupStructWithDifferentCaseTest() throws Exception {
    fs.mkdirs(new Path("/parquet/"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.copyFromLocalFile(false, true, new Path(FileUtils.getResourceAsFile("/parquet_struct_with_different_case").getAbsolutePath()),
      new Path("/parquet/"));
    fs.setPermission(new Path("/parquet/parquet_struct_with_different_case"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
  }

  @BeforeClass
  public static void init() throws Exception {
    assumeNonMaprProfile();
    startMiniDfsCluster(TestHdfsAsyncParquet.class.getName());
    String[] hostPort = dfsCluster.getNameNode().getHostAndPort().split(":");
    host = hostPort[0];
    port = Integer.parseInt(hostPort[1]);
    setupSchemaLearnTest();
    setupIntUnionTest();
    setupStructSchemaChangeTest();
    setupStructWithDifferentCaseTest();
    try (Timer.TimedBlock b = Timer.time("TestHdfsAsyncParquet.@BeforeClass")) {
      dremioDaemon = DACDaemon.newDremioDaemon(
        DACConfig
          .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
          .autoPort(true)
          .allowTestApis(true)
          .writePath(folder.getRoot().getAbsolutePath())
          .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
          .clusterMode(ClusterMode.LOCAL)
          .serveUI(true),
        DremioTest.CLASSPATH_SCAN_RESULT,
        new DACDaemonModule());
      dremioDaemon.init();
      dremioBinder = BaseTestServer.createBinder(dremioDaemon.getBindingProvider());
      JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
      provider.setMapper(JSONUtil.prettyMapper());
      client = ClientBuilder.newBuilder().register(provider).register(MultiPartFeature.class).build();
    }
  }

  @AfterClass
  public static void close() throws Exception {
    /*
     JUnit assume() call results in AssumptionViolatedException, which is handled by JUnit with a goal to ignore
     the test having the assume() call. Multiple assume() calls, or other exceptions coupled with a single assume()
     call, result in multiple exceptions, which aren't handled by JUnit, leading to test deemed to be failed.
     We thus use isMaprProfile() check instead of assumeNonMaprProfile() here.
     */
    if (isMaprProfile()) {
      return;
    }

    try (Timer.TimedBlock b = Timer.time("TestHdfsAsyncParquet.@AfterClass")) {
      if (dremioDaemon != null) {
        dremioDaemon.close();
      }
      if (client != null) {
        client.close();
      }
      stopMiniDfsCluster();
    }
  }

  private static <T> T l(Class<T> clazz) {
    return dremioBinder.lookup(clazz);
  }

  @Before
  public void setup() throws Exception {
    {
      SampleDataPopulator.addDefaultFirstUser(l(UserService.class), new NamespaceServiceImpl(l(LegacyKVStoreProvider.class)));
      final HDFSConf hdfsConfig = new HDFSConf();
      hdfsConfig.hostname = host;
      hdfsConfig.port = port;
      SourceConfig source = new SourceConfig();
      source.setName(SOURCE_NAME);
      source.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE);
      source.setConnectionConf(hdfsConfig);
      source.setId(new EntityId(SOURCE_ID));
      source.setDescription(SOURCE_DESC);
      allocator = l(BootStrapContext.class).getAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
      ((CatalogServiceImpl)l(CatalogService.class)).getSystemUserCatalog().createSource(source);
    }
  }

  @After
  public void cleanup() throws Exception {
    TestUtilities.clear(l(CatalogService.class), l(LegacyKVStoreProvider.class), null, null);
    allocator.close();
  }

  @Test
  public void listSource() throws Exception {
    NamespaceTree ns = l(SourceService.class).listSource(new SourceName(SOURCE_NAME), null, SampleDataPopulator.DEFAULT_USER_NAME);
    assertEquals(1, ns.getFolders().size());
    assertEquals(0, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());
  }

  @Test
  public void listFolder() throws Exception {
    NamespaceTree ns = l(SourceService.class).listFolder(new SourceName(SOURCE_NAME), new SourceFolderPath(SOURCE_NAME + ".parquet.schemalearn"), SampleDataPopulator.DEFAULT_USER_NAME);
    assertEquals(0, ns.getFolders().size());
    assertEquals(2, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());
  }

  @Test
  public void testQueryOnFile() throws Exception {
    try (final JobDataFragment jobData = submitJobAndGetData(l(JobsService.class),
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("SELECT * FROM "+SOURCE_NAME+".parquet.schemalearn", SampleDataPopulator.DEFAULT_USER_NAME))
        .build(), 0, 500, allocator)) {
      assertEquals(2, jobData.getReturnedRowCount());
      assertEquals(3, jobData.getColumns().size());
    }
  }

  @Test
  public void testIntUnion() throws Exception {
    // DX-21572 - test case
    // Test has two files
    // File 1: two int64 columns
    // File 2: two int32 columns and one column name is common with File 1
    // When select * succeeds it should contain total of three columns
    // (1 common column, remaining column from File 1, remaining column from File 2)
    try (final JobDataFragment jobData = submitJobAndGetData(l(JobsService.class),
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("SELECT * FROM "+SOURCE_NAME+".parquet.intunionschemalearn", SampleDataPopulator.DEFAULT_USER_NAME))
        .build(), 0, 500, allocator)) {
      assertEquals(100, jobData.getReturnedRowCount());
      assertEquals(3, jobData.getColumns().size());
    }
  }

  @Test
  public void testSchemaChangeStruct() throws Exception {
    // DX-21572 - test case
    // Test has two files
    // File 1: one int column and one struct column with f1, f2 int fields
    // File 2: same int column and same struct column but with fields f1, f3 int fields
    // When select * succeeds it should contain total of two columns and two rows
    try (final JobDataFragment jobData = submitJobAndGetData(l(JobsService.class),
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("SELECT * FROM "+SOURCE_NAME+".parquet.schemachangestruct", SampleDataPopulator.DEFAULT_USER_NAME))
        .build(), 0, 500, allocator)) {
      assertEquals(2, jobData.getReturnedRowCount());
      assertEquals(2, jobData.getColumns().size());
    }
  }

  @Test
  public void testStructWithDifferentCase() throws Exception {
    // DX-21926 - test case
    // Test has two files
    // File 1: parquet file with 'EXPR$1' as one of its struct type column names
    // File 2: parquet file with 'expr$1' as one of its struct type column names
    // When select EXPR$1 succeeds, it should contain two rows
    // by doing case insensitive column name mapping
    try (final JobDataFragment jobData = submitJobAndGetData(l(JobsService.class),
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(
          "SELECT \"parquet_struct_with_different_case\".\"EXPR$1\".age FROM " +
            SOURCE_NAME+".parquet.parquet_struct_with_different_case", SampleDataPopulator.DEFAULT_USER_NAME))
        .build(), 0, 500, allocator)) {
      assertEquals(2, jobData.getReturnedRowCount());
      assertEquals(1, jobData.getColumns().size());
    }
  }
}
