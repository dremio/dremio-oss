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
package com.dremio.dac.daemon;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static org.junit.Assert.assertEquals;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.perf.Timer;
import com.dremio.common.util.FileUtils;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.proto.model.source.HdfsConfig;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.sources.HDFSSourceConfigurator;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.BaseTestMiniDFS;
import com.dremio.service.Binder;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.UserService;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * HDFS tests.
 */
public class TestHdfs extends BaseTestMiniDFS {

  private static DACDaemon dremioDaemon;
  private static Binder dremioBinder;
  private static Client client;
  private static String host;
  private static int port;
  private static final String SOURCE_NAME = "dachdfs_test";
  private static final String SOURCE_ID = "12345";
  private static final String SOURCE_DESC = "description";

  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    assumeNonMaprProfile();
    startMiniDfsCluster(TestHdfs.class.getName());
    String[] hostPort = dfsCluster.getNameNode().getHostAndPort().split(":");
    host = hostPort[0];
    port = Integer.parseInt(hostPort[1]);
    fs.mkdirs(new Path("/dir1/"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.mkdirs(new Path("/dir1/json"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.mkdirs(new Path("/dir1/text"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.mkdirs(new Path("/dir1/parquet"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.mkdirs(new Path("/dir2"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    fs.copyFromLocalFile(false, true, new Path(FileUtils.getResourceAsFile("/datasets/users.json").getAbsolutePath()),
      new Path("/dir1/json/users.json"));
    fs.setPermission(new Path("/dir1/json/users.json"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    try (Timer.TimedBlock b = Timer.time("TestHdfs.@BeforeClass")) {
      dremioDaemon = DACDaemon.newDremioDaemon(
        DACConfig
          .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
          .autoPort(true)
          .allowTestApis(true)
          .writePath(folder.getRoot().getAbsolutePath())
          .clusterMode(ClusterMode.LOCAL)
          .serveUI(true),
        DremioTest.CLASSPATH_SCAN_RESULT,
        new DACDaemonModule(),
        new HDFSSourceConfigurator());
      dremioDaemon.init();
      dremioBinder = BaseTestServer.createBinder(dremioDaemon.getBindingProvider());
      JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
      provider.setMapper(JSONUtil.prettyMapper());
      client = ClientBuilder.newBuilder().register(provider).register(MultiPartFeature.class).build();
    }
  }

  @AfterClass
  public static void close() throws Exception {
    try (Timer.TimedBlock b = Timer.time("TestHdfs.@AfterClass")) {
      if (dremioDaemon != null) {
        dremioDaemon.close();
      }
      if (client != null) {
        client.close();
      }
      stopMiniDfsCluster();
    }
  }

  protected static <T> T l(Class<T> clazz) {
    return dremioBinder.lookup(clazz);
  }

  @Before
  public void setup() throws Exception {
    {
      SampleDataPopulator.addDefaultFirstUser(l(UserService.class), new NamespaceServiceImpl(l(KVStoreProvider.class)));
      final HdfsConfig hdfsConfig = new HdfsConfig();
      hdfsConfig.setHostname(host);
      hdfsConfig.setPort(port);
      SourceUI source = new SourceUI();
      source.setName(SOURCE_NAME);
      source.setConfig(hdfsConfig);
      source.setId(SOURCE_ID);
      source.setDescription(SOURCE_DESC);
      source.setVersion(null);
      l(SourceService.class).registerSourceWithRuntime(source);
    }
  }

  @After
  public void cleanup() throws Exception {
    ((LocalKVStoreProvider) l(KVStoreProvider.class)).deleteEverything();
    l(SourceService.class).unregisterSourceWithRuntime(new SourceName(SOURCE_NAME));
  }

  @Test
  public void listSource() throws Exception {
    NamespaceTree ns = l(SourceService.class).listSource(new SourceName(SOURCE_NAME), null, SampleDataPopulator.DEFAULT_USER_NAME);
    assertEquals(2, ns.getFolders().size());
    assertEquals(0, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());
  }

  @Test
  public void listFolder() throws Exception {
    NamespaceTree ns = l(SourceService.class).listFolder(new SourceName(SOURCE_NAME), new SourceFolderPath("dachdfs_test.dir1.json"), SampleDataPopulator.DEFAULT_USER_NAME);
    assertEquals(0, ns.getFolders().size());
    assertEquals(1, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());
  }

  @Test
  public void testQueryOnFile() throws Exception {
    final JobsService jobService = l(JobsService.class);
    Job job = jobService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("SELECT * FROM dachdfs_test.dir1.json.\"users.json\"", SampleDataPopulator.DEFAULT_USER_NAME))
        .build(), NoOpJobStatusListener.INSTANCE);
    JobDataFragment jobData = job.getData().truncate(500);
    assertEquals(3, jobData.getReturnedRowCount());
    assertEquals(2, jobData.getSchema().getFieldCount());
  }

  // DX-7484: only a single instance of HDFS may exist
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Test
  public void testSingleInstanceCreate() throws Exception {
    // 'setup' would have already registered a Hdfs instance
    // Attempting to register a second one. Expect a failure
    final HdfsConfig hdfsConfig = new HdfsConfig();
    hdfsConfig.setHostname(host);
    hdfsConfig.setPort(port);
    SourceUI source = new SourceUI();
    source.setName("expect failure");
    source.setConfig(hdfsConfig);
    expectedException.expect(UserException.class);
    expectedException.expectMessage("Conflict with existing HDFS source dachdfs_test. Dremio only allows a single instance of this type");
    l(SourceService.class).registerSourceWithRuntime(source);
  }

  // DX-8184: editing of single-instance HDFS storage plugin
  @Test
  public void testSingleInstanceEdit() throws Exception {
    // 'setup' would have already registered an Hdfs instance 'SOURCE_NAME'
    // Attempting to edit that instance. Expect success
    final SourceConfig origConfig = l(NamespaceService.class).getSource(new SourcePath(new SourceName(SOURCE_NAME)).toNamespaceKey());
    assertEquals(origConfig.getDescription(), SOURCE_DESC);

    String newDesc = SOURCE_DESC + "_amended";

    final HdfsConfig hdfsConfig = new HdfsConfig();
    hdfsConfig.setHostname(host);
    hdfsConfig.setPort(port);
    SourceUI source = new SourceUI();
    source.setName(SOURCE_NAME);
    source.setConfig(hdfsConfig);
    source.setId(SOURCE_ID);
    source.setDescription(newDesc);
    source.setVersion(origConfig.getVersion());
    l(SourceService.class).registerSourceWithRuntime(source);

    final SourceConfig modifiedConfig = l(NamespaceService.class).getSource(new SourcePath(new SourceName(SOURCE_NAME)).toNamespaceKey());
    assertEquals(modifiedConfig.getDescription(), newDesc);

    // Back to original description
    source.setDescription(SOURCE_DESC);
    source.setVersion(modifiedConfig.getVersion());
    l(SourceService.class).registerSourceWithRuntime(source);

    final SourceConfig backToOrigConfig = l(NamespaceService.class).getSource(new SourcePath(new SourceName(SOURCE_NAME)).toNamespaceKey());
    assertEquals(backToOrigConfig.getDescription(), SOURCE_DESC);
  }
}
