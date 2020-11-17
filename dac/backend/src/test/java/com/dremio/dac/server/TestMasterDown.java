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
package com.dremio.dac.server;

import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.inject.Provider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.AutoCloseables;
import com.dremio.common.perf.Timer;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.daemon.ServerHealthMonitor;
import com.dremio.dac.daemon.ZkServer;
import com.dremio.dac.explore.model.DataPOJO;
import com.dremio.dac.explore.model.ViewFieldTypeMixin;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobsUI;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.model.system.ServerStatus;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.server.NodeRegistration;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.service.BindingProvider;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.zk.ZKClusterCoordinator;
import com.dremio.service.jobs.HybridJobsService;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.ReflectionAdministrationService;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.UserService;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * Test that when master goes down services on other nodes wait in loop for master to come back up.
 * Test should not be run in parallel with other tests since it makes assumption about port master will start on.
 * Its enabled only when system property dremio_multinode is set.
 */
public class TestMasterDown extends BaseClientUtils {

  private static final String API_LOCATION = "apiv2";
  private static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;

  private static final String DEFAULT_USERNAME = SampleDataPopulator.DEFAULT_USER_NAME;
  private static final String DEFAULT_PASSWORD = SampleDataPopulator.PASSWORD;

  private static Client client;
  private static Client masterClient;
  private static WebTarget masterApiV2;
  private static WebTarget currentApiV2;
  private static DACDaemon currentDremioDaemon;
  private static DACDaemon masterDremioDaemon;

  @ClassRule
  public static final TemporaryFolder folder1 = new TemporaryFolder();

  @ClassRule
  public static final TemporaryFolder folder2 = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    Assume.assumeTrue(BaseTestServer.isMultinode());
    try (Timer.TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      masterDremioDaemon = DACDaemon.newDremioDaemon(
        DACConfig
          .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
          .autoPort(true)
          .addDefaultUser(true)
          .allowTestApis(true)
          .serveUI(false)
          .jobServerEnabled(false)
          .inMemoryStorage(true)
          .writePath(folder1.getRoot().getAbsolutePath())
          .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
          .clusterMode(DACDaemon.ClusterMode.DISTRIBUTED),
        DremioTest.CLASSPATH_SCAN_RESULT);

      // remote node
      currentDremioDaemon = DACDaemon.newDremioDaemon(
        DACConfig
          .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
          .isMaster(false)
          .autoPort(true)
          .allowTestApis(true)
          .serveUI(false)
          .inMemoryStorage(true)
          .writePath(folder2.getRoot().getAbsolutePath())
          .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
          .clusterMode(DACDaemon.ClusterMode.DISTRIBUTED)
          .isRemote(true),
        DremioTest.CLASSPATH_SCAN_RESULT);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(
      new AutoCloseable(){
        @Override
        public void close() throws Exception {
          if (client != null) {
            client.close();
          }
        }
      },
      new AutoCloseable(){
        @Override
        public void close() throws Exception {
          if (masterClient != null) {
            masterClient.close();
          }
        }
      },
      currentDremioDaemon, masterDremioDaemon);
  }

  private static void initClient() {
    JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    ObjectMapper objectMapper = JSONUtil.prettyMapper();
    JSONUtil.registerStorageTypes(objectMapper, DremioTest.CLASSPATH_SCAN_RESULT,
        ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG));
    objectMapper.registerModule(
      new SimpleModule()
        .addDeserializer(JobDataFragment.class,
          new JsonDeserializer<JobDataFragment>() {
            @Override
            public JobDataFragment deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
              return jsonParser.readValueAs(DataPOJO.class);
            }
          }
        )
    ).addMixIn(ViewFieldType.class, ViewFieldTypeMixin.class);
    provider.setMapper(objectMapper);
    client = ClientBuilder.newBuilder().register(provider).register(MultiPartFeature.class).build();
    WebTarget rootTarget = client.target("http://localhost:" + currentDremioDaemon.getWebServer().getPort());
    currentApiV2 = rootTarget.path(API_LOCATION);
  }

  private static void initMasterClient() {
    JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    ObjectMapper objectMapper = JSONUtil.prettyMapper();
    JSONUtil.registerStorageTypes(objectMapper, DremioTest.CLASSPATH_SCAN_RESULT,
        ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG));
    objectMapper.registerModule(
      new SimpleModule()
        .addDeserializer(JobDataFragment.class,
          new JsonDeserializer<JobDataFragment>() {
            @Override
            public JobDataFragment deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
              return jsonParser.readValueAs(DataPOJO.class);
            }
          }
        )
    );
    provider.setMapper(objectMapper);
    masterClient = ClientBuilder.newBuilder().register(provider).register(MultiPartFeature.class).build();
    WebTarget rootTarget = masterClient.target("http://localhost:" + masterDremioDaemon.getWebServer().getPort());
    masterApiV2 = rootTarget.path(API_LOCATION);
  }

  private void checkNodeStatus(long giveUpAfterMs, WebTarget webTarget, Response.Status expectedStatus, ServerStatus expectedServerStatus) throws Exception {
    final long sleepBetweenRetries = 10; // ms
    long sleptSoFar = 0;
    ServerStatus serverStatus = ServerStatus.OK;
    int responseStatusCode = -1;
    while (sleptSoFar < giveUpAfterMs) {
      Response response = webTarget.path("/server_status").request(JSON).buildGet().invoke();
      response.bufferEntity();
      responseStatusCode = response.getStatusInfo().getStatusCode();
      if (responseStatusCode == expectedStatus.getStatusCode()) {
        serverStatus = response.readEntity(ServerStatus.class);
        if (serverStatus.equals(expectedServerStatus)) {
          return;
        }
      }
      Thread.sleep(sleepBetweenRetries);
      sleptSoFar += sleepBetweenRetries;
    }
    assertEquals(responseStatusCode, expectedStatus.getStatusCode());
    assertEquals(serverStatus, expectedServerStatus);
  }

  private void checkMasterOk(long giveUpAfterMs) throws Exception {
    checkNodeStatus(giveUpAfterMs, masterApiV2, Response.Status.OK, ServerStatus.OK);
  }

  private void checkNodeOk(long giveUpAfterMs) throws Exception {
    checkNodeStatus(giveUpAfterMs, currentApiV2, Response.Status.OK, ServerStatus.OK);
  }

  /**
   * Check if the master node is down. Give up after 'giveUpAfterMs' milliseconds
   */
  private void checkNodeMasterDown(long giveUpAfterMs) throws Exception {
    checkNodeStatus(giveUpAfterMs, currentApiV2, Response.Status.SERVICE_UNAVAILABLE, ServerStatus.MASTER_DOWN);
  }

  private void sanityCheck() throws Exception {
    UserLoginSession ul = login();
    String authHeader =  HttpHeader.AUTHORIZATION.toString();
    String authToken = "_dremio" + ul.getToken();
    Space dg = expectSuccess(currentApiV2.path("space/DG").request(JSON).header(authHeader, authToken).buildGet(), Space.class);
    assertEquals(10, dg.getContents().getDatasets().size());
    expectSuccess(currentApiV2.path("/jobs").request(JSON).header(authHeader, authToken).buildGet(), JobsUI.class);

    SourceUI source = expectSuccess(currentApiV2.path("source/LocalFS1").request(JSON).header(authHeader, authToken).buildGet(), SourceUI.class);
    NamespaceTree ns = source.getContents();
    assertEquals("/source/LocalFS1", source.getResourcePath().toString());
    assertTrue(ns.getDatasets().size() + ns.getFolders().size() + ns.getFiles().size() > 0);

    String folderName = "folder_" + System.currentTimeMillis();
    expectSuccess((currentApiV2.path("space/DG/folder/").request(JSON).header(authHeader, authToken)).buildPost(Entity.json("{\"name\": \""+folderName+"\"}")), Folder.class);
  }

  @Test
  public void testMasterDown() throws Exception {
    final long timeoutMs = 5_000; // Timeout when checking if a node reached a given status
    Provider<Integer> jobsPortProvider = () -> currentDremioDaemon.getBindingProvider().lookup(ConduitServer.class).getPort();

    masterDremioDaemon.startPreServices();

    ((ZKClusterCoordinator)currentDremioDaemon.getBindingProvider().lookup(ClusterCoordinator.class))
      .setPortProvider(() -> masterDremioDaemon.getBindingProvider().lookup(ZkServer.class).getPort());

    currentDremioDaemon.startPreServices();

    // start non master node which should wait till master registers to cluster.
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          currentDremioDaemon.startServices(); // waiting
          currentDremioDaemon.getBindingProvider().lookup(HybridJobsService.class).setPortProvider(jobsPortProvider);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    t1.start();

    BindingProvider mp = currentDremioDaemon.getBindingProvider();
    assertEquals(ServerStatus.MASTER_DOWN, mp.lookup(ServerHealthMonitor.class).getStatus());

    masterDremioDaemon.startServices();
    masterDremioDaemon.getBindingProvider().lookup(HybridJobsService.class).setPortProvider(jobsPortProvider);
    t1.join();
    initClient();
    initMasterClient();
    checkMasterOk(timeoutMs);
    checkNodeOk(timeoutMs);
    NamespaceService ns = mp.lookup(NamespaceService.Factory.class).get(DEFAULT_USERNAME);
    final SabotContext sabotContext = mp.lookup(SabotContext.class);

    final DatasetVersionMutator datasetVersionMutator = new DatasetVersionMutator(
        mp.lookup(InitializerRegistry.class),
        mp.lookup(LegacyKVStoreProvider.class),
        ns,
        mp.lookup(JobsService.class),
        mp.lookup(CatalogService.class),
        sabotContext.getOptionManager());

    TestUtilities.addClasspathSourceIf(sabotContext.getCatalogService());
    DACSecurityContext dacSecurityContext = new DACSecurityContext(new UserName(SystemUser.SYSTEM_USERNAME), SystemUser.SYSTEM_USER, null);

    currentDremioDaemon.getBindingCreator().bindProvider(ReflectionAdministrationService.class, () -> {
      ReflectionAdministrationService.Factory factory = mp.lookup(ReflectionAdministrationService.Factory.class);
      return factory.get(new ReflectionContext(DEFAULT_USER_NAME, true));
    });

    SampleDataPopulator populator = new SampleDataPopulator(
      sabotContext,
      new SourceService(
        ns,
        datasetVersionMutator,
        sabotContext.getCatalogService(),
        mp.lookup(ReflectionServiceHelper.class),
        new CollaborationHelper(mp.lookup(LegacyKVStoreProvider.class), sabotContext, mp.lookup(NamespaceService.class), dacSecurityContext, mp.lookup(SearchService.class)),
        ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG),
        dacSecurityContext
      ),
      datasetVersionMutator,
      mp.lookup(UserService.class),
      ns,
      DEFAULT_USERNAME
    );
    populator.populateInitialData();

    sanityCheck();
    checkMasterOk(timeoutMs);
    checkNodeOk(timeoutMs);

    // stop master, fake it by un-registering master node from zk
    masterDremioDaemon.getBindingProvider().lookup(NodeRegistration.class).close();
    checkNodeMasterDown(timeoutMs);
    // start master
    masterDremioDaemon.getBindingProvider().lookup(NodeRegistration.class).start();

    checkNodeOk(timeoutMs);
    checkMasterOk(timeoutMs);

    sanityCheck();
  }

  public UserLoginSession login() {
    UserLogin userLogin = new UserLogin(DEFAULT_USERNAME, DEFAULT_PASSWORD);
    return expectSuccess(currentApiV2.path("/login").request(JSON).buildPost(Entity.json(userLogin)), UserLoginSession.class);
  }
}
