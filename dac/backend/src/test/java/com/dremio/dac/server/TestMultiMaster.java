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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import javax.inject.Provider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.AutoCloseables;
import com.dremio.common.perf.Timer;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.daemon.DACDaemonModule;
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
import com.dremio.dac.service.exec.MasterElectionService;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.service.BindingProvider;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.zk.KillZkSession;
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
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Test that when two masters are configured, only one is active and the second one is
 * waiting on the first one to terminate (or disconnect from ZooKeeper).
 */
public class TestMultiMaster extends BaseClientUtils {

  private static final String API_LOCATION = "apiv2";
  private static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;

  private static final String DEFAULT_USERNAME = SampleDataPopulator.DEFAULT_USER_NAME;
  private static final String DEFAULT_PASSWORD = SampleDataPopulator.PASSWORD;

  private ZkServer zkServer;
  private Client client;
  private Client hotMasterClient;
  private Client coldMasterClient;
  private WebTarget hotMasterApiV2;
  private WebTarget coldMasterApiV2;
  private WebTarget currentApiV2;
  private DACDaemon currentDremioDaemon;
  private DACDaemon masterDremioDaemon1;
  private DACDaemon masterDremioDaemon2;

  private Provider<Integer> jobsPortProvider = () -> currentDremioDaemon.getBindingProvider().lookup(ConduitServer.class).getPort();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws Exception {
    Assume.assumeTrue(BaseTestServer.isMultinode());
    try (Timer.TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      zkServer = new ZkServer(temporaryFolder.newFolder("zk").getAbsolutePath(), 2181, true);
      zkServer.start();

      final File masterPath = temporaryFolder.newFolder("master");
      masterDremioDaemon1 = DACDaemon.newDremioDaemon(
          DACConfig
            .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
            .autoPort(true)
            .addDefaultUser(true)
            .allowTestApis(true)
            .serveUI(false)
            .jobServerEnabled(false)
            .inMemoryStorage(false)
            .writePath(masterPath.getAbsolutePath())
            .clusterMode(DACDaemon.ClusterMode.DISTRIBUTED)
            .zk("localhost:" + zkServer.getPort())
            .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
            .with(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL, false)
            .with(DremioConfig.DEBUG_DISABLE_MASTER_ELECTION_SERVICE_BOOL, false),
          DremioTest.CLASSPATH_SCAN_RESULT,
          new DACDaemonModule() {
            @Override
            public void bootstrap(final Runnable shutdownHook, SingletonRegistry bootstrapRegistry, ScanResult scanResult, DACConfig dacConfig, boolean isMaster) {
              super.bootstrap(shutdownHook, bootstrapRegistry, scanResult, dacConfig, isMaster);

              bootstrapRegistry.replace(MasterElectionService.class, new MasterElectionService(bootstrapRegistry.provider(ClusterCoordinator.class) ) {
                @Override
                protected void abort() {
                  shutdownHook.run();
                }
              });
            };
          }
        );

        masterDremioDaemon2 = DACDaemon.newDremioDaemon(
          DACConfig
            .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
            .autoPort(true)
            .allowTestApis(true)
            .serveUI(false)
            .jobServerEnabled(false)
            .inMemoryStorage(false)
            .writePath(masterPath.getAbsolutePath())
            .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
            .clusterMode(DACDaemon.ClusterMode.DISTRIBUTED)
            .zk("localhost:" + zkServer.getPort())
            .with(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL, false)
            .with(DremioConfig.DEBUG_DISABLE_MASTER_ELECTION_SERVICE_BOOL, false),
          DremioTest.CLASSPATH_SCAN_RESULT,
          new DACDaemonModule() {
            @Override
            public void bootstrap(final Runnable shutdownHook, SingletonRegistry bootstrapRegistry, ScanResult scanResult, DACConfig dacConfig, boolean isMaster) {
              super.bootstrap(shutdownHook, bootstrapRegistry, scanResult, dacConfig, isMaster);

              bootstrapRegistry.replace(MasterElectionService.class, new MasterElectionService(bootstrapRegistry.provider(ClusterCoordinator.class) ) {
                @Override
                protected void abort() {
                  shutdownHook.run();
                }
              });
            };
          }
          );

      // remote node
      currentDremioDaemon = DACDaemon.newDremioDaemon(
        DACConfig
          .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
          .isMaster(false)
          .autoPort(true)
          .allowTestApis(true)
          .serveUI(false)
          .inMemoryStorage(true)
          .writePath(temporaryFolder.newFolder("remote").getAbsolutePath())
          .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
          .clusterMode(DACDaemon.ClusterMode.DISTRIBUTED)
          .zk("localhost:" + zkServer.getPort()),
        DremioTest.CLASSPATH_SCAN_RESULT);

    }
  }

  private static final AutoCloseable toAutoCloseable(final Client client) {
    return new AutoCloseable(){
      @Override
      public void close() throws Exception {
        if (client != null) {
          client.close();
        }
      }
    };
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(
      toAutoCloseable(client), toAutoCloseable(hotMasterClient), toAutoCloseable(coldMasterClient),
      currentDremioDaemon, masterDremioDaemon1, masterDremioDaemon2, zkServer);
  }

  private static Client newClient() {
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
    return ClientBuilder.newBuilder().register(provider).register(MultiPartFeature.class).build();
  }

  private static WebTarget newApiV2Target(Client client, DACDaemon daemon) {
    WebTarget rootTarget = client.target("http://localhost:" + daemon.getWebServer().getPort());
    return rootTarget.path(API_LOCATION);
  }

  private void initClient() {
    client = newClient();
    currentApiV2 = newApiV2Target(client, currentDremioDaemon);
  }

  private void initHotMasterClient(DACDaemon dacDaemon) {
    hotMasterClient = newClient();
    hotMasterApiV2 = newApiV2Target(hotMasterClient, dacDaemon);
  }

  private void initColdMasterClient(DACDaemon dacDaemon) {
    coldMasterClient = newClient();
    coldMasterApiV2 = newApiV2Target(coldMasterClient, dacDaemon);
  }

  private void checkHotMasterOk() throws Exception {
    ServerStatus serverStatus = expectSuccess(hotMasterApiV2.path("/server_status").request(JSON).buildGet(), ServerStatus.class);
    assertEquals(ServerStatus.OK, serverStatus);
  }

  private void checkColdMasterOk() throws Exception {
    ServerStatus serverStatus = expectSuccess(coldMasterApiV2.path("/server_status").request(JSON).buildGet(), ServerStatus.class);
    assertEquals(ServerStatus.OK, serverStatus);
  }

  private void checkNodeOk() throws Exception {
    ServerStatus serverStatus = expectSuccess(currentApiV2.path("/server_status").request(JSON).buildGet(), ServerStatus.class);
    assertEquals(ServerStatus.OK, serverStatus);
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

  /**
   * Return the result of the first future to complete
   *
   * @param futures
   * @return
   */
  private static <T> ListenableFuture<T> any(Iterable<ListenableFuture<T>> futures) {
    final SettableFuture<T> promise = SettableFuture.create();
    for(final ListenableFuture<T> future: futures) {
      Futures.addCallback(future, new FutureCallback<T>() {
        @Override
        public void onSuccess(T result) {
          promise.set(result);
        }

        @Override
        public void onFailure(Throwable t) {
          promise.setException(t);
        }
      }, MoreExecutors.directExecutor());
    }
    return promise;
  }

  @Test
  public void testMasterFailover() throws Exception {
    currentDremioDaemon.startPreServices();

    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    ListenableFuture<?> currentDremioDaemonStarted = executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          currentDremioDaemon.startServices(); // waiting
          currentDremioDaemon.getBindingProvider().lookup(HybridJobsService.class).setPortProvider(jobsPortProvider);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    BindingProvider mp = currentDremioDaemon.getBindingProvider();
    assertEquals(ServerStatus.MASTER_DOWN, mp.lookup(ServerHealthMonitor.class).getStatus());

    List<ListenableFuture<DACDaemon>> masterDremioDaemonsStarted = new ArrayList<>();
    final DACDaemon[] dacDaemons = new DACDaemon[] { masterDremioDaemon1, masterDremioDaemon2};
    for(final DACDaemon daemon: dacDaemons) {
      masterDremioDaemonsStarted.add(executorService.submit(new Callable<DACDaemon>() {
        @Override
        public DACDaemon call() {
          try {
            daemon.startPreServices();
            daemon.startServices();
            daemon.getBindingProvider().lookup(HybridJobsService.class).setPortProvider(jobsPortProvider);
            return daemon;
          } catch(Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }));
    }

    try {
      // Discover which master is hot and which one is cold
      ListenableFuture<DACDaemon> hotMasterDremioDaemonStarted = any(masterDremioDaemonsStarted);
      // Wait on the first master candidate to start
      DACDaemon hotMasterDremioDaemon = hotMasterDremioDaemonStarted.get();
      ListenableFuture<DACDaemon> coldMasterDremioDaemonStarted = null;
      DACDaemon coldMasterDremioDaemon = null;
      for (int i = 0; i < dacDaemons.length; i++) {
        if (dacDaemons[i] == hotMasterDremioDaemon) {
          continue;
        }
        coldMasterDremioDaemon = dacDaemons[i];
        coldMasterDremioDaemonStarted = masterDremioDaemonsStarted.get(i);
        break;
      }

      // Let's check that the loop worked correctly
      assertNotNull(coldMasterDremioDaemonStarted);
      assertNotNull(coldMasterDremioDaemon);

      // Let's confirm that the cold master hasn't started yet
      assertFalse(coldMasterDremioDaemonStarted.isDone());

      // Let's wait on the current daemon to start too now that one master is up
      currentDremioDaemonStarted.get();
      initClient();

      initHotMasterClient(hotMasterDremioDaemon);
      checkHotMasterOk();
      checkNodeOk();
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

      currentDremioDaemon.getBindingCreator().bindProvider(ReflectionAdministrationService.class, () -> {
        ReflectionAdministrationService.Factory factory = mp.lookup(ReflectionAdministrationService.Factory.class);
        return factory.get(new ReflectionContext(DEFAULT_USER_NAME, true));
      });

      DACSecurityContext dacSecurityContext = new DACSecurityContext(new UserName(SystemUser.SYSTEM_USERNAME), SystemUser.SYSTEM_USER, null);
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
      checkHotMasterOk();
      checkNodeOk();

      // stop hot master and wait for cold master to take relay
      hotMasterDremioDaemon.close();
      coldMasterDremioDaemonStarted.get();
      // can only init client once server is started (because of port hunting)
      initColdMasterClient(coldMasterDremioDaemon);

      checkNodeOk();
      checkColdMasterOk();

      sanityCheck();
    } catch (AssertionError | Exception e) {
      // let both dremio daemon start threads exit to avoid deadlock in teardown
      for (ListenableFuture<DACDaemon> dremioDaemonStarted : masterDremioDaemonsStarted) {
        dremioDaemonStarted.cancel(true);
      }
      throw e;
    }
  }

  @Test
  public void testMasterFailoverOnZkSessionLost() throws Exception {
    currentDremioDaemon.startPreServices();

    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    ListenableFuture<?> currentDremioDaemonStarted = executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          currentDremioDaemon.startServices(); // waiting
          currentDremioDaemon.getBindingProvider().lookup(HybridJobsService.class).setPortProvider(jobsPortProvider);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    BindingProvider mp = currentDremioDaemon.getBindingProvider();
    assertEquals(ServerStatus.MASTER_DOWN, mp.lookup(ServerHealthMonitor.class).getStatus());

    List<ListenableFuture<DACDaemon>> masterDremioDaemonsStarted = new ArrayList<>();
    final DACDaemon[] dacDaemons = new DACDaemon[] { masterDremioDaemon1, masterDremioDaemon2};
    for(final DACDaemon daemon: dacDaemons) {
      masterDremioDaemonsStarted.add(executorService.submit(new Callable<DACDaemon>() {
        @Override
        public DACDaemon call() {
          try {
            daemon.startPreServices();
            daemon.startServices();
            daemon.getBindingProvider().lookup(HybridJobsService.class).setPortProvider(jobsPortProvider);
            return daemon;
          } catch(Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }));
    }

    try {
      // Discover which master is hot and which one is cold
      ListenableFuture<DACDaemon> hotMasterDremioDaemonStarted = any(masterDremioDaemonsStarted);
      // Wait on the first master candidate to start
      DACDaemon hotMasterDremioDaemon = hotMasterDremioDaemonStarted.get();
      ListenableFuture<DACDaemon> coldMasterDremioDaemonStarted = null;
      DACDaemon coldMasterDremioDaemon = null;
      for (int i = 0; i < dacDaemons.length; i++) {
        if (dacDaemons[i] == hotMasterDremioDaemon) {
          continue;
        }
        coldMasterDremioDaemon = dacDaemons[i];
        coldMasterDremioDaemonStarted = masterDremioDaemonsStarted.get(i);
        break;
      }

      // Let's check that the loop worked correctly
      assertNotNull(coldMasterDremioDaemonStarted);
      assertNotNull(coldMasterDremioDaemon);

      // Let's confirm that the cold master hasn't started yet
      assertFalse(coldMasterDremioDaemonStarted.isDone());

      // Let's wait on the current daemon to start too now that one master is up
      currentDremioDaemonStarted.get();
      initClient();

      initHotMasterClient(hotMasterDremioDaemon);
      checkHotMasterOk();
      checkNodeOk();
      NamespaceService ns = mp.lookup(NamespaceService.Factory.class).get(DEFAULT_USERNAME);
      final SabotContext sabotContext = mp.lookup(SabotContext.class);

      final DatasetVersionMutator datasetVersionMutator = new DatasetVersionMutator(
        mp.lookup(InitializerRegistry.class),
        mp.lookup(LegacyKVStoreProvider.class),
        ns,
        mp.lookup(JobsService.class),
        mp.lookup(CatalogService.class),
        sabotContext.getOptionManager());

      TestUtilities.addClasspathSourceIf(mp.lookup(CatalogService.class));
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
      checkHotMasterOk();
      checkNodeOk();

      // "kill" zookeeper connection and check hot is killed
      KillZkSession.kill((ZKClusterCoordinator) hotMasterDremioDaemon.getBindingProvider().lookup(ClusterCoordinator.class));
      coldMasterDremioDaemonStarted.get();
      // can only init client once server is started (because of port hunting)
      initColdMasterClient(coldMasterDremioDaemon);

      checkNodeOk();
      checkColdMasterOk();

      sanityCheck();
    } catch (AssertionError | Exception e) {
      // let both dremio daemon start threads exit to avoid deadlock in teardown
      for (ListenableFuture<DACDaemon> dremioDaemonStarted : masterDremioDaemonsStarted) {
        dremioDaemonStarted.cancel(true);
      }
      throw e;
    }
  }

  public UserLoginSession login() {
    UserLogin userLogin = new UserLogin(DEFAULT_USERNAME, DEFAULT_PASSWORD);
    return expectSuccess(currentApiV2.path("/login").request(JSON).buildPost(Entity.json(userLogin)), UserLoginSession.class);
  }
}
