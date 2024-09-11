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

import static com.dremio.common.utils.PathUtils.getKeyJoiner;
import static com.dremio.common.utils.PathUtils.getPathJoiner;
import static com.dremio.dac.server.FamilyExpectation.CLIENT_ERROR;
import static com.dremio.dac.server.JobsServiceTestUtils.submitJobAndGetData;
import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static javax.ws.rs.client.Entity.entity;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;
import com.dremio.dac.daemon.DACDaemonModule;
import com.dremio.dac.daemon.DACModule;
import com.dremio.dac.daemon.ZkServer;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetSearchUIs;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DatasetUIWithHistory;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.explore.model.InitialDataPreviewResponse;
import com.dremio.dac.explore.model.InitialPendingTransformResponse;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.admin.Setting;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetDownloadManager;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.support.ImmutableSupportRequest;
import com.dremio.dac.support.SupportService;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.NodeRegistration;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.file.FilePath;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.grpc.ManagedChannel;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import javax.inject.Provider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import org.apache.arrow.memory.BufferAllocator;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/** base class for server tests */
@Category(DremioMultiNodeTests.class)
public abstract class BaseTestServer extends BaseClientUtils {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseTestServer.class);

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(Duration.ofMinutes(2));

  private static final String API_LOCATION = "apiv2";
  private static final String API_LOCATION_v3 = "api/v3";
  private static final String NESSIE_PROXY = "nessie-proxy";
  private static final String PUBLIC_API_LOCATION = "api";
  private static final String SCIM_V2_API_LOCATION = "scim/v2";
  private static final String OAUTH_API_LOCATION = "oauth";

  public static final String USERNAME = SampleDataPopulator.TEST_USER_NAME;
  protected static final String DEFAULT_USERNAME = SampleDataPopulator.DEFAULT_USER_NAME;
  protected static final String DEFAULT_PASSWORD = SampleDataPopulator.PASSWORD;
  protected static final SecurityContext DEFAULT_SECURITY_CONTEXT =
      new SecurityContext() {
        @Override
        public Principal getUserPrincipal() {
          return () -> DEFAULT_USERNAME;
        }

        @Override
        public boolean isUserInRole(String role) {
          return true; // admin
        }

        @Override
        public boolean isSecure() {
          return true;
        }

        @Override
        public String getAuthenticationScheme() {
          return null;
        }
      };

  private PrintWriter docLog;
  private UserLoginSession uls;

  private static boolean defaultUser = true;
  private static boolean testApiEnabled = true;
  private static boolean inMemoryStorage = true;
  private static boolean addDefaultUser = false;

  protected static boolean isDefaultUserEnabled() {
    return defaultUser;
  }

  protected static void enableDefaultUser(boolean enabled) {
    defaultUser = enabled;
  }

  protected static void enableTestAPi(boolean enabled) {
    testApiEnabled = enabled;
  }

  protected static void inMemoryStorage(boolean enabled) {
    inMemoryStorage = enabled;
  }

  protected static void addDefaultUser(boolean enabled) {
    addDefaultUser = enabled;
  }

  protected void doc(String message) {
    docLog.println("[doc] " + message);
  }

  @Rule
  public TestWatcher watcher =
      new TestWatcher() {
        private TimedBlock b;

        @Override
        protected void starting(Description description) {
          b = Timer.time(desc(description));
          logger.info(format("Test starting: %s", desc(description)));
          AccessLogFilter accessLogFilter = currentDremioDaemon.getWebServer().getAccessLogFilter();
          if (accessLogFilter != null) {

            // sanitize method name from test since it could be a parameterized test with random
            // characters (or be really long).
            String methodName = description.getMethodName();
            methodName = methodName.replaceAll("\\W+", "");
            if (methodName.length() > 200) {
              methodName = methodName.substring(0, 200);
            }
            File logFile =
                new File(
                    format(
                        "target/test-access_logs/%s-%s.log",
                        description.getClassName(), methodName));
            File parent = logFile.getParentFile();
            if ((parent.exists() && !parent.isDirectory())
                || (!parent.exists() && !parent.mkdirs())) {
              throw new RuntimeException("could not create dir " + parent);
            }
            if (logFile.exists()) {
              logFile.delete();
            }
            try {
              logger.info(format("access log at %s", logFile.getAbsolutePath()));
              docLog = new PrintWriter(logFile);
            } catch (FileNotFoundException e) {
              throw new RuntimeException(e);
            }
            accessLogFilter.startLoggingToFile(docLog);
          }
        }

        @Override
        protected void finished(Description description) {
          b.close();
          b = null;
          logger.info("Test finished: " + desc(description));
          AccessLogFilter accessLogFilter = currentDremioDaemon.getWebServer().getAccessLogFilter();
          if (accessLogFilter != null) {
            accessLogFilter.stopLoggingToFile();
          }
          if (docLog != null) {
            docLog.close();
          }
        }

        private String desc(Description description) {
          return description.getClassName() + "." + description.getMethodName();
        }
      };

  @Before
  public void resetDefaultUser() {
    if (defaultUser) {
      try {
        SampleDataPopulator.addDefaultFirstUser(getUserService(), getNamespaceService());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      login();
    }
  }

  private static Client client;
  private static DremioClient dremioClient;
  private static WebTarget rootTarget;
  private static WebTarget metricsEndpoint;
  private static WebTarget apiV2;
  private static WebTarget apiV3;
  private static WebTarget nessieProxy;
  private static WebTarget masterApiV2;
  private static WebTarget publicAPI;
  private static WebTarget masterPublicAPI;
  private static WebTarget scimV2API;
  private static WebTarget oAuthApi;
  private static DACDaemon executorDaemon;
  private static DACDaemon currentDremioDaemon;
  private static DACDaemon masterDremioDaemon;
  private static SampleDataPopulator populator;

  public static void setClient(Client client) {
    Preconditions.checkState(BaseTestServer.client == null);
    BaseTestServer.client = Preconditions.checkNotNull(client);
  }

  public static void setCurrentDremioDaemon(DACDaemon currentDremioDaemon) {
    Preconditions.checkState(BaseTestServer.currentDremioDaemon == null);
    BaseTestServer.currentDremioDaemon = Preconditions.checkNotNull(currentDremioDaemon);
  }

  protected static void closeCurrentDremioDaemon() throws Exception {
    Preconditions.checkNotNull(BaseTestServer.currentDremioDaemon);
    try {
      BaseTestServer.currentDremioDaemon.close();
    } finally {
      BaseTestServer.currentDremioDaemon = null;
    }
  }

  public static void setMasterDremioDaemon(DACDaemon masterDremioDaemon) {
    Preconditions.checkState(BaseTestServer.masterDremioDaemon == null);
    BaseTestServer.masterDremioDaemon = Preconditions.checkNotNull(masterDremioDaemon);
  }

  public static void setPopulator(SampleDataPopulator populator) throws Exception {
    if (BaseTestServer.populator != null) {
      BaseTestServer.populator.close();
    }
    BaseTestServer.populator = Preconditions.checkNotNull(populator);
  }

  protected static WebTarget getAPIv2() {
    return apiV2;
  }

  protected static WebTarget getAPIv3() {
    return apiV3;
  }

  protected static WebTarget getNessieProxy() {
    return nessieProxy;
  }

  protected static WebTarget getScimAPIv2() {
    return scimV2API;
  }

  protected static WebTarget getOAuthApi() {
    return oAuthApi;
  }

  protected static WebTarget getMetricsEndpoint() {
    return metricsEndpoint;
  }

  protected static WebTarget getMasterAPIv2() {
    return masterApiV2;
  }

  public static WebTarget getPublicAPI(Integer version) {
    return publicAPI.path("v" + version);
  }

  public static WebTarget getPublicAPI(String version) {
    return publicAPI.path(version);
  }

  public static WebTarget getMasterPublicAPI(Integer version) {
    return masterPublicAPI.path("v" + version);
  }

  protected static DACDaemon getCurrentDremioDaemon() {
    return currentDremioDaemon;
  }

  protected static DACDaemon getMasterDremioDaemon() {
    return masterDremioDaemon;
  }

  protected static DACDaemon getExecutorDaemon() {
    return executorDaemon;
  }

  public static boolean isMultinode() {
    return System.getProperty("dremio_multinode", null) != null;
  }

  protected DremioClient getRpcClient() throws RpcException {
    if (dremioClient == null) {
      dremioClient = new DremioClient(true);
      dremioClient.connect(
          new Properties() {
            {
              put("direct", "localhost:" + l(UserServer.class).getPort());
              put("user", "dremio");
              put("password", "dremio123");
            }
          });
    }
    return dremioClient;
  }

  @ClassRule public static TemporaryFolder folder0 = new TemporaryFolder();

  @ClassRule public static TemporaryFolder folder1 = new TemporaryFolder();

  @ClassRule public static final TemporaryFolder folder2 = new TemporaryFolder();

  @ClassRule public static final TemporaryFolder folder3 = new TemporaryFolder();

  protected static void initClient() throws Exception {
    initClient(newClientObjectMapper());
  }

  private static void initClient(ObjectMapper mapper) throws Exception {
    addDummySecurityContextForDefaultUser(currentDremioDaemon);

    final Path path = new File(folder0.getRoot().getAbsolutePath() + "/testplugins").toPath();
    if (!Files.exists(path)) {
      TestUtilities.addDefaultTestPlugins(getCatalogService(), path.toString());
    }

    setPopulator(
        new SampleDataPopulator(
            getSabotContext(),
            getSourceService(),
            getDatasetVersionMutator(),
            getUserService(),
            getNamespaceService(),
            DEFAULT_USERNAME,
            getCollaborationHelper()));

    client = newClient(mapper);
    rootTarget = client.target("http://localhost:" + currentDremioDaemon.getWebServer().getPort());
    final WebTarget livenessServiceTarget =
        client.target(
            "http://localhost:" + currentDremioDaemon.getLivenessService().getLivenessPort());
    metricsEndpoint = livenessServiceTarget.path("metrics");
    apiV2 = rootTarget.path(API_LOCATION);
    apiV3 = rootTarget.path(API_LOCATION_v3);
    nessieProxy = rootTarget.path(NESSIE_PROXY);
    publicAPI = rootTarget.path(PUBLIC_API_LOCATION);
    scimV2API = rootTarget.path(SCIM_V2_API_LOCATION);
    oAuthApi = rootTarget.path(OAUTH_API_LOCATION);

    if (isMultinode()) {
      masterApiV2 =
          client
              .target("http://localhost:" + masterDremioDaemon.getWebServer().getPort())
              .path(API_LOCATION);
      masterPublicAPI =
          client
              .target("http://localhost:" + masterDremioDaemon.getWebServer().getPort())
              .path(PUBLIC_API_LOCATION);
    } else {
      masterApiV2 = apiV2;
      masterPublicAPI = publicAPI;
    }
  }

  /**
   * Do not call this method directly unless from an "overriden" static "init" method of a subclass
   */
  @BeforeClass
  public static void init() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      initializeCluster(new DACDaemonModule(), null, null, true);
    }
  }

  protected static void initializeCluster(
      DACModule dacModule,
      TemporaryFolder distPathsFolder,
      TemporaryFolder localPathsFolder,
      boolean memory)
      throws Exception {
    if (distPathsFolder != null) {
      folder0 = distPathsFolder;
      folder1 = localPathsFolder;
      inMemoryStorage = memory;
    }
    initializeCluster(dacModule, o -> o);
  }

  protected static void initializeCluster(
      DACModule dacModule, Function<ObjectMapper, ObjectMapper> mapperUpdate) throws Exception {
    initializeCluster(dacModule, mapperUpdate, new HashMap<>());
  }

  protected static void initializeCluster(
      DACModule dacModule,
      Function<ObjectMapper, ObjectMapper> mapperUpdate,
      Map<String, Object> moreConfigs)
      throws Exception {

    Preconditions.checkState(currentDremioDaemon == null, "Old cluster not stopped properly");
    Preconditions.checkState(masterDremioDaemon == null, "Old cluster not stopped properly");
    Preconditions.checkState(executorDaemon == null, "Old cluster not stopped properly");

    // Turning on flag for NaaS
    System.setProperty("nessie.source.resource.testing.enabled", "true");

    String rootFolder0 = folder0.getRoot().getAbsolutePath();
    String rootFolder1 = folder1.getRoot().getAbsolutePath();
    String rootFolder2 = folder2.getRoot().getAbsolutePath();
    String rootFolder3 = folder3.getRoot().getAbsolutePath();

    // we'll share the same file:/// writepath for all nodes. Pdfs causes
    // problems as it uses static variables so resolution doesn't work in a
    // single ClassLoader
    final String distpath = "file://" + rootFolder0;

    if (isMultinode()) {
      logger.info("Running tests in multinode mode");

      // run all tests on remote coordinator node relying on additional remote executor node

      // jobsResultsStore stored in <distpath>/results and accelerator store in
      // <distpath>/accelerator, both of which
      // need to exist
      Files.createDirectories(new File(rootFolder0 + "/results").toPath());
      Files.createDirectories(new File(rootFolder0 + "/accelerator").toPath());
      Files.createDirectories(new File(rootFolder0 + "/scratch").toPath());
      Files.createDirectories(new File(rootFolder0 + "/metadata").toPath());
      Files.createDirectories(new File(rootFolder0 + "/gandiva").toPath());
      Files.createDirectories(new File(rootFolder0 + "/system_iceberg_tables").toPath());
      Files.createDirectories(new File(rootFolder0 + "/node_history").toPath());

      // Get a random port
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        socket.setReuseAddress(true);
        port = socket.getLocalPort();
      }

      // create master node.
      masterDremioDaemon =
          DACDaemon.newDremioDaemon(
              DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
                  .autoPort(true)
                  .allowTestApis(testApiEnabled)
                  .serveUI(false)
                  .jobServerEnabled(true)
                  .inMemoryStorage(inMemoryStorage)
                  .writePath(rootFolder1)
                  .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
                  .with(DremioConfig.ENABLE_EXECUTOR_BOOL, false)
                  .with(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_PORT_INT, port)
                  .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
                  .with(DremioConfig.NESSIE_SERVICE_ENABLED_BOOLEAN, true)
                  .with(DremioConfig.NESSIE_SERVICE_IN_MEMORY_BOOLEAN, true)
                  .clusterMode(ClusterMode.DISTRIBUTED),
              DremioTest.CLASSPATH_SCAN_RESULT,
              dacModule);
      masterDremioDaemon.init();

      int zkPort = masterDremioDaemon.getInstance(ZkServer.class).getPort();
      int fabricPort = masterDremioDaemon.getInstance(FabricService.class).getPort() + 1;

      // remote coordinator node
      DACConfig dacConfig =
          DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .isMaster(false)
              .autoPort(true)
              .allowTestApis(testApiEnabled)
              .serveUI(false)
              .inMemoryStorage(inMemoryStorage)
              .jobServerEnabled(true)
              .writePath(rootFolder2)
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
              .clusterMode(ClusterMode.DISTRIBUTED)
              .localPort(fabricPort)
              .isRemote(true)
              .with(DremioConfig.ENABLE_EXECUTOR_BOOL, false)
              .with(DremioConfig.NESSIE_SERVICE_ENABLED_BOOLEAN, true)
              .with(DremioConfig.NESSIE_SERVICE_IN_MEMORY_BOOLEAN, true)
              .zk("localhost:" + zkPort);
      for (Map.Entry<String, Object> entry : moreConfigs.entrySet()) {
        dacConfig = dacConfig.with(entry.getKey(), entry.getValue());
      }
      currentDremioDaemon =
          DACDaemon.newDremioDaemon(dacConfig, DremioTest.CLASSPATH_SCAN_RESULT, dacModule);
      currentDremioDaemon.init();

      // remote executor node
      DACConfig dacExecConfig =
          DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .autoPort(true)
              .allowTestApis(testApiEnabled)
              .serveUI(false)
              .inMemoryStorage(inMemoryStorage)
              .with(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
              .writePath(rootFolder3)
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .clusterMode(ClusterMode.DISTRIBUTED)
              .localPort(fabricPort)
              .isRemote(true)
              .zk("localhost:" + zkPort);
      for (Map.Entry<String, Object> entry : moreConfigs.entrySet()) {
        dacExecConfig = dacExecConfig.with(entry.getKey(), entry.getValue());
      }
      executorDaemon =
          DACDaemon.newDremioDaemon(dacExecConfig, DremioTest.CLASSPATH_SCAN_RESULT, dacModule);
      executorDaemon.init();
    } else {
      logger.info("Running tests in local mode");
      DACConfig dacConfig =
          DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .autoPort(true)
              .allowTestApis(testApiEnabled)
              .serveUI(false)
              .addDefaultUser(addDefaultUser)
              .inMemoryStorage(inMemoryStorage)
              .writePath(rootFolder1)
              .with(DremioConfig.METADATA_PATH_STRING, distpath + "/metadata")
              .with(DremioConfig.ACCELERATOR_PATH_STRING, distpath + "/accelerator")
              .with(DremioConfig.GANDIVA_CACHE_PATH_STRING, distpath + "/gandiva")
              .with(
                  DremioConfig.SYSTEM_ICEBERG_TABLES_PATH_STRING,
                  distpath + "/system_iceberg_tables")
              .with(DremioConfig.NODE_HISTORY_PATH_STRING, distpath + "/node_history")
              .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
              .with(DremioConfig.NESSIE_SERVICE_ENABLED_BOOLEAN, true)
              .with(DremioConfig.NESSIE_SERVICE_IN_MEMORY_BOOLEAN, true)
              .clusterMode(DACDaemon.ClusterMode.LOCAL);
      for (Map.Entry<String, Object> entry : moreConfigs.entrySet()) {
        dacConfig = dacConfig.with(entry.getKey(), entry.getValue());
      }
      currentDremioDaemon =
          DACDaemon.newDremioDaemon(dacConfig, DremioTest.CLASSPATH_SCAN_RESULT, dacModule);
      currentDremioDaemon.init();
    }

    initClient(mapperUpdate.apply(newClientObjectMapper()));
  }

  protected static NamespaceService getNamespaceService() {
    return l(NamespaceService.class);
  }

  protected static CatalogService getCatalogService() {
    return l(CatalogService.class);
  }

  protected static SourceService getSourceService() {
    return l(SourceService.class);
  }

  protected static UserService getUserService() {
    return l(UserService.class);
  }

  protected static JobsService getJobsService() {
    return l(JobsService.class);
  }

  protected static OptionManager getOptionManager() {
    return l(OptionManager.class);
  }

  protected static CollaborationHelper getCollaborationHelper() {
    return l(CollaborationHelper.class);
  }

  protected static DatasetVersionMutator getDatasetVersionMutator() {
    return l(DatasetVersionMutator.class);
  }

  protected static SabotContext getSabotContext() {
    return l(SabotContext.class);
  }

  protected static DremioRootAllocator getRootAllocator() {
    BufferAllocator allocator = getSabotContext().getAllocator();
    if (allocator instanceof DremioRootAllocator) {
      return (DremioRootAllocator) allocator;
    }
    throw new IllegalStateException("Not a DremioRootAllocator: " + allocator.getClass().getName());
  }

  protected static <T> T l(Class<T> clazz) {
    return currentDremioDaemon.getInstance(clazz);
  }

  protected static <T> Provider<T> p(Class<T> clazz) {
    return currentDremioDaemon.getProvider(clazz);
  }

  protected static <T> T lMaster(Class<T> clazz) {
    if (masterDremioDaemon != null) {
      return masterDremioDaemon.getInstance(clazz);
    }
    return l(clazz);
  }

  protected static <T> Provider<T> pMaster(Class<T> clazz) {
    if (masterDremioDaemon != null) {
      return masterDremioDaemon.getProvider(clazz);
    }
    return p(clazz);
  }

  protected static ManagedChannel getConduitChannelTo(DACDaemon daemon) {
    ConduitServer server = daemon.getInstance(ConduitServer.class);
    ConduitProvider conduitProvider = daemon.getInstance(ConduitProvider.class);

    final int port = server.getPort();
    final CoordinationProtos.NodeEndpoint target =
        CoordinationProtos.NodeEndpoint.newBuilder()
            .setAddress("127.0.0.1")
            .setConduitPort(port)
            .build();

    return conduitProvider.getOrCreateChannel(target);
  }

  protected void deleteSource(String name) {
    ((CatalogServiceImpl) getCatalogService()).deleteSource(name);
  }

  public static void addDummySecurityContextForDefaultUser(DACDaemon daemon) {
    daemon.getBindingCreator().bindIfUnbound(SecurityContext.class, DEFAULT_SECURITY_CONTEXT);
  }

  protected static void closeExecutorDaemon() throws Exception {
    try {
      executorDaemon.close();
    } finally {
      executorDaemon = null;
    }
  }

  private static int getResourceAllocatorCount(DACDaemon daemon) {
    return daemon
        .getInstance(BufferAllocatorFactory.class)
        .getBaseAllocator()
        .getChildAllocators()
        .size();
  }

  private static int getQueryPlanningAllocatorCount(DACDaemon daemon) {
    final BufferAllocator queryPlanningAllocator =
        daemon.getInstance(SabotContext.class).getQueryPlanningAllocator();
    if (queryPlanningAllocator == null) {
      return 0;
    }
    return queryPlanningAllocator.getChildAllocators().size();
  }

  @AfterClass
  public static void close() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServer.@AfterClass")) {
      if (currentDremioDaemon != null) {
        assertNoRunningQueries(currentDremioDaemon);
        assertAllocatorsAreClosed(currentDremioDaemon);
      }

      // in case another test disables the default user and forgets to enable it back
      // again at the end
      defaultUser = true;

      AutoCloseables.close(
          dremioClient,
          () -> {
            if (client != null) {
              client.close();
            }
          },
          executorDaemon,
          currentDremioDaemon,
          masterDremioDaemon,
          populator);
    } finally {
      dremioClient = null;
      executorDaemon = null;
      currentDremioDaemon = null;
      masterDremioDaemon = null;
      populator = null;
    }
  }

  public static void assertNoRunningQueries(DACDaemon daemon) throws Exception {
    // Prevent new incoming job requests
    final NodeRegistration nodeRegistration = daemon.getInstance(NodeRegistration.class);
    final ForemenWorkManager foremenWorkManager = daemon.getInstance(ForemenWorkManager.class);
    nodeRegistration.close();
    foremenWorkManager.close();

    // Drain actively running jobs
    await()
        .atMost(Duration.ofSeconds(100))
        .until(() -> foremenWorkManager.getActiveQueryCount() == 0);

    // Fail if any jobs are still running and record query information
    final StringBuilder msg = new StringBuilder();
    msg.append("There are actively running queries that have not finished:\n");
    foremenWorkManager
        .getActiveProfiles()
        .forEach(
            profile ->
                msg.append("Query ")
                    .append(QueryIdHelper.getQueryId(profile.getId()))
                    .append(": ")
                    .append(profile.getQuery())
                    .append("\n\n"));
    assertEquals(msg.toString(), 0, foremenWorkManager.getActiveQueryCount());
  }

  public static void assertAllocatorsAreClosed(DACDaemon daemon) {
    int resourceAllocatorCount = getResourceAllocatorCount(daemon);
    int queryPlanningAllocatorCount = getQueryPlanningAllocatorCount(daemon);
    SoftAssertions.assertSoftly(
        softly -> {
          softly
              .assertThat(resourceAllocatorCount)
              .withFailMessage("Not all resource allocators were closed.")
              .isEqualTo(0);
          softly
              .assertThat(queryPlanningAllocatorCount)
              .withFailMessage(
                  "Not all query-planning allocators were closed. "
                      + "There are queries that are still running and have not been cancelled.")
              .isEqualTo(0);
        });
  }

  protected void login() {
    login(DEFAULT_USERNAME, DEFAULT_PASSWORD);
  }

  protected void login(final String userName, final String password) {
    UserLogin userLogin = new UserLogin(userName, password);
    this.setUls(
        expectSuccess(
            getAPIv2().path("/login").request(JSON).buildPost(Entity.json(userLogin)),
            UserLoginSession.class));
  }

  protected static SampleDataPopulator getPopulator() {
    return populator;
  }

  protected String getAuthHeaderName() {
    return HttpHeader.AUTHORIZATION.toString();
  }

  protected String getAuthHeaderValue() {
    return "_dremio" + getUls().getToken();
  }

  protected Invocation.Builder getBuilder(WebTarget webTarget) {
    return webTarget.request(JSON).header(getAuthHeaderName(), getAuthHeaderValue());
  }

  protected Invocation.Builder getBuilder(String apiUrl) {
    return getBuilder(
        client.target(
            "http://localhost:"
                + currentDremioDaemon.getWebServer().getPort()
                + "/"
                + API_LOCATION
                + apiUrl));
  }

  protected UserLoginSession getUls() {
    return uls;
  }

  protected void setUls(UserLoginSession uls) {
    this.uls = uls;
  }

  protected static void populateInitialData()
      throws DatasetNotFoundException,
          DatasetVersionNotFoundException,
          ExecutionSetupException,
          NamespaceException,
          IOException {
    populator.populateInitialData();
  }

  public static void clearAllDataExceptUser() throws IOException, NamespaceException {
    TestUtilities.clear(
        lMaster(CatalogService.class),
        lMaster(LegacyKVStoreProvider.class),
        lMaster(KVStoreProvider.class),
        ImmutableList.of(SimpleUserService.USER_STORE),
        ImmutableList.of("cp"));
    if (isMultinode()) {
      ((CatalogServiceImpl) getCatalogService()).synchronizeSources();
    }
  }

  protected void setSpace() throws NamespaceException, IOException {
    clearAllDataExceptUser();
    final SpaceConfig foo = new SpaceConfig().setName("spacefoo");
    SpacePath spacePath = new SpacePath(new SpaceName(foo.getName()));
    NamespaceService namespaceService = getNamespaceService();
    namespaceService.addOrUpdateSpace(spacePath.toNamespaceKey(), foo);
    namespaceService.addOrUpdateFolder(
        new FolderPath("spacefoo.folderbar").toNamespaceKey(),
        new FolderConfig().setName("folderbar").setFullPathList(asList("spacefoo", "folderbar")));
    namespaceService.addOrUpdateFolder(
        new FolderPath("spacefoo.folderbar.folderbaz").toNamespaceKey(),
        new FolderConfig()
            .setName("folderbaz")
            .setFullPathList(asList("spacefoo", "folderbar", "folderbaz")));
  }

  protected DatasetPath getDatasetPath(DatasetUI datasetUI) {
    return new DatasetPath(datasetUI.getFullPath());
  }

  protected DatasetVersionResourcePath getDatasetVersionPath(DatasetUI datasetUI) {
    return new DatasetVersionResourcePath(getDatasetPath(datasetUI), datasetUI.getDatasetVersion());
  }

  protected String versionedResourcePath(DatasetUI datasetUI) {
    return getPathJoiner()
        .join(
            "/dataset",
            getKeyJoiner().join(datasetUI.getFullPath()),
            "version",
            datasetUI.getDatasetVersion());
  }

  protected String resourcePath(DatasetUI datasetUI) {
    return getPathJoiner().join("/dataset", getKeyJoiner().join(datasetUI.getFullPath()));
  }

  protected String getName(DatasetUI datasetUI) {
    return datasetUI.getFullPath().get(datasetUI.getFullPath().size() - 1);
  }

  protected String getRoot(DatasetUI datasetUI) {
    return datasetUI.getFullPath().get(0);
  }

  protected Invocation getDatasetInvocation(DatasetPath datasetPath) {
    return getBuilder(getAPIv2().path("dataset/" + datasetPath.toString())).buildGet();
  }

  protected DatasetUI getDataset(DatasetPath datasetPath) {
    return expectSuccess(getDatasetInvocation(datasetPath), DatasetUI.class);
  }

  protected DatasetUI getVersionedDataset(DatasetVersionResourcePath datasetVersionPath) {
    final Invocation invocation =
        getBuilder(getAPIv2().path(getPathJoiner().join(datasetVersionPath.toString(), "preview")))
            .buildGet();

    return expectSuccess(invocation, InitialPreviewResponse.class).getDataset();
  }

  protected InitialPreviewResponse getPreview(DatasetUI datasetUI) {
    final Invocation invocation =
        getBuilder(
                getAPIv2().path(getPathJoiner().join(versionedResourcePath(datasetUI), "preview")))
            .buildGet();

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  /**
   * Get the preview response for given dataset path. Dataset can be physical or virutal.
   *
   * @param datasetPath
   * @return
   */
  protected InitialDataPreviewResponse getPreview(DatasetPath datasetPath) {
    final Invocation invocation =
        getBuilder(getAPIv2().path("dataset/" + datasetPath.toPathString() + "/preview"))
            .buildGet();

    return expectSuccess(invocation, InitialDataPreviewResponse.class);
  }

  protected JobDataFragment getData(String paginationUrl, long offset, long limit) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(paginationUrl)
                    .queryParam("offset", offset)
                    .queryParam("limit", limit))
            .buildGet();

    return expectSuccess(invocation, JobDataFragment.class);
  }

  protected InitialPreviewResponse transform(DatasetUI datasetUI, TransformBase transformBase) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(
                        getPathJoiner()
                            .join(versionedResourcePath(datasetUI), "transformAndPreview"))
                    .queryParam("newVersion", newVersion()))
            .buildPost(entity(transformBase, JSON));

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  protected DatasetSearchUIs search(String filter) {
    final Invocation invocation =
        getBuilder(getAPIv2().path("datasets/search").queryParam("filter", filter)).buildGet();

    return expectSuccess(invocation, DatasetSearchUIs.class);
  }

  protected Invocation reapplyInvocation(DatasetVersionResourcePath versionResourcePath) {
    return getBuilder(getAPIv2().path(versionResourcePath.toString() + "/editOriginalSql"))
        .buildPost(null);
  }

  protected InitialPreviewResponse reapply(DatasetVersionResourcePath versionResourcePath) {
    return expectSuccess(reapplyInvocation(versionResourcePath), InitialPreviewResponse.class);
  }

  protected DatasetUIWithHistory save(DatasetUI datasetUI, String saveVersion) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("savedTag", saveVersion))
            .buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  protected DatasetUI rename(DatasetPath datasetPath, String newName) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path("dataset/" + datasetPath.toString() + "/rename")
                    .queryParam("renameTo", newName))
            .buildPost(null);

    return expectSuccess(invocation, DatasetUI.class);
  }

  protected DatasetUI move(DatasetPath currenPath, DatasetPath newPath) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path("dataset/" + currenPath.toString() + "/moveTo/" + newPath.toString()))
            .buildPost(null);

    return expectSuccess(invocation, DatasetUI.class);
  }

  protected DatasetUIWithHistory saveAs(DatasetUI datasetUI, DatasetPath newName) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("as", newName))
            .buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  protected void saveAsExpectError(DatasetUI datasetUI, DatasetPath newName) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("as", newName))
            .buildPost(entity("", JSON));

    expectError(CLIENT_ERROR, invocation, ValidationErrorMessage.class);
  }

  protected DatasetUI delete(String datasetResourcePath, String savedVersion) {
    final Invocation invocation =
        getBuilder(getAPIv2().path(datasetResourcePath).queryParam("savedTag", savedVersion))
            .buildDelete();

    return expectSuccess(invocation, DatasetUI.class);
  }

  protected void saveExpectConflict(DatasetUI datasetUI, String saveVersion) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("savedTag", saveVersion))
            .buildPost(entity("", JSON));

    expectStatus(Status.CONFLICT, invocation);
  }

  protected InitialPendingTransformResponse transformPeek(
      DatasetUI datasetUI, TransformBase transform) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/transformPeek")
                    .queryParam("newVersion", newVersion()))
            .buildPost(entity(transform, JSON));

    return expectSuccess(invocation, InitialPendingTransformResponse.class);
  }

  protected DatasetUI createDatasetFromSQLAndSave(
      DatasetPath datasetPath, String sql, List<String> context) {
    InitialPreviewResponse datasetCreateResponse = createDatasetFromSQL(sql, context);
    return saveAs(datasetCreateResponse.getDataset(), datasetPath).getDataset();
  }

  protected InitialPreviewResponse createDatasetFromSQL(String sql, List<String> context) {
    return expectSuccess(
        getBuilder(
                getAPIv2().path("datasets/new_untitled_sql").queryParam("newVersion", newVersion()))
            .buildPost(entity(new CreateFromSQL(sql, context), JSON)), // => sending
        InitialPreviewResponse.class); // <= receiving
  }

  // Wait for the job complete
  protected void waitForJobComplete(String jobId) {
    int retry = 20;
    while (retry-- >= 0) {
      try {
        Thread.sleep(1000);
        JobUI job =
            expectSuccess(
                getBuilder(getAPIv2().path(getPathJoiner().join("job", jobId)))
                    .buildGet(), // => sending
                JobUI.class); // <= receiving
        if (job.getJobAttempt().getState() == JobState.COMPLETED) {
          break;
        } else if (job.getJobAttempt().getState() == JobState.FAILED
            || job.getJobAttempt().getState() == JobState.CANCELED
            || job.getJobAttempt().getState() == JobState.CANCELLATION_REQUESTED) {
          fail(String.format("Job (%s) failed.", jobId));
        }
      } catch (InterruptedException e) {
        fail(e.getMessage());
      } catch (Exception e) {
        // Ignore, retry
      }
    }

    if (retry == 0) {
      fail(String.format("Job (%s) timed out.", jobId));
    }
  }

  protected InitialPreviewResponse createDatasetFromParent(String parentDataset) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path("datasets/new_untitled")
                    .queryParam("newVersion", newVersion())
                    .queryParam("parentDataset", parentDataset))
            .buildPost(null);

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  protected DatasetUI createDatasetFromParentAndSave(
      DatasetPath newDatasetPath, String parentDataset) {
    InitialPreviewResponse response = createDatasetFromParent(parentDataset);

    return saveAs(response.getDataset(), newDatasetPath).getDataset();
  }

  protected DatasetUI createDatasetFromParentAndSave(String newDataSetName, String parentDataset)
      throws Exception {
    setSpace();
    InitialPreviewResponse response = createDatasetFromParent(parentDataset);

    return saveAs(
            response.getDataset(),
            new DatasetPath("spacefoo.folderbar.folderbaz." + newDataSetName))
        .getDataset();
  }

  protected SqlQuery getQueryFromConfig(DatasetUI config) {
    return new SqlQuery(config.getSql(), config.getContext(), DEFAULT_USERNAME);
  }

  protected SqlQuery getQueryFromConfig(VirtualDatasetUI config) {
    return new SqlQuery(config.getSql(), config.getState().getContextList(), DEFAULT_USERNAME);
  }

  protected SqlQuery getQueryFromSQL(String sql) {
    return new SqlQuery(sql, DEFAULT_USERNAME);
  }

  protected JobId submitAndWaitUntilSubmitted(JobRequest request, JobStatusListener listener) {
    return JobsServiceTestUtils.submitAndWaitUntilSubmitted(getJobsService(), request, listener);
  }

  protected JobId submitAndWaitUntilSubmitted(JobRequest request) {
    return JobsServiceTestUtils.submitAndWaitUntilSubmitted(getJobsService(), request);
  }

  protected JobId submitJobAndWaitUntilCompletion(JobRequest request, JobStatusListener listener) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(getJobsService(), request, listener)
        .getJobId();
  }

  protected boolean submitJobAndCancelOnTimeOut(JobRequest request, long timeOutInMillis)
      throws Exception {
    return JobsServiceTestUtils.submitJobAndCancelOnTimeout(
        getJobsService(), request, timeOutInMillis);
  }

  protected static JobId submitJobAndWaitUntilCompletion(JobRequest request) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(getJobsService(), request);
  }

  protected static JobSubmission getJobSubmissionAfterJobCompletion(JobRequest request) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(
        getJobsService(), request, JobStatusListener.NO_OP);
  }

  protected void runQuery(
      JobsService jobsService,
      String name,
      int rows,
      int columns,
      FolderPath parent,
      BufferAllocator allocator)
      throws JobNotFoundException {
    FilePath filePath;
    if (parent == null) {
      filePath =
          new FilePath(
              ImmutableList.of(HomeName.getUserHomePath(DEFAULT_USER_NAME).getName(), name));
    } else {
      List<String> path = Lists.newArrayList(parent.toPathList());
      path.add(name);
      filePath = new FilePath(path);
    }
    try (final JobDataFragment truncData =
        submitJobAndGetData(
            jobsService,
            JobRequest.newBuilder()
                .setSqlQuery(
                    new SqlQuery(
                        format("select * from %s", filePath.toPathString()), DEFAULT_USER_NAME))
                .build(),
            0,
            rows + 1,
            allocator)) {
      assertEquals(rows, truncData.getReturnedRowCount());
      assertEquals(columns, truncData.getColumns().size());
    }
  }

  protected static UserBitShared.QueryProfile getQueryProfile(JobRequest request) throws Exception {
    return JobsServiceTestUtils.getQueryProfile(getJobsService(), request);
  }

  protected static UserBitShared.QueryProfile getQueryProfile(JobSubmission submission, int attempt)
      throws Exception {
    return getJobsService()
        .getProfile(
            QueryProfileRequest.newBuilder()
                .setJobId(JobsProtoUtil.toBuf(submission.getJobId()))
                .setUserName(DEFAULT_USERNAME)
                .setAttempt(attempt)
                .build());
  }

  protected static void setSystemOption(final OptionValidator option, final String value) {
    setSystemOption(option.getOptionName(), value);
  }

  protected static void setSystemOption(String optionName, String optionValue) {
    JobsServiceTestUtils.setSystemOption(getJobsService(), optionName, optionValue);
  }

  protected static void resetSystemOption(String optionName) {
    JobsServiceTestUtils.resetSystemOption(getJobsService(), optionName);
  }

  protected static AutoCloseable withSystemOption(String optionName, String optionValue) {
    setSystemOption(optionName, optionValue);
    return () -> resetSystemOption(optionName);
  }

  /**
   * Update the system option to the given value with an AutoCloseable that will set it back to the
   * value we started with. Note that this does not reset it to the default.
   *
   * @param option The name of the system option
   * @param value The value we want to set the option to.
   * @return An AutoCloseable that will set the option back to what it was before we called this
   *     function.
   */
  protected AutoCloseable withSystemOptionAutoResetToInitial(
      final String option, final String value) {
    Setting setting =
        expectSuccess(
            getBuilder(getAPIv2().path("settings").path(option)).buildGet(), Setting.class);
    setSystemOption(option, value);
    return () -> setSystemOption(option, setting.getValue().toString());
  }

  protected static UserBitShared.QueryProfile getTestProfile() throws Exception {
    String query =
        "select sin(val_int_64) + 10 from cp"
            + ".\"parquet/decimals/mixedDecimalsInt32Int64FixedLengthWithStats.parquet\"";

    UserBitShared.QueryProfile queryProfile =
        getQueryProfile(
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery(query, DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .setDatasetVersion(DatasetVersion.NONE)
                .build());
    return queryProfile;
  }

  protected static String readResourceAsString(String fileName) {
    return TestTools.readTestResourceAsString(fileName);
  }

  protected WebTarget getUserApiV2(final String username) {
    return getAPIv2().path("user/" + username);
  }

  protected void downloadSupportRequest(String jobId) {
    SupportService supportService = l(SupportService.class);
    final ImmutableSupportRequest profileRequest =
        new ImmutableSupportRequest.Builder()
            .setUserId(SystemUser.SYSTEM_USERNAME)
            .setJobId(new JobId(jobId))
            .build();
    try {
      DatasetDownloadManager.DownloadDataResponse response =
          supportService.downloadSupportRequest(profileRequest);
      logger.info("Query profile for job {} saved to: {}", jobId, response.getFileName());
    } catch (UserNotFoundException | IOException | JobNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
