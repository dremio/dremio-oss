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

import static com.dremio.dac.server.JobsServiceTestUtils.submitJobAndGetData;
import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static java.lang.String.format;
import static java.util.Arrays.asList;
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
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.test.SampleDataPopulator;
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
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.EnumValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;
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
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.SecurityContext;
import org.apache.arrow.memory.BufferAllocator;
import org.assertj.core.api.SoftAssertions;
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
  private static DACHttpClient httpClient;
  private static DACHttpClient masterHttpClient;
  private static DremioClient dremioClient;
  private static DACDaemon executorDaemon;
  private static DACDaemon currentDremioDaemon;
  private static DACDaemon masterDremioDaemon;
  private static SampleDataPopulator populator;

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

  protected static DACHttpClient getHttpClient() {
    return Preconditions.checkNotNull(httpClient, "httpClient not initialized");
  }

  protected static DACHttpClient getMasterHttpClient() {
    return Preconditions.checkNotNull(masterHttpClient, "masterHttpClient not initialized");
  }

  protected void login() {
    login(DEFAULT_USERNAME, DEFAULT_PASSWORD);
  }

  protected void login(final String userName, final String password) {
    getHttpClient().login(userName, password, UserLoginSession.class);
  }

  protected Invocation.Builder getBuilder(WebTarget webTarget) {
    return getHttpClient().getBuilder(webTarget);
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
    httpClient = new DACHttpClient(client, currentDremioDaemon);
    masterHttpClient =
        masterDremioDaemon != null ? new DACHttpClient(client, masterDremioDaemon) : httpClient;
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
      client = null;
      httpClient = null;
      masterHttpClient = null;
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

  protected static SampleDataPopulator getPopulator() {
    return populator;
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

  public static void setSpace() throws NamespaceException, IOException {
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

  // Wait for the job complete
  protected void waitForJobComplete(String jobId) {
    int retry = 20;
    while (retry-- >= 0) {
      try {
        Thread.sleep(1000);
        JobUI job =
            expectSuccess(
                getBuilder(getHttpClient().getAPIv2().path("job").path(jobId))
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

  /**
   * @deprecated prefer {@link #withSystemOption(BooleanValidator, boolean)} instead]
   */
  @Deprecated
  protected static void setSystemOption(BooleanValidator option, boolean value) {
    setSystemOptionInternal(option, Boolean.toString(value));
  }

  /**
   * @deprecated prefer {@link #withSystemOption(LongValidator, long)} instead]
   */
  @Deprecated
  protected static void setSystemOption(LongValidator option, long value) {
    setSystemOptionInternal(option, Long.toString(value));
  }

  /**
   * @deprecated prefer {@link #withSystemOption(DoubleValidator, double)} instead]
   */
  @Deprecated
  protected static void setSystemOption(DoubleValidator option, double value) {
    setSystemOptionInternal(option, Double.toString(value));
  }

  /**
   * @deprecated prefer {@link #withSystemOption(StringValidator, String)} instead]
   */
  @Deprecated
  protected static void setSystemOption(StringValidator option, String value) {
    setSystemOptionInternal(option, "'" + value + "'");
  }

  /**
   * @deprecated prefer {@link #withSystemOption(EnumValidator, Enum)} instead]
   */
  @Deprecated
  protected static <T extends Enum<T>> void setSystemOption(EnumValidator<T> option, T value) {
    setSystemOptionInternal(option, "'" + value + "'");
  }

  private static void setSystemOptionInternal(OptionValidator option, String value) {
    if (value.equals(option.getDefault().getValue())) {
      logger.warn(
          "Test called setSystemOptionInternal with default value: {}", option.getOptionName());
    }
    JobsServiceTestUtils.setSystemOption(getJobsService(), option.getOptionName(), value);
  }

  protected static void resetSystemOption(OptionValidator option) {
    JobsServiceTestUtils.resetSystemOption(getJobsService(), option.getOptionName());
  }

  protected static AutoCloseable withSystemOption(BooleanValidator option, boolean value) {
    return withSystemOptionInternal(option, Boolean.toString(value));
  }

  protected static AutoCloseable withSystemOption(LongValidator option, long value) {
    return withSystemOptionInternal(option, Long.toString(value));
  }

  protected static AutoCloseable withSystemOption(DoubleValidator option, double value) {
    return withSystemOptionInternal(option, Double.toString(value));
  }

  protected static AutoCloseable withSystemOption(StringValidator option, String value) {
    return withSystemOptionInternal(option, "'" + value + "'");
  }

  protected static <T extends Enum<T>> AutoCloseable withSystemOption(
      EnumValidator<T> option, T value) {
    return withSystemOptionInternal(option, "'" + value + "'");
  }

  private static AutoCloseable withSystemOptionInternal(OptionValidator option, String value) {
    if (value.equals(option.getDefault().getValue())) {
      logger.warn(
          "Test called withSystemOptionInternal with default value: {}", option.getOptionName());
    }
    setSystemOptionInternal(option, value);
    return () -> resetSystemOption(option);
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
