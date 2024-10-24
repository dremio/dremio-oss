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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.util.TestTools;
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
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.file.FilePath;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.EnumValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import javax.inject.Provider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

@Tag("dremio-multi-node-tests")
public abstract class BaseTestServerJunit5 extends BaseClientUtils {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseTestServerJunit5.class);

  protected static final String DEFAULT_USERNAME = SampleDataPopulator.DEFAULT_USER_NAME;
  protected static final String DEFAULT_PASSWORD = SampleDataPopulator.PASSWORD;

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

  @BeforeEach
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
    Preconditions.checkState(BaseTestServerJunit5.currentDremioDaemon == null);
    BaseTestServerJunit5.currentDremioDaemon = Preconditions.checkNotNull(currentDremioDaemon);
  }

  protected static void closeCurrentDremioDaemon() throws Exception {
    Preconditions.checkNotNull(BaseTestServerJunit5.currentDremioDaemon);
    try {
      BaseTestServerJunit5.currentDremioDaemon.close();
    } finally {
      BaseTestServerJunit5.currentDremioDaemon = null;
    }
  }

  public static void setMasterDremioDaemon(DACDaemon masterDremioDaemon) {
    Preconditions.checkState(BaseTestServerJunit5.masterDremioDaemon == null);
    BaseTestServerJunit5.masterDremioDaemon = Preconditions.checkNotNull(masterDremioDaemon);
  }

  public static void setPopulator(SampleDataPopulator populator) throws Exception {
    if (BaseTestServerJunit5.populator != null) {
      BaseTestServerJunit5.populator.close();
    }
    BaseTestServerJunit5.populator = Preconditions.checkNotNull(populator);
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

  @TempDir static File folder0;
  @TempDir static File folder1;
  @TempDir static File folder2;
  @TempDir static File folder3;

  protected static void initClient() throws Exception {
    initClient(newClientObjectMapper());
  }

  private static void initClient(ObjectMapper mapper) throws Exception {
    BaseTestServer.addDummySecurityContextForDefaultUser(currentDremioDaemon);

    final Path path = new File(folder0.getAbsolutePath() + "/testplugins").toPath();
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
        masterDremioDaemon != null ? new DACHttpClient(client, masterDremioDaemon) : null;
  }

  /**
   * Do not call this method directly unless from an "overriden" static "init" method of a subclass
   */
  @BeforeAll
  public static void init() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServerJunit5.@BeforeAll")) {
      initializeCluster(new DACDaemonModule());
    }
  }

  protected static void initializeCluster(DACModule dacModule) throws Exception {
    initializeCluster(dacModule, o -> o, new HashMap<>());
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

    String rootFolder0 = folder0.getAbsolutePath();
    String rootFolder1 = folder1.getAbsolutePath();
    String rootFolder2 = folder2.getAbsolutePath();
    String rootFolder3 = folder3.getAbsolutePath();

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

      // Get a random port
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        socket.setReuseAddress(true);
        port = socket.getLocalPort();
      }
      DACConfig masterConfig =
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
              .clusterMode(ClusterMode.DISTRIBUTED);

      for (Map.Entry<String, Object> entry : moreConfigs.entrySet()) {
        masterConfig = masterConfig.with(entry.getKey(), entry.getValue());
      }
      // create master node.
      masterDremioDaemon =
          DACDaemon.newDremioDaemon(masterConfig, DremioTest.CLASSPATH_SCAN_RESULT, dacModule);
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

  protected static void closeExecutorDaemon() throws Exception {
    try {
      executorDaemon.close();
    } finally {
      executorDaemon = null;
    }
  }

  @AfterAll
  public static void serverClose() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServerJunit5.@AfterAll")) {

      if (currentDremioDaemon != null) {
        BaseTestServer.assertNoRunningQueries(currentDremioDaemon);
        BaseTestServer.assertAllocatorsAreClosed(currentDremioDaemon);
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

  protected static JobRequest createNewJobRequestFromSql(String sql) {
    return JobRequest.newBuilder().setSqlQuery(new SqlQuery(sql, DEFAULT_USERNAME)).build();
  }

  protected static JobRequest createJobRequestFromSqlAndSessionId(String sql, String sessionId) {
    return JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(sql, ImmutableList.of(), DEFAULT_USERNAME, null, sessionId))
        .build();
  }

  protected static void runQueryInSession(String sql, String sessionId) {
    String executedSesssionId =
        getJobSubmissionAfterJobCompletion(createJobRequestFromSqlAndSessionId(sql, sessionId))
            .getSessionId()
            .getId();
    assertThat(executedSesssionId).isEqualTo(sessionId);
  }

  protected static String runQueryAndGetSessionId(String sql) {
    return getJobSubmissionAfterJobCompletion(createNewJobRequestFromSql(sql))
        .getSessionId()
        .getId();
  }

  protected void runQueryCheckResults(
      JobsService jobsService,
      String sourcePluginName,
      List<String> tablePath,
      int rows,
      int columns,
      BufferAllocator allocator,
      String sessionId)
      throws JobNotFoundException {

    try (final JobDataFragment truncData =
        submitJobAndGetData(
            jobsService,
            JobRequest.newBuilder()
                .setSqlQuery(
                    new SqlQuery(
                        format(
                            "select * from %s",
                            String.format("%s.%s", sourcePluginName, String.join(".", tablePath))),
                        ImmutableList.of(),
                        DEFAULT_USERNAME,
                        null,
                        sessionId))
                .build(),
            0,
            rows + 1,
            allocator)) {
      assertEquals(rows, truncData.getReturnedRowCount());
      assertEquals(columns, truncData.getColumns().size());
    }
  }

  protected JobDataFragment runQueryAndGetResults(String sql, BufferAllocator allocator)
      throws JobNotFoundException {
    return submitJobAndGetData(
        l(JobsService.class),
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(sql, Collections.emptyList(), DEFAULT_USERNAME))
            .build(),
        0,
        500,
        allocator);
  }

  protected static JobId runQuery(String query) {
    return submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .build());
  }

  protected static JobId runQuery(String query, String sessionId) {
    final SqlQuery sqlQuery =
        (sessionId != null)
            ? new SqlQuery(query, Collections.emptyList(), DEFAULT_USERNAME, null, sessionId)
            : new SqlQuery(query, DEFAULT_USERNAME);
    return submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(sqlQuery)
            .setQueryType(QueryType.UI_RUN)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }

  protected static JobId runQuery(SqlQuery sqlQuery) {
    return submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(sqlQuery)
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .build());
  }
}
