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
import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;
import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static javax.ws.rs.client.Entity.entity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.glassfish.jersey.CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.common.AutoCloseables;
import com.dremio.common.SentinelSecure;
import com.dremio.common.exceptions.ExecutionSetupException;
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
import com.dremio.dac.explore.model.DataPOJO;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetSearchUIs;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DatasetUIWithHistory;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.explore.model.InitialDataPreviewResponse;
import com.dremio.dac.explore.model.InitialPendingTransformResponse;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.explore.model.ViewFieldTypeMixin;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.planner.physical.PlannerSettings;
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
import com.dremio.service.Binder;
import com.dremio.service.BindingProvider;
import com.dremio.service.conduit.server.ConduitServer;
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
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import javax.inject.Provider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import org.apache.arrow.memory.BufferAllocator;
import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public abstract class BaseTestServerJunit5 extends BaseClientUtils {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseTestServerJunit5.class);
  private static final String API_LOCATION = "apiv2";
  private static final String NESSIE_PROXY = "nessie-proxy";
  private static final String PUBLIC_API_LOCATION = "api";
  private static final String SCIM_V2_API_LOCATION = "scim/v2";
  protected static final String DEFAULT_USERNAME = SampleDataPopulator.DEFAULT_USER_NAME;
  protected static final String DEFAULT_PASSWORD = SampleDataPopulator.PASSWORD;

  private PrintWriter docLog;
  private UserLoginSession uls;

  public static final String USERNAME = SampleDataPopulator.TEST_USER_NAME;

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

  protected static boolean isComplexTypeSupport() {
    return PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal();
  }

  @BeforeEach
  public void resetDefaultUser() {
    if (defaultUser) {
      try {
        SampleDataPopulator.addDefaultFirstUser(l(UserService.class), newNamespaceService());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      login();
    }
  }

  protected static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;
  private static Client client;
  private static DremioClient dremioClient;
  private static WebTarget rootTarget;
  private static WebTarget metricsEndpoint;
  private static WebTarget apiV2;
  private static WebTarget nessieProxy;
  private static WebTarget masterApiV2;
  private static WebTarget publicAPI;
  private static WebTarget masterPublicAPI;
  private static WebTarget scimV2API;
  private static DACDaemon executorDaemon;
  private static DACDaemon currentDremioDaemon;
  private static DACDaemon masterDremioDaemon;
  private static Binder dremioBinder;
  private static SampleDataPopulator populator;
  private static boolean executorDaemonClosed = false;

  public static void setClient(Client client) {
    BaseTestServerJunit5.client = client;
  }

  public static void setCurrentDremioDaemon(DACDaemon currentDremioDaemon) {
    BaseTestServerJunit5.currentDremioDaemon = currentDremioDaemon;
  }

  public static void setMasterDremioDaemon(DACDaemon masterDremioDaemon) {
    BaseTestServerJunit5.masterDremioDaemon = masterDremioDaemon;
  }

  public static void setPopulator(SampleDataPopulator populator) {
    BaseTestServerJunit5.populator = populator;
  }

  protected static WebTarget getAPIv2() {
    return apiV2;
  }

  protected static WebTarget getNessieProxy() {
    return nessieProxy;
  }

  protected static WebTarget getScimAPIv2() {
    return scimV2API;
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

  @TempDir static File folder0;
  @TempDir static File folder1;
  @TempDir static File folder2;
  @TempDir static File folder3;

  private static ObjectMapper configureObjectMapper() {
    ObjectMapper objectMapper = JSONUtil.prettyMapper();
    JSONUtil.registerStorageTypes(
        objectMapper,
        DremioTest.CLASSPATH_SCAN_RESULT,
        ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG));
    objectMapper
        .registerModule(
            new SimpleModule()
                .addDeserializer(
                    JobDataFragment.class,
                    new JsonDeserializer<JobDataFragment>() {
                      @Override
                      public JobDataFragment deserialize(
                          JsonParser jsonParser, DeserializationContext deserializationContext)
                          throws IOException {
                        return jsonParser.readValueAs(DataPOJO.class);
                      }
                    }))
        .addMixIn(ViewFieldType.class, ViewFieldTypeMixin.class);
    objectMapper.setFilterProvider(
        new SimpleFilterProvider()
            .addFilter(SentinelSecure.FILTER_NAME, SentinelSecureFilter.TEST_ONLY));
    return objectMapper;
  }

  protected static void initClient() throws Exception {
    initClient(configureObjectMapper());
  }

  private static void initClient(ObjectMapper mapper) throws Exception {
    setBinder(createBinder(currentDremioDaemon.getBindingProvider()));

    final Path path = new File(folder0.getAbsolutePath() + "/testplugins").toPath();
    if (!Files.exists(path)) {
      TestUtilities.addDefaultTestPlugins(l(CatalogService.class), path.toString());
    }

    setPopulator(
        new SampleDataPopulator(
            l(SabotContext.class),
            newSourceService(),
            newDatasetVersionMutator(),
            l(UserService.class),
            newNamespaceService(),
            DEFAULT_USERNAME,
            l(CollaborationHelper.class)));

    final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(mapper);

    client =
        ClientBuilder.newBuilder()
            .property(FEATURE_AUTO_DISCOVERY_DISABLE, true)
            .register(provider)
            .register(MultiPartFeature.class)
            .build();
    rootTarget = client.target("http://localhost:" + currentDremioDaemon.getWebServer().getPort());
    final WebTarget livenessServiceTarget =
        client.target(
            "http://localhost:" + currentDremioDaemon.getLivenessService().getLivenessPort());
    metricsEndpoint = livenessServiceTarget.path("metrics");
    apiV2 = rootTarget.path(API_LOCATION);
    nessieProxy = rootTarget.path(NESSIE_PROXY);
    publicAPI = rootTarget.path(PUBLIC_API_LOCATION);
    scimV2API = rootTarget.path(SCIM_V2_API_LOCATION);
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

  private static void startCurrentDaemon() throws Exception {
    currentDremioDaemon.init();
  }

  @BeforeAll
  public static void init() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServerJunit5.@BeforeAll")) {
      initializeCluster(new DACDaemonModule());
    }
  }

  protected static void initializeCluster(DACModule dacModule) throws Exception {
    initializeCluster(dacModule, o -> o);
  }

  protected static void initializeCluster(
      DACModule dacModule, Function<ObjectMapper, ObjectMapper> mapperUpdate) throws Exception {
    final String hostname = InetAddress.getLocalHost().getCanonicalHostName();
    Provider<Integer> jobsPortProvider =
        () -> currentDremioDaemon.getBindingProvider().lookup(ConduitServer.class).getPort();

    // Turning on flag for NaaS
    System.setProperty("nessie.source.resource.testing.enabled", "true");

    if (isMultinode()) {
      logger.info("Running tests in multinode mode");

      // run all tests on remote coordinator node relying on additional remote executor node

      // we'll share the same file:/// writepath for all nodes. Pdfs causes
      // problems as it uses static variables so resolution doesn't work in a
      // single ClassLoader
      final String distpath = "file://" + folder0.getAbsolutePath();
      // jobsResultsStore stored in <distpath>/results and accelerator store in
      // <distpath>/accelerator, both of which
      // need to exist
      Files.createDirectories(new File(folder0.getAbsolutePath() + "/results").toPath());
      Files.createDirectories(new File(folder0.getAbsolutePath() + "/accelerator").toPath());
      Files.createDirectories(new File(folder0.getAbsolutePath() + "/scratch").toPath());
      Files.createDirectories(new File(folder0.getAbsolutePath() + "/metadata").toPath());
      Files.createDirectories(new File(folder0.getAbsolutePath() + "/gandiva").toPath());

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
                  .writePath(folder1.getAbsolutePath())
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

      // remote coordinator node
      int zkPort = masterDremioDaemon.getBindingProvider().lookup(ZkServer.class).getPort();
      currentDremioDaemon =
          DACDaemon.newDremioDaemon(
              DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
                  .isMaster(false)
                  .autoPort(true)
                  .allowTestApis(testApiEnabled)
                  .serveUI(false)
                  .inMemoryStorage(inMemoryStorage)
                  .jobServerEnabled(true)
                  .writePath(folder2.getAbsolutePath())
                  .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
                  .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
                  .clusterMode(ClusterMode.DISTRIBUTED)
                  .localPort(
                      masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort()
                          + 1)
                  .isRemote(true)
                  .with(DremioConfig.ENABLE_EXECUTOR_BOOL, false)
                  .with(DremioConfig.NESSIE_SERVICE_ENABLED_BOOLEAN, true)
                  .with(DremioConfig.NESSIE_SERVICE_IN_MEMORY_BOOLEAN, true)
                  .zk("localhost:" + zkPort),
              DremioTest.CLASSPATH_SCAN_RESULT,
              dacModule);
      startCurrentDaemon();

      // remote executor node
      executorDaemon =
          DACDaemon.newDremioDaemon(
              DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
                  .autoPort(true)
                  .allowTestApis(testApiEnabled)
                  .serveUI(false)
                  .inMemoryStorage(inMemoryStorage)
                  .with(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
                  .writePath(folder3.getAbsolutePath())
                  .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
                  .clusterMode(ClusterMode.DISTRIBUTED)
                  .localPort(
                      masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort()
                          + 1)
                  .isRemote(true)
                  .zk("localhost:" + zkPort),
              DremioTest.CLASSPATH_SCAN_RESULT,
              dacModule);
      executorDaemonClosed = false;
      executorDaemon.init();
    } else {
      logger.info("Running tests in local mode");
      final String distpath = "file://" + folder0.getAbsolutePath();
      currentDremioDaemon =
          DACDaemon.newDremioDaemon(
              DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
                  .autoPort(true)
                  .allowTestApis(testApiEnabled)
                  .serveUI(false)
                  .addDefaultUser(addDefaultUser)
                  .inMemoryStorage(inMemoryStorage)
                  .writePath(folder1.getAbsolutePath())
                  .with(DremioConfig.METADATA_PATH_STRING, distpath + "/metadata")
                  .with(DremioConfig.ACCELERATOR_PATH_STRING, distpath + "/accelerator")
                  .with(DremioConfig.GANDIVA_CACHE_PATH_STRING, distpath + "/gandiva")
                  .with(
                      DremioConfig.SYSTEM_ICEBERG_TABLES_PATH_STRING,
                      distpath + "/system_iceberg_tables")
                  .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
                  .with(DremioConfig.NESSIE_SERVICE_ENABLED_BOOLEAN, true)
                  .with(DremioConfig.NESSIE_SERVICE_IN_MEMORY_BOOLEAN, true)
                  .clusterMode(DACDaemon.ClusterMode.LOCAL),
              DremioTest.CLASSPATH_SCAN_RESULT,
              dacModule);
      masterDremioDaemon = null;
      startCurrentDaemon();
    }

    initClient(mapperUpdate.apply(configureObjectMapper()));
  }

  protected static NamespaceService newNamespaceService() {
    return l(NamespaceService.class);
  }

  protected static CatalogService newCatalogService() {
    return l(CatalogService.class);
  }

  protected static SourceService newSourceService() {
    return l(SourceService.class);
  }

  protected static ReflectionServiceHelper newReflectionServiceHelper() {
    return l(ReflectionServiceHelper.class);
  }

  protected static DatasetVersionMutator newDatasetVersionMutator() {
    return l(DatasetVersionMutator.class);
  }

  protected static SabotContext getSabotContext() {
    return l(SabotContext.class);
  }

  protected static <T> T l(Class<T> clazz) {
    return dremioBinder.lookup(clazz);
  }

  protected static <T> Provider<T> p(Class<T> clazz) {
    return dremioBinder.provider(clazz);
  }

  protected static <T> Provider<T> pMaster(Class<T> clazz) {
    if (masterDremioDaemon != null) {
      return masterDremioDaemon.getBindingProvider().provider(clazz);
    }

    return p(clazz);
  }

  protected void deleteSource(String name) {
    ((CatalogServiceImpl) l(CatalogService.class)).deleteSource(name);
  }

  protected static void setBinder(Binder binder) {
    dremioBinder = binder;
  }

  public static Binder createBinder(BindingProvider dremioBindingProvider) {
    Binder dremioBinder = dremioBindingProvider.newChild();
    dremioBinder.bind(
        SecurityContext.class,
        new SecurityContext() {
          @Override
          public Principal getUserPrincipal() {
            return new Principal() {
              @Override
              public String getName() {
                return DEFAULT_USERNAME;
              }
            };
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
        });
    SabotContext context = dremioBinder.lookup(SabotContext.class);
    dremioBinder.bind(OptionManager.class, context.getOptionManager());
    return dremioBinder;
  }

  protected static void closeExecutorDaemon() throws Exception {
    if (!executorDaemonClosed) {
      executorDaemon.close();
      executorDaemonClosed = true;
    }
  }

  private static int getResourceAllocatorCount() {
    return l(BufferAllocatorFactory.class).getBaseAllocator().getChildAllocators().size();
  }

  private static int getQueryPlanningAllocatorCount() {
    final BufferAllocator queryPlanningAllocator =
        l(SabotContext.class).getQueryPlanningAllocator();
    if (queryPlanningAllocator == null) {
      return 0;
    }
    return queryPlanningAllocator.getChildAllocators().size();
  }

  private static void assertAllocatorsAreClosed() {
    int resourceAllocatorCount = getResourceAllocatorCount();
    int queryPlanningAllocatorCount = getQueryPlanningAllocatorCount();
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

  @AfterAll
  public static void serverClose() throws Exception {
    if (dremioBinder == null) {
      return;
    }
    try (TimedBlock b = Timer.time("BaseTestServerJunit5.@AfterAll")) {
      // Prevent new incoming job requests
      final NodeRegistration nodeRegistration = l(NodeRegistration.class);
      final ForemenWorkManager foremenWorkManager = l(ForemenWorkManager.class);
      nodeRegistration.close();
      foremenWorkManager.close();

      // Drain actively running jobs
      await()
          .atMost(Duration.ofSeconds(50))
          .until(() -> foremenWorkManager.getActiveQueryCount() == 0);

      // Fail if any jobs are still running and record query information
      Assertions.assertEquals(
          0,
          foremenWorkManager.getActiveQueryCount(),
          () -> {
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
            return msg.toString();
          });

      assertAllocatorsAreClosed();

      defaultUser =
          true; // in case another test disables the default user and forgets to enable it back
      // again at the end
      AutoCloseables.close(
          new AutoCloseable() {
            @Override
            public void close() throws Exception {
              if (dremioClient != null) {
                // since the client is only created when needed, make sure we don't re-close an old
                // client.
                DremioClient localClient = dremioClient;
                dremioClient = null;
                localClient.close();
              }
            }
          },
          new AutoCloseable() {
            @Override
            public void close() throws Exception {
              if (client != null) {
                client.close();
              }
            }
          },
          (executorDaemonClosed ? null : executorDaemon),
          currentDremioDaemon,
          masterDremioDaemon,
          populator);
      executorDaemonClosed = true;
      dremioClient = null;
    }
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

  public static void assertContains(String expectedContains, String string) {
    assertThat(string).contains(expectedContains);
  }

  public static void assertNotContains(String expectedNotContains, String string) {
    assertThat(string).doesNotContain(expectedNotContains);
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
    @SuppressWarnings("resource")
    DACDaemon daemon = isMultinode() ? getMasterDremioDaemon() : getCurrentDremioDaemon();
    TestUtilities.clear(
        daemon.getBindingProvider().lookup(CatalogService.class),
        daemon.getBindingProvider().lookup(LegacyKVStoreProvider.class),
        ImmutableList.of(SimpleUserService.USER_STORE),
        ImmutableList.of("cp"));
    if (isMultinode()) {
      ((CatalogServiceImpl)
              getCurrentDremioDaemon().getBindingProvider().lookup(CatalogService.class))
          .synchronizeSources();
    }
  }

  protected void setSpace() throws NamespaceException, IOException {
    clearAllDataExceptUser();
    final SpaceConfig foo = new SpaceConfig().setName("spacefoo");
    SpacePath spacePath = new SpacePath(new SpaceName(foo.getName()));
    newNamespaceService().addOrUpdateSpace(spacePath.toNamespaceKey(), foo);
    newNamespaceService()
        .addOrUpdateFolder(
            new FolderPath("spacefoo.folderbar").toNamespaceKey(),
            new FolderConfig()
                .setName("folderbar")
                .setFullPathList(asList("spacefoo", "folderbar")));
    newNamespaceService()
        .addOrUpdateFolder(
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

  protected UserExceptionMapper.ErrorMessageWithContext saveAsExpectError(
      DatasetUI datasetUI, DatasetPath newName) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("as", newName))
            .buildPost(entity("", JSON));

    return expectError(CLIENT_ERROR, invocation, UserExceptionMapper.ErrorMessageWithContext.class);
  }

  protected DatasetUIWithHistory saveAsInBranch(
      DatasetUI datasetUI, DatasetPath newName, String savedTag, String branchName) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("as", newName)
                    .queryParam("branchName", branchName)
                    .queryParam("savedTag", savedTag))
            .buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  protected DatasetUIWithHistory saveInBranch(
      DatasetUI datasetUI, String saveVersion, String branchName) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("savedTag", saveVersion)
                    .queryParam("branchName", branchName))
            .buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  protected UserExceptionMapper.ErrorMessageWithContext saveAsInBranchExpectError(
      DatasetUI datasetUI, DatasetPath newName, String savedTag, String branchName) {
    final Invocation invocation =
        getBuilder(
                getAPIv2()
                    .path(versionedResourcePath(datasetUI) + "/save")
                    .queryParam("as", newName)
                    .queryParam("branchName", branchName)
                    .queryParam("savedTag", savedTag))
            .buildPost(entity("", JSON));

    return expectError(CLIENT_ERROR, invocation, UserExceptionMapper.ErrorMessageWithContext.class);
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

  protected UserExceptionMapper.ErrorMessageWithContext createDatasetFromSQLAndSaveExpectError(
      DatasetPath datasetPath, String sql, List<String> context) {
    InitialPreviewResponse datasetCreateResponse = createDatasetFromSQL(sql, context);
    return saveAsExpectError(datasetCreateResponse.getDataset(), datasetPath);
  }

  protected DatasetUI createVersionedDatasetFromSQLAndSave(
      DatasetPath datasetPath,
      String sql,
      List<String> context,
      String pluginName,
      String branchName) {
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        pluginName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));
    InitialPreviewResponse datasetCreateResponse =
        createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    return saveAsInBranch(datasetCreateResponse.getDataset(), datasetPath, null, branchName)
        .getDataset();
  }

  protected InitialPreviewResponse createDatasetFromSQL(String sql, List<String> context) {
    return expectSuccess(
        getBuilder(
                getAPIv2().path("datasets/new_untitled_sql").queryParam("newVersion", newVersion()))
            .buildPost(entity(new CreateFromSQL(sql, context), JSON)), // => sending
        InitialPreviewResponse.class); // <= receiving
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

  protected InitialPreviewResponse createVersionedDatasetFromSQL(
      String sql, List<String> context, String pluginName, String branchName) {
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        pluginName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));

    return expectSuccess(
        getBuilder(
                getAPIv2().path("datasets/new_untitled_sql").queryParam("newVersion", newVersion()))
            .buildPost(entity(new CreateFromSQL(sql, context, references), JSON)), // => sending
        InitialPreviewResponse.class); // <= receiving
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

  protected void assertErrorMessage(final GenericErrorMessage error, final String errorMessage) {
    assertEquals(
        "error message should be '" + errorMessage + "'", errorMessage, error.getErrorMessage());
  }

  protected void assertErrorMessage(
      final GenericErrorMessage error, final String errorMessage, final String expectedMoreInfo) {
    assertEquals(
        "error message should be '" + errorMessage + "'", errorMessage, error.getErrorMessage());
    assertThat(error.getMoreInfo()).contains(expectedMoreInfo);
  }

  protected JobId submitAndWaitUntilSubmitted(JobRequest request, JobStatusListener listener) {
    return JobsServiceTestUtils.submitAndWaitUntilSubmitted(
        l(JobsService.class), request, listener);
  }

  protected JobId submitAndWaitUntilSubmitted(JobRequest request) {
    return JobsServiceTestUtils.submitAndWaitUntilSubmitted(l(JobsService.class), request);
  }

  protected JobId submitJobAndWaitUntilCompletion(JobRequest request, JobStatusListener listener) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(
            l(JobsService.class), request, listener)
        .getJobId();
  }

  protected boolean submitJobAndCancelOnTimeOut(JobRequest request, long timeOutInMillis)
      throws Exception {
    return JobsServiceTestUtils.submitJobAndCancelOnTimeout(
        l(JobsService.class), request, timeOutInMillis);
  }

  protected static JobId submitJobAndWaitUntilCompletion(JobRequest request) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(l(JobsService.class), request);
  }

  protected static JobSubmission getJobSubmissionAfterJobCompletion(JobRequest request) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(
        l(JobsService.class), request, JobStatusListener.NO_OP);
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
    return JobsServiceTestUtils.getQueryProfile(l(JobsService.class), request);
  }

  protected static void setSystemOption(final OptionValidator option, final String value) {
    setSystemOption(option.getOptionName(), value);
  }

  protected static void setSystemOption(String optionName, String optionValue) {
    JobsServiceTestUtils.setSystemOption(l(JobsService.class), optionName, optionValue);
  }

  protected static void resetSystemOption(String optionName) {
    JobsServiceTestUtils.resetSystemOption(l(JobsService.class), optionName);
  }

  protected static AutoCloseable enableRowCountStat(boolean enableStatAfterTest) {
    setSystemOption(PlannerSettings.USE_ROW_COUNT_STATISTICS.getOptionName(), "true");
    setSystemOption(PlannerSettings.USE_STATISTICS.getOptionName(), "false");
    return () -> {
      setSystemOption(PlannerSettings.USE_ROW_COUNT_STATISTICS.getOptionName(), "false");
      setSystemOption(
          PlannerSettings.USE_STATISTICS.getOptionName(), String.valueOf(enableStatAfterTest));
    };
  }

  protected static AutoCloseable withSystemOption(String optionName, String optionValue) {
    setSystemOption(optionName, optionValue);
    return () -> resetSystemOption(optionName);
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

  protected static AutoCloseable enableVersionedViews() {
    setSystemOption(VERSIONED_VIEW_ENABLED.getOptionName(), "true");
    return () -> {
      setSystemOption(
          VERSIONED_VIEW_ENABLED.getOptionName(),
          VERSIONED_VIEW_ENABLED.getDefault().getBoolVal().toString());
    };
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
    AssertionsForClassTypes.assertThat(executedSesssionId).isEqualTo(sessionId);
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
