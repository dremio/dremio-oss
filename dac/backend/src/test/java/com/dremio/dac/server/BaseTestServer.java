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
import static org.glassfish.jersey.CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.security.Principal;
import java.time.Duration;
import java.util.List;
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
import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.dremio.common.AutoCloseables;
import com.dremio.common.SentinelSecure;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
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
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.file.FilePath;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.Binder;
import com.dremio.service.BindingProvider;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.ReflectionAdministrationService;
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

/**
 * base class for server tests
 */
public abstract class BaseTestServer extends BaseClientUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestServer.class);

  private static final String API_LOCATION = "apiv2";
  private static final String PUBLIC_API_LOCATION = "api";
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

  @Rule public TestWatcher watcher = new TestWatcher() {
    private TimedBlock b;
    @Override
    protected void starting(Description description) {
      b = Timer.time(desc(description));
      logger.info(format("Test starting: %s", desc(description)));
      AccessLogFilter accessLogFilter = currentDremioDaemon.getWebServer().getAccessLogFilter();
      if (accessLogFilter != null) {

        // sanitize method name from test since it could be a parameterized test with random characters (or be really long).
        String methodName = description.getMethodName();
        methodName = methodName.replaceAll("\\W+", "");
        if(methodName.length() > 200){
          methodName = methodName.substring(0,  200);
        }
        File logFile = new File(format("target/test-access_logs/%s-%s.log", description.getClassName(), methodName));
        File parent = logFile.getParentFile();
        if ((parent.exists() && !parent.isDirectory()) || (!parent.exists() && !parent.mkdirs())) {
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
        docLog.close();
      }
    }

    private String desc(Description description) {
      return description.getClassName() + "." +  description.getMethodName();
    }
  };

  @Before
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
  private static WebTarget masterApiV2;
  private static WebTarget publicAPI;
  private static WebTarget masterPublicAPI;
  private static DACDaemon executorDaemon;
  private static DACDaemon currentDremioDaemon;
  private static DACDaemon masterDremioDaemon;
  private static Binder dremioBinder;
  private static SampleDataPopulator populator;
  private static boolean executorDaemonClosed = false;

  public static void setClient(Client client) {
    BaseTestServer.client = client;
  }

  public static void setCurrentDremioDaemon(DACDaemon currentDremioDaemon) {
    BaseTestServer.currentDremioDaemon = currentDremioDaemon;
  }

  public static void setMasterDremioDaemon(DACDaemon masterDremioDaemon) {
    BaseTestServer.masterDremioDaemon = masterDremioDaemon;
  }

  public static void setPopulator(SampleDataPopulator populator) {
    BaseTestServer.populator = populator;
  }

  protected static WebTarget getAPIv2() {
    return apiV2;
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
    if(dremioClient == null){
      dremioClient = new DremioClient(true);
      dremioClient.connect(new Properties(){{
        put("direct", "localhost:" + l(UserServer.class).getPort());
        put("user", "dremio");
        put("password", "dremio123");
      }});
    }
    return dremioClient;
  }

  @ClassRule
  public static final TemporaryFolder folder0 = new TemporaryFolder();

  @ClassRule
  public static final TemporaryFolder folder1 = new TemporaryFolder();

  @ClassRule
  public static final TemporaryFolder folder2 = new TemporaryFolder();

  @ClassRule
  public static final TemporaryFolder folder3 = new TemporaryFolder();

  private static ObjectMapper configureObjectMapper() {
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
    objectMapper.setFilterProvider(new SimpleFilterProvider().addFilter(SentinelSecure.FILTER_NAME, SentinelSecureFilter.TEST_ONLY));
    return objectMapper;
  }

  protected static void initClient() throws Exception {
    initClient(configureObjectMapper());
  }

  private static void initClient(ObjectMapper mapper) throws Exception {
    setBinder(createBinder(currentDremioDaemon.getBindingProvider()));

    if (!Files.exists(new File(folder0.getRoot().getAbsolutePath() + "/testplugins").toPath())) {
      TestUtilities.addDefaultTestPlugins(l(CatalogService.class), folder0.newFolder("testplugins").toString());
    }

    setPopulator(new SampleDataPopulator(
      l(SabotContext.class),
      newSourceService(),
      newDatasetVersionMutator(),
      l(UserService.class),
      newNamespaceService(),
      DEFAULT_USERNAME
    ));

    final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(mapper);

    client = ClientBuilder.newBuilder()
      .property(FEATURE_AUTO_DISCOVERY_DISABLE, true)
      .register(provider)
      .register(MultiPartFeature.class)
      .build();
    rootTarget = client.target("http://localhost:" + currentDremioDaemon.getWebServer().getPort());
    final WebTarget livenessServiceTarget = client.target("http://localhost:" + currentDremioDaemon.getLivenessService().getLivenessPort());
    metricsEndpoint = livenessServiceTarget.path("metrics");
    apiV2 = rootTarget.path(API_LOCATION);
    publicAPI = rootTarget.path(PUBLIC_API_LOCATION);
    if (isMultinode()) {
      masterApiV2 = client.target("http://localhost:" + masterDremioDaemon.getWebServer().getPort()).path(API_LOCATION);
      masterPublicAPI = client.target("http://localhost:" + masterDremioDaemon.getWebServer().getPort()).path(PUBLIC_API_LOCATION);
    } else {
      masterApiV2 = apiV2;
      masterPublicAPI = publicAPI;
    }
  }

  private static void startCurrentDaemon() throws Exception {
    currentDremioDaemon.init();
  }

  @BeforeClass
  public static void init() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      init(isMultinode());
    }
  }

  public static void init(boolean multiMode) throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      initializeCluster(multiMode, new DACDaemonModule());
    }
  }

  protected static void initializeCluster(final boolean isMultiNode, DACModule dacModule) throws Exception {
    initializeCluster(isMultiNode, dacModule, o -> o);
  }

  protected static void initializeCluster(final boolean isMultiNode, DACModule dacModule, Function<ObjectMapper, ObjectMapper> mapperUpdate) throws Exception {
    final String hostname = InetAddress.getLocalHost().getCanonicalHostName();
    Provider<Integer> jobsPortProvider = () -> currentDremioDaemon.getBindingProvider().lookup(ConduitServer.class).getPort();
    if (isMultiNode) {
      logger.info("Running tests in multinode mode");

      // run all tests on remote coordinator node relying on additional remote executor node

      // we'll share the same file:/// writepath for all nodes. Pdfs causes
      // problems as it uses static variables so resolution doesn't work in a
      // single ClassLoader
      final String distpath = "file://" + folder0.getRoot().getAbsolutePath();
      // jobsResultsStore stored in <distpath>/results and accelerator store in <distpath>/accelerator, both of which
      // need to exist
      Files.createDirectories(new File(folder0.getRoot().getAbsolutePath() + "/results").toPath());
      Files.createDirectories(new File(folder0.getRoot().getAbsolutePath() + "/accelerator").toPath());
      Files.createDirectories(new File(folder0.getRoot().getAbsolutePath() + "/scratch").toPath());

      // Get a random port
      int port;
      try(ServerSocket socket = new ServerSocket(0)) {
        socket.setReuseAddress(true);
        port = socket.getLocalPort();
      }
      // create master node.
      masterDremioDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .autoPort(true)
              .allowTestApis(testApiEnabled)
              .serveUI(false)
              .jobServerEnabled(true)
              .inMemoryStorage(inMemoryStorage)
              .writePath(folder1.getRoot().getAbsolutePath())
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .with(DremioConfig.ENABLE_EXECUTOR_BOOL, false)
              .with(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_PORT_INT, port)
              .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
              .clusterMode(ClusterMode.DISTRIBUTED),
          DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule);
      masterDremioDaemon.init();

      // remote coordinator node
      int zkPort = masterDremioDaemon.getBindingProvider().lookup(ZkServer.class).getPort();
      currentDremioDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .isMaster(false)
              .autoPort(true)
              .allowTestApis(testApiEnabled)
              .serveUI(false)
              .inMemoryStorage(inMemoryStorage)
              .jobServerEnabled(true)
              .writePath(folder2.getRoot().getAbsolutePath())
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
              .clusterMode(ClusterMode.DISTRIBUTED)
              .localPort(masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort() + 1)
              .isRemote(true)
              .with(DremioConfig.ENABLE_EXECUTOR_BOOL, false)
              .zk("localhost:" + zkPort),
              DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule);
      startCurrentDaemon();

      // remote executor node
      executorDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .autoPort(true)
              .allowTestApis(testApiEnabled)
              .serveUI(false)
              .inMemoryStorage(inMemoryStorage)
              .with(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
              .writePath(folder3.getRoot().getAbsolutePath())
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .clusterMode(ClusterMode.DISTRIBUTED)
              .localPort(masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort() + 1)
              .isRemote(true)
              .zk("localhost:" + zkPort),
              DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule
          );
      executorDaemonClosed = false;
      executorDaemon.init();
    } else {
      logger.info("Running tests in local mode");
      currentDremioDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .autoPort(true)
              .allowTestApis(testApiEnabled)
              .serveUI(false)
              .addDefaultUser(addDefaultUser)
              .inMemoryStorage(inMemoryStorage)
              .writePath(folder1.getRoot().getAbsolutePath())
              .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
              .clusterMode(DACDaemon.ClusterMode.LOCAL),
              DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule
          );
      masterDremioDaemon = null;
      startCurrentDaemon();
    }

    initClient(mapperUpdate.apply(configureObjectMapper()));
  }

  protected static NamespaceService newNamespaceService(){
    return l(NamespaceService.class);
  }

  protected static CatalogService newCatalogService() {
    return l(CatalogService.class);
  }

  protected static SourceService newSourceService(){
    return l(SourceService.class);
  }

  protected static ReflectionServiceHelper newReflectionServiceHelper(){
    return l(ReflectionServiceHelper.class);
  }

  protected static DatasetVersionMutator newDatasetVersionMutator(){
    return l(DatasetVersionMutator.class);
  }

  protected static SabotContext getSabotContext(){
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
    ((CatalogServiceImpl)l(CatalogService.class)).deleteSource(name);
  }

  protected static void setBinder(Binder binder) {
    dremioBinder = binder;
  }

  public static Binder createBinder(BindingProvider dremioBindingProvider) {
    Binder dremioBinder = dremioBindingProvider.newChild();
    dremioBinder.bind(SecurityContext.class, new SecurityContext() {
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

    dremioBinder.bindProvider(ReflectionAdministrationService.class, () -> {
      ReflectionAdministrationService.Factory factory = dremioBindingProvider.lookup(ReflectionAdministrationService.Factory.class);
      return factory.get(new ReflectionContext(DEFAULT_USER_NAME, true));
    });

    return dremioBinder;
  }

  protected static void closeExecutorDaemon() throws Exception {
    if (!executorDaemonClosed) {
      executorDaemon.close();
      executorDaemonClosed = true;
    }
  }

  private static int getResourceAllocatorCount() {
    return l(BufferAllocatorFactory.class)
      .getBaseAllocator()
      .getChildAllocators()
      .size();
  }

  private static int getQueryPlanningAllocatorCount() {
    return l(SabotContext.class)
      .getQueryPlanningAllocator()
      .getChildAllocators()
      .size();
  }

  @AfterClass
  public static void close() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServer.@AfterClass")) {

      await().atMost(Duration.ofSeconds(50))
        .untilAsserted(() -> assertEquals("Not all the resource/query planning allocators were closed.",
          0, getResourceAllocatorCount() + getQueryPlanningAllocatorCount()));

      defaultUser = true; // in case another test disables the default user and forgets to enable it back again at the end
      AutoCloseables.close(
          new AutoCloseable(){
            @Override
            public void close() throws Exception {
              if (dremioClient != null) {
                // since the client is only created when needed, make sure we don't re-close an old client.
                DremioClient localClient = dremioClient;
                dremioClient = null;
                localClient.close();
              }
            }
          },
          new AutoCloseable(){
            @Override
            public void close() throws Exception {
              if (client != null) {
                client.close();
              }
            }
          },
        (executorDaemonClosed ? null : executorDaemon), currentDremioDaemon, masterDremioDaemon, populator);
      executorDaemonClosed = true;
      dremioClient = null;
    }
  }

  protected void login() {
    login(DEFAULT_USERNAME, DEFAULT_PASSWORD);
  }

  protected void login(final String userName, final String password) {
    UserLogin userLogin = new UserLogin(userName, password);
    this.setUls(expectSuccess(getAPIv2().path("/login").request(JSON).buildPost(Entity.json(userLogin)), UserLoginSession.class));
  }

  protected Invocation.Builder getBuilder(WebTarget webTarget) {
    return webTarget.request(JSON).header(getAuthHeaderName(), getAuthHeaderValue());
  }

  protected static SampleDataPopulator getPopulator(){
    return populator;
  }

  protected String getAuthHeaderName(){
    return HttpHeader.AUTHORIZATION.toString();
  }

  protected String getAuthHeaderValue(){
    return "_dremio" + getUls().getToken();
  }


  protected Invocation.Builder getBuilder(String apiUrl){
    return client.target("http://localhost:" + currentDremioDaemon.getWebServer().getPort() + "/" + API_LOCATION + apiUrl)
        .request(JSON).header(getAuthHeaderName(), getAuthHeaderValue());
  }

  protected Invocation.Builder getBuilder(WebTarget webTarget, String authorizationToken) {
    return webTarget.request(JSON).header(getAuthHeaderName(), "_dremio" + authorizationToken);
  }

  public static void assertContains(String expectedContains, String string) {
    assertTrue(string + " should contain " + expectedContains, string.contains(expectedContains));
  }

  public static void assertNotContains(String expectedNotContains, String string) {
    assertFalse(string + " should not contain " + expectedNotContains, string.contains(expectedNotContains));
  }

  protected UserLoginSession getUls() {
    return uls;
  }

  protected void setUls(UserLoginSession uls) {
    this.uls = uls;
  }

  protected static void populateInitialData() throws DatasetNotFoundException, DatasetVersionNotFoundException, ExecutionSetupException, NamespaceException, IOException{
    populator.populateInitialData();
  }

  public static void clearAllDataExceptUser() throws IOException, NamespaceException {
    @SuppressWarnings("resource")
    DACDaemon daemon = isMultinode() ? getMasterDremioDaemon() : getCurrentDremioDaemon();
    TestUtilities.clear(daemon.getBindingProvider().lookup(CatalogService.class), daemon.getBindingProvider().lookup(LegacyKVStoreProvider.class),
      ImmutableList.of(SimpleUserService.USER_STORE), ImmutableList.of("cp"));
    if(isMultinode()) {
      ((CatalogServiceImpl)getCurrentDremioDaemon().getBindingProvider().lookup(CatalogService.class)).synchronizeSources();
    }
  }

  protected void setSpace() throws NamespaceException, IOException {
    clearAllDataExceptUser();
    final SpaceConfig foo = new SpaceConfig().setName("spacefoo");
    SpacePath spacePath = new SpacePath(new SpaceName(foo.getName()));
    newNamespaceService().addOrUpdateSpace(spacePath.toNamespaceKey(), foo);
    newNamespaceService().addOrUpdateFolder(new FolderPath("spacefoo.folderbar").toNamespaceKey(), new FolderConfig().setName("folderbar").setFullPathList(asList("spacefoo", "folderbar")));
    newNamespaceService().addOrUpdateFolder(new FolderPath("spacefoo.folderbar.folderbaz").toNamespaceKey(), new FolderConfig().setName("folderbaz").setFullPathList(asList("spacefoo", "folderbar", "folderbaz")));
  }

  protected DatasetPath getDatasetPath(DatasetUI datasetUI) {
    return new DatasetPath(datasetUI.getFullPath());
  }

  protected DatasetVersionResourcePath getDatasetVersionPath(DatasetUI datasetUI) {
    return new DatasetVersionResourcePath(getDatasetPath(datasetUI), datasetUI.getDatasetVersion());
  }

  protected String versionedResourcePath(DatasetUI datasetUI) {
    return
        getPathJoiner()
            .join(
                "/dataset",
                getKeyJoiner().join(datasetUI.getFullPath()),
                "version",
                datasetUI.getDatasetVersion()
            );
  }

  protected String resourcePath(DatasetUI datasetUI) {
    return
        getPathJoiner()
            .join(
                "/dataset",
                getKeyJoiner().join(datasetUI.getFullPath())
            );
  }

  protected String getName(DatasetUI datasetUI) {
    return datasetUI.getFullPath().get(datasetUI.getFullPath().size() - 1);
  }

  protected String getRoot(DatasetUI datasetUI) {
    return datasetUI.getFullPath().get(0);
  }

  protected Invocation getDatasetInvocation(DatasetPath datasetPath) {
    return
        getBuilder(
            getAPIv2()
                .path("dataset/" + datasetPath.toString())
        ).buildGet();
  }

  protected DatasetUI getDataset(DatasetPath datasetPath) {
    return expectSuccess(getDatasetInvocation(datasetPath), DatasetUI.class);
  }

  protected DatasetUI getVersionedDataset(DatasetVersionResourcePath datasetVersionPath) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(getPathJoiner().join(datasetVersionPath.toString(), "preview"))
        ).buildGet();

    return expectSuccess(invocation, InitialPreviewResponse.class).getDataset();
  }

  protected InitialPreviewResponse getPreview(DatasetUI datasetUI) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(getPathJoiner().join(versionedResourcePath(datasetUI), "preview"))
        ).buildGet();

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  /**
   * Get the preview response for given dataset path. Dataset can be physical or virutal.
   * @param datasetPath
   * @return
   */
  protected InitialDataPreviewResponse getPreview(DatasetPath datasetPath) {
    final Invocation invocation =
        getBuilder(getAPIv2().path("dataset/" + datasetPath.toPathString() + "/preview")).buildGet();

    return expectSuccess(invocation, InitialDataPreviewResponse.class);
  }

  protected JobDataFragment getData(String paginationUrl, long offset, long limit) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(paginationUrl)
                .queryParam("offset", offset)
                .queryParam("limit", limit)
        ).buildGet();

    return expectSuccess(invocation, JobDataFragment.class);
  }

  protected InitialPreviewResponse transform(DatasetUI datasetUI, TransformBase transformBase) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(getPathJoiner().join(
                    versionedResourcePath(datasetUI),
                    "transformAndPreview")
                ).queryParam("newVersion", newVersion())
        ).buildPost(entity(transformBase, JSON));

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  protected DatasetSearchUIs search(String filter) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path("datasets/search")
                .queryParam("filter", filter)
        ).buildGet();

    return expectSuccess(invocation, DatasetSearchUIs.class);
  }

  protected Invocation reapplyInvocation(DatasetVersionResourcePath versionResourcePath) {
   return getBuilder(getAPIv2().path(versionResourcePath.toString() + "/editOriginalSql")).buildPost(null);
  }

  protected InitialPreviewResponse reapply(DatasetVersionResourcePath versionResourcePath) {
    return expectSuccess(reapplyInvocation(versionResourcePath), InitialPreviewResponse.class);
  }

  protected DatasetUIWithHistory save(DatasetUI datasetUI, String saveVersion) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(versionedResourcePath(datasetUI) + "/save")
                .queryParam("savedTag", saveVersion)
        ).buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  protected DatasetUI rename(DatasetPath datasetPath, String newName) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path("dataset/" + datasetPath.toString() + "/rename")
                .queryParam("renameTo", newName)
        ).buildPost(null);

    return expectSuccess(invocation, DatasetUI.class);
  }

  protected DatasetUI move(DatasetPath currenPath, DatasetPath newPath) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path("dataset/" + currenPath.toString() + "/moveTo/" + newPath.toString())
        ).buildPost(null);

    return expectSuccess(invocation, DatasetUI.class);
  }

  protected DatasetUIWithHistory saveAs(DatasetUI datasetUI, DatasetPath newName) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(versionedResourcePath(datasetUI) + "/save")
                .queryParam("as", newName)
        ).buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  protected void saveAsExpectError(DatasetUI datasetUI, DatasetPath newName) {
    final Invocation invocation =
            getBuilder(
                    getAPIv2()
                            .path(versionedResourcePath(datasetUI) + "/save")
                            .queryParam("as", newName)
            ).buildPost(entity("", JSON));

    expectError(CLIENT_ERROR, invocation, ValidationErrorMessage.class);
  }

  protected DatasetUI delete(String datasetResourcePath, String savedVersion) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(datasetResourcePath)
                .queryParam("savedTag", savedVersion)
        ).buildDelete();

    return expectSuccess(invocation, DatasetUI.class);
  }

  protected void saveExpectConflict(DatasetUI datasetUI, String saveVersion) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(versionedResourcePath(datasetUI) + "/save")
                .queryParam("savedTag", saveVersion)
        ).buildPost(entity("", JSON));

    expectStatus(Status.CONFLICT, invocation);
  }

  protected InitialPendingTransformResponse transformPeek(DatasetUI datasetUI, TransformBase transform) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(versionedResourcePath(datasetUI) + "/transformPeek")
                .queryParam("newVersion", newVersion())
        ).buildPost(entity(transform, JSON));

    return expectSuccess(invocation, InitialPendingTransformResponse.class);
  }

  protected DatasetUI createDatasetFromSQLAndSave(DatasetPath datasetPath, String sql, List<String> context) {
    InitialPreviewResponse datasetCreateResponse = createDatasetFromSQL(sql, context);
    return saveAs(datasetCreateResponse.getDataset(), datasetPath).getDataset();
  }

  protected InitialPreviewResponse createDatasetFromSQL(String sql, List<String> context) {
    return expectSuccess(
        getBuilder(
            getAPIv2()
                .path("datasets/new_untitled_sql")
                .queryParam("newVersion", newVersion())
        ).buildPost(entity(new CreateFromSQL(sql, context), JSON)), // => sending
        InitialPreviewResponse.class); // <= receiving
  }

  protected InitialPreviewResponse createDatasetFromParent(String parentDataset) {
    final Invocation invocation = getBuilder(
        getAPIv2()
            .path("datasets/new_untitled")
            .queryParam("newVersion", newVersion())
            .queryParam("parentDataset", parentDataset)
    ).buildPost(null);


    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  protected DatasetUI createDatasetFromParentAndSave(DatasetPath newDatasetPath, String parentDataset) {
    InitialPreviewResponse response = createDatasetFromParent(parentDataset);

    return saveAs(response.getDataset(), newDatasetPath).getDataset();
  }

  protected DatasetUI createDatasetFromParentAndSave(String newDataSetName, String parentDataset) throws Exception {
    setSpace();
    InitialPreviewResponse response = createDatasetFromParent(parentDataset);

    return saveAs(response.getDataset(), new DatasetPath("spacefoo.folderbar.folderbaz." + newDataSetName)).getDataset();
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

  protected void assertErrorMessage(final GenericErrorMessage errorMessage, final String expectedMoreInfo) {
    assertEquals("error message should be '" + GenericErrorMessage.GENERIC_ERROR_MSG + "'", GenericErrorMessage.GENERIC_ERROR_MSG, errorMessage.getErrorMessage());
    assertThat(errorMessage.getMoreInfo(), CoreMatchers.containsString(expectedMoreInfo));
  }

  protected void assertErrorMessage(final GenericErrorMessage error, final String errorMessage, final String expectedMoreInfo) {
    assertEquals("error message should be '" + errorMessage + "'", errorMessage, error.getErrorMessage());
    assertTrue("Unexpected more infos field", error.getMoreInfo().contains(expectedMoreInfo));
  }

  protected JobId submitAndWaitUntilSubmitted(JobRequest request, JobStatusListener listener) {
    return JobsServiceTestUtils.submitAndWaitUntilSubmitted(l(JobsService.class), request, listener);
  }

  protected JobId submitAndWaitUntilSubmitted(JobRequest request) {
    return JobsServiceTestUtils.submitAndWaitUntilSubmitted(l(JobsService.class), request);
  }

  protected JobId submitJobAndWaitUntilCompletion(JobRequest request, JobStatusListener listener) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(l(JobsService.class), request, listener);
  }

  protected boolean submitJobAndCancelOnTimeOut(JobRequest request, long timeOutInMillis) throws Exception {
    return JobsServiceTestUtils.submitJobAndCancelOnTimeout(l(JobsService.class), request, timeOutInMillis);
  }

  protected JobId submitJobAndWaitUntilCompletion(JobRequest request) {
    return JobsServiceTestUtils.submitJobAndWaitUntilCompletion(l(JobsService.class), request);
  }

  protected void runQuery(JobsService jobsService, String name, int rows, int columns, FolderPath parent, BufferAllocator allocator) throws JobNotFoundException {
    FilePath filePath;
    if (parent == null) {
      filePath = new FilePath(ImmutableList.of(HomeName.getUserHomePath(DEFAULT_USER_NAME).getName(), name));
    } else {
      List<String> path = Lists.newArrayList(parent.toPathList());
      path.add(name);
      filePath = new FilePath(path);
    }
    try (final JobDataFragment truncData = submitJobAndGetData(jobsService,
      JobRequest.newBuilder().setSqlQuery(new SqlQuery(format("select * from %s", filePath.toPathString()), DEFAULT_USER_NAME)).build(),
      0, rows + 1, allocator)) {
      assertEquals(rows, truncData.getReturnedRowCount());
      assertEquals(columns, truncData.getColumns().size());
    }
  }

  protected UserBitShared.QueryProfile getQueryProfile(JobRequest request) throws Exception {
    return JobsServiceTestUtils.getQueryProfile(l(JobsService.class), request);
  }

  protected static void setSystemOption(String optionName, String optionValue) {
    JobsServiceTestUtils.setSystemOption(l(JobsService.class), optionName, optionValue);
  }

  protected static void resetSystemOption(String optionName) {
    JobsServiceTestUtils.resetSystemOption(l(JobsService.class), optionName);
  }

}
