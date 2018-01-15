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
package com.dremio.dac.server;

import static com.dremio.common.utils.PathUtils.getKeyJoiner;
import static com.dremio.common.utils.PathUtils.getPathJoiner;
import static com.dremio.dac.server.FamilyExpectation.CLIENT_ERROR;
import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static javax.ws.rs.client.Entity.entity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.nio.file.Files;
import java.security.Principal;
import java.util.List;
import java.util.Properties;

import javax.inject.Provider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.dremio.common.AutoCloseables;
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
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.CatalogServiceImpl;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.Binder;
import com.dremio.service.BindingProvider;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
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
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.Sets;

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

  protected static boolean isDefaultUserEnabled() {
    return defaultUser;
  }

  protected static void enableDefaultUser(boolean enabled) {
    defaultUser = enabled;
  }

  protected void doc(String message) {
    docLog.println("[doc] " + message);
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
  private static WebTarget apiV2;
  private static WebTarget masterApiV2;
  private static WebTarget publicAPI;
  private static WebTarget masterPublicAPI;
  private static DACDaemon executorDaemon;
  private static DACDaemon currentDremioDaemon;
  private static DACDaemon masterDremioDaemon;
  private static Binder dremioBinder;
  private static SampleDataPopulator populator;

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

  protected static WebTarget getMasterAPIv2() {
    return masterApiV2;
  }

  public static WebTarget getPublicAPI(Integer version) {
    return publicAPI.path("v" + version);
  }

  public static WebTarget getMasterPublicAPI() {
    return masterPublicAPI;
  }

  protected static DACDaemon getCurrentDremioDaemon() {
    return currentDremioDaemon;
  }

  protected static DACDaemon getMasterDremioDaemon() {
    return masterDremioDaemon;
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

  protected static void initClient() {
    ObjectMapper objectMapper = JSONUtil.prettyMapper();
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
    initClient(objectMapper);
  }

  protected static void initClient(ObjectMapper mapper) {
    JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(mapper);
    client = ClientBuilder.newBuilder().register(provider).register(MultiPartFeature.class).build();
    rootTarget = client.target("http://localhost:" + currentDremioDaemon.getWebServer().getPort());
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
      initializeCluster(isMultinode(), new DACDaemonModule(), SingleSourceToStoragePluginConfig.of(new NASSourceConfigurator(), new ClassPathSourceConfigurator()));
    }
  }

  protected static void initializeCluster(final boolean isMultiNode, DACModule dacModule, SourceToStoragePluginConfig configurator) throws Exception {
    final String hostname = InetAddress.getLocalHost().getCanonicalHostName();
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

      // create master node.
      masterDremioDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .masterNode(hostname)
              .autoPort(true)
              .allowTestApis(true)
              .serveUI(false)
              .inMemoryStorage(true)
              .writePath(folder1.getRoot().getAbsolutePath())
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .with(DremioConfig.ENABLE_EXECUTOR_BOOL, false)
              .clusterMode(ClusterMode.DISTRIBUTED),
          DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule,
          configurator);
      masterDremioDaemon.init();

      // remote coordinator node
      int zkPort = masterDremioDaemon.getBindingProvider().lookup(ZkServer.class).getPort();
      currentDremioDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .masterNode(hostname)
              .autoPort(true)
              .allowTestApis(true)
              .serveUI(false)
              .inMemoryStorage(true)
              .writePath(folder2.getRoot().getAbsolutePath())
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .clusterMode(ClusterMode.DISTRIBUTED)
              .localPort(masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort() + 1)
              .masterPort(masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort())
              .isRemote(true)
              .with(DremioConfig.ENABLE_EXECUTOR_BOOL, false)
              .zk("localhost:" + zkPort),
              DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule,
          configurator);
      startCurrentDaemon();

      // remote executor node
      executorDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .masterNode(hostname)
              .autoPort(true)
              .allowTestApis(true)
              .serveUI(false)
              .inMemoryStorage(true)
              .with(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
              .writePath(folder3.getRoot().getAbsolutePath())
              .with(DremioConfig.DIST_WRITE_PATH_STRING, distpath)
              .clusterMode(ClusterMode.DISTRIBUTED)
              .localPort(masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort() + 1)
              .masterPort(masterDremioDaemon.getBindingProvider().lookup(FabricService.class).getPort())
              .isRemote(true)
              .zk("localhost:" + zkPort),
              DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule,
          configurator);
      executorDaemon.init();

      initClient();
    } else {
      logger.info("Running tests in local mode");
      currentDremioDaemon = DACDaemon.newDremioDaemon(
          DACConfig
              .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
              .masterNode(hostname)
              .autoPort(true)
              .allowTestApis(true)
              .serveUI(false)
              .inMemoryStorage(true)
              .writePath(folder1.getRoot().getAbsolutePath())
              .clusterMode(DACDaemon.ClusterMode.LOCAL),
              DremioTest.CLASSPATH_SCAN_RESULT,
          dacModule,
          configurator);
      masterDremioDaemon = null;
      startCurrentDaemon();
      initClient();
    }

    setBinder(createBinder(currentDremioDaemon.getBindingProvider()));

    setPopulator(new SampleDataPopulator(
        l(SabotContext.class),
        newSourceService(),
        newDatasetVersionMutator(),
        l(UserService.class),
        newNamespaceService(),
        DEFAULT_USERNAME
    ));
  }

  protected static NamespaceService newNamespaceService(){
    return l(NamespaceService.class);
  }

  protected static CatalogService newCatelogService() {
    return l(CatalogService.class);
  }

  protected static SourceService newSourceService(){
    return l(SourceService.class);
  }

  protected static DatasetVersionMutator newDatasetVersionMutator(){
    return l(DatasetVersionMutator.class);
  }

  protected static <T> T l(Class<T> clazz) {
    return dremioBinder.lookup(clazz);
  }

  protected static <T> Provider<T> p(Class<T> clazz) {
    return dremioBinder.provider(clazz);
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
    return dremioBinder;
  }

  @AfterClass
  public static void close() throws Exception {
    try (TimedBlock b = Timer.time("BaseTestServer.@AfterClass")) {
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
          executorDaemon, currentDremioDaemon, masterDremioDaemon, populator);
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

  public static void clearAllDataExceptUser() throws IOException {
    if (isMultinode()) {
      ((LocalKVStoreProvider) masterDremioDaemon.getBindingProvider().lookup(KVStoreProvider.class))
          .deleteEverything(SimpleUserService.USER_STORE);
    } else {
      ((LocalKVStoreProvider) getCurrentDremioDaemon().getBindingProvider().lookup(KVStoreProvider.class))
          .deleteEverything(SimpleUserService.USER_STORE);
    }
    currentDremioDaemon.getBindingProvider().lookup(StoragePluginRegistry.class).updateNamespace(Sets.newHashSet("cp", "__datasetDownload", "__home", "__jobResultsStore", "$scratch"), CatalogService.REFRESH_EVERYTHING_NOW);
    ((CatalogServiceImpl)currentDremioDaemon.getBindingProvider().lookup(CatalogService.class)).testTrimBackgroundTasks();
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
   return getBuilder(getAPIv2().path(versionResourcePath.toString() + "/reapply")).buildPost(null);
  }

  protected InitialPreviewResponse reapply(DatasetVersionResourcePath versionResourcePath) {
    return expectSuccess(reapplyInvocation(versionResourcePath), InitialPreviewResponse.class);
  }

  protected DatasetUIWithHistory save(DatasetUI datasetUI, Long saveVersion) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(versionedResourcePath(datasetUI) + "/save")
                .queryParam("savedVersion", saveVersion)
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

  protected DatasetUI delete(String datasetResourcePath, Long savedVersion) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(datasetResourcePath)
                .queryParam("savedVersion", savedVersion)
        ).buildDelete();

    return expectSuccess(invocation, DatasetUI.class);
  }

  protected void saveExpectConflict(DatasetUI datasetUI, Long saveVersion) {
    final Invocation invocation =
        getBuilder(
            getAPIv2()
                .path(versionedResourcePath(datasetUI) + "/save")
                .queryParam("savedVersion", saveVersion)
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
    assertTrue("Unexpected more infos field", errorMessage.getMoreInfo().contains(expectedMoreInfo));
  }

  protected void assertErrorMessage(final GenericErrorMessage error, final String errorMessage, final String expectedMoreInfo) {
    assertEquals("error message should be '" + errorMessage + "'", errorMessage, error.getErrorMessage());
    assertTrue("Unexpected more infos field", error.getMoreInfo().contains(expectedMoreInfo));
  }
}
