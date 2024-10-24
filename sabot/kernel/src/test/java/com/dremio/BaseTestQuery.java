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
package com.dremio;

import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.DEFAULT_PASSWORD;
import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.PARQUET_SCHEMA_FALLBACK_DISABLED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.SupportedEngines.CodeGenOption;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.CopyErrorsTests;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SabotNode;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.util.TestUtilities;
import com.dremio.exec.util.VectorUtil;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.hadoop.security.alias.DremioCredentialProviderFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionValidator;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.EnumValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.services.credentials.CredentialsServiceImpl;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

public class BaseTestQuery extends ExecTest {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  protected static final String TEMP_SCHEMA = "dfs_test";
  protected static final String TEMP_SCHEMA_HADOOP = "dfs_test_hadoop";

  protected static final Set<String> SCHEMAS_FOR_TEST = Sets.newHashSet(TEMP_SCHEMA_HADOOP);

  protected static final int MAX_WIDTH_PER_NODE = 2;
  protected static FileSystem localFs;

  protected static Properties defaultProperties = null;

  protected static String setTableOptionQuery(String table, String optionName, String optionValue) {
    return String.format("ALTER TABLE %s SET %s = %s", table, optionName, optionValue);
  }

  @SuppressWarnings("serial")
  private static final Properties TEST_CONFIGURATIONS =
      new Properties() {
        {
          put(ExecConstants.HTTP_ENABLE, "false");
          put(PARQUET_SCHEMA_FALLBACK_DISABLED, "false");
        }
      };

  public static final class SabotProviderConfig {
    private final boolean allRoles;

    public SabotProviderConfig(boolean allRoles) {
      this.allRoles = allRoles;
    }

    public boolean allRoles() {
      return allRoles;
    }
  }

  public static final class SabotNodeRule extends ExternalResource {
    private static final BiFunction<SabotProviderConfig, List<Module>, SabotNode> DEFAULT_PROVIDER =
        (sabotProviderConfig, overrideModules) -> {
          try {
            return new SabotNode(
                config,
                clusterCoordinator,
                CLASSPATH_SCAN_RESULT,
                sabotProviderConfig.allRoles(),
                overrideModules);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        };

    private BiFunction<SabotProviderConfig, List<Module>, SabotNode> provider = DEFAULT_PROVIDER;
    private List<Module> overrideModules = new ArrayList<>();

    private void resetFields() {
      // we have tests calling BaseTestQuery.setupDefaultTestCluster without being a subclass of
      // BaseTestQuery, thus the rule is not properly executed. by calling this method in both
      // before() and after() we make sure at least our proper tests start with and leave behind
      // a clean state without leaking test-specific configurations through the static rule field
      provider = DEFAULT_PROVIDER;
      overrideModules = new ArrayList<>();
    }

    @Override
    protected void before() throws Throwable {
      resetFields();
    }

    @Override
    protected void after() {
      resetFields();
    }

    public SabotNode newSabotNode(SabotProviderConfig config) {
      return provider.apply(config, overrideModules);
    }

    public void setSabotNodeProvider(
        BiFunction<SabotProviderConfig, List<Module>, SabotNode> provider) {
      this.provider = Preconditions.checkNotNull(provider);
    }

    /***
     * Registers a new guice module to potentially override the default injections from SabotNode.
     * Note when registering multiple modules their bindings are not allowed to overlap, so make
     * sure to use fine-grained modules to have each override specific sub-systems for testing.
     */
    public void register(Module module) {
      overrideModules.add(Preconditions.checkNotNull(module));
    }
  }

  @ClassRule public static final SabotNodeRule SABOT_NODE_RULE = new SabotNodeRule();

  protected static DremioClient client;
  protected static SabotNode[] nodes;
  protected static ClusterCoordinator clusterCoordinator;
  protected static SabotConfig config;

  /**
   * Number of nodes in test cluster. Default is 1.
   *
   * <p>Tests can update the cluster size through {@link #updateTestCluster(int, SabotConfig)}
   */
  private static int nodeCount = 1;

  /** Location of the dfs_test.tmp schema on local filesystem. */
  private static String dfsTestTmpSchemaLocation;

  private int[] columnWidths = new int[] {8};

  private static final SimpleJobRunner INTERNAL_REFRESH_QUERY_RUNNER =
      new SimpleJobRunner() {
        @Override
        public void runQueryAsJob(
            String query, String userName, String queryType, String queryLabel) {
          try {
            runSQL(query); // queries we get here are inner 'refresh dataset' queries
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        }

        @Override
        public List<RecordBatchHolder> runQueryAsJobForResults(
            String query,
            String userName,
            String queryType,
            String queryLabel,
            int offset,
            int limit)
            throws Exception {
          // stub to cover for lack of SysFlight plugin (and inited
          // SystemIcebergTablesStoragePluginConfigProvider) in tests
          if ("COPY_ERRORS_PLAN".equals(queryType)) {
            return CopyErrorsTests.handleCopyErrorsTest(query);
          } else {
            throw new UnsupportedOperationException();
          }
        }
      };

  private static AbstractModule newDefaultModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SimpleJobRunner.class).toInstance(INTERNAL_REFRESH_QUERY_RUNNER);
      }
    };
  }

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    config = SabotConfig.create(TEST_CONFIGURATIONS);

    SABOT_NODE_RULE.register(newDefaultModule());

    DremioCredentialProviderFactory.configure(
        () -> CredentialsServiceImpl.newInstance(DEFAULT_DREMIO_CONFIG, CLASSPATH_SCAN_RESULT));

    openClient();
    localFs = HadoopFileSystem.getLocal(new Configuration());

    // turns on the verbose errors in tests
    // sever side stacktraces are added to the message before sending back to the client
    setSessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS, true);
  }

  protected static void updateTestCluster(int newNodeCount, SabotConfig newConfig) {
    Preconditions.checkArgument(newNodeCount > 0, "Number of Nodes must be at least one");
    if (nodeCount != newNodeCount || config != null) {
      // TODO: Currently we have to shutdown the existing SabotNode cluster before starting a new
      // one with the given
      // SabotNode count. Revisit later to avoid stopping the cluster.
      try {
        closeClient();
        nodeCount = newNodeCount;
        if (newConfig != null) {
          // For next test class, updated SabotConfig will be replaced by default SabotConfig in
          // BaseTestQuery as part
          // of the @BeforeClass method of test class.
          config = newConfig;
        }
        openClient();
      } catch (Exception e) {
        throw new RuntimeException("Failure while updating the test SabotNode cluster.", e);
      }
    }
  }

  /**
   * Useful for tests that require a SabotContext to get/add storage plugins, options etc.
   *
   * @return SabotContext of first SabotNode in the cluster.
   */
  protected static SabotContext getSabotContext() {
    return getSabotContext(0);
  }

  protected static SabotContext getSabotContext(int idx) {
    return getInstance(SabotContext.class, idx);
  }

  private static Injector getInjector() {
    return getInjector(0);
  }

  private static Injector getInjector(int idx) {
    Preconditions.checkState(
        idx < nodeCount && nodes != null && nodes[idx] != null, "Nodes are not setup.");
    return nodes[idx].getInjector();
  }

  protected static <T> T getInstance(Class<T> type) {
    return getInstance(type, 0);
  }

  protected static <T> T getInstance(Class<T> type, int idx) {
    return getInjector(idx).getInstance(type);
  }

  protected static <T> Provider<T> getProvider(Class<T> type) {
    return getInjector().getProvider(type);
  }

  protected static CatalogService getCatalogService() {
    return getInstance(CatalogService.class);
  }

  protected static LocalQueryExecutor getLocalQueryExecutor() {
    return getInstance(LocalQueryExecutor.class);
  }

  protected static DremioRootAllocator getDremioRootAllocator() {
    BufferAllocator allocator = getSabotContext().getAllocator();
    if (allocator instanceof DremioRootAllocator) {
      return (DremioRootAllocator) allocator;
    }
    throw new IllegalStateException("Not a DremioRootAllocator: " + allocator.getClass().getName());
  }

  protected static Properties cloneDefaultTestConfigProperties() {
    final Properties props = new Properties();
    for (String propName : TEST_CONFIGURATIONS.stringPropertyNames()) {
      props.put(propName, TEST_CONFIGURATIONS.getProperty(propName));
    }

    return props;
  }

  public static String getDfsTestTmpSchemaLocation() {
    return dfsTestTmpSchemaLocation;
  }

  protected static void openClient() throws Exception {

    /*
    TODO: enable preconditions and fix all failing tests
    Preconditions.checkState(
        clusterCoordinator == null, "Old cluster not stopped properly (by previous Test?)");
    Preconditions.checkState(
        nodes == null, "Old cluster not stopped properly (by previous Test?)");
    Preconditions.checkState(
        client == null, "Old cluster not stopped properly (by previous Test?)");
    */

    clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

    nodes = new SabotNode[nodeCount];
    for (int i = 0; i < nodeCount; i++) {
      // first node has all roles, and all others are only executors
      nodes[i] = SABOT_NODE_RULE.newSabotNode(new SabotProviderConfig(i == 0));
      nodes[i].run();
    }

    TestUtilities.addDefaultTestPlugins(getCatalogService(), dfsTestTmpSchemaLocation, true);

    client =
        QueryTestUtil.createClient(
            config, clusterCoordinator, MAX_WIDTH_PER_NODE, defaultProperties);

    // turn off re-attempts, this needs to be set at the system level as many unit test will
    // reset the user session by restarting the client
    setEnableReAttempts(false);
  }

  public static void setEnableReAttempts(boolean enabled) throws Exception {
    runSQL(
        "ALTER SYSTEM SET "
            + SqlUtils.QUOTE
            + ExecConstants.ENABLE_REATTEMPTS.getOptionName()
            + SqlUtils.QUOTE
            + " = "
            + enabled);
  }

  protected void createSource(SourceConfig sourceConfig) throws RpcException {
    getCatalogService().createSourceIfMissingWithThrow(sourceConfig);
  }

  protected static void disablePlanCache() throws Exception {
    final String alterSessionSetSql = "ALTER SESSION SET planner.query_plan_cache_enabled = false";
    runSQL(alterSessionSetSql);
  }

  private static void closeCurrentClient() {
    Preconditions.checkState(nodes != null && nodes[0] != null, "Nodes are not setup.");
    if (client != null) {
      client.close();
      client = null;
    }
  }

  /**
   * Close the current <i>client</i> and open a new client using the given <i>properties</i>. All
   * tests executed after this method call use the new <i>client</i>.
   *
   * @param properties
   */
  public static void updateClient(Properties properties) throws Exception {
    closeCurrentClient();
    client = QueryTestUtil.createClient(config, clusterCoordinator, MAX_WIDTH_PER_NODE, properties);
  }

  /*
   * Close the current <i>client</i> and open a new client for the given user. All tests executed
   * after this method call use the new <i>client</i>.
   * @param user
   */
  public static void updateClient(String user) throws Exception {
    updateClient(user, DEFAULT_PASSWORD);
  }

  /*
   * Close the current <i>client</i> and open a new client for the given user and password credentials. Tests
   * executed after this method call use the new <i>client</i>.
   * @param user
   */
  public static void updateClient(final String user, final String password) throws Exception {
    final Properties props = new Properties();
    props.setProperty(UserSession.USER, user);
    if (password != null) {
      props.setProperty(UserSession.PASSWORD, password);
    }
    updateClient(props);
  }

  public static CoordinationProtos.NodeEndpoint getFirstCoordinatorEndpoint() {
    return Preconditions.checkNotNull(clusterCoordinator).getCoordinatorEndpoints().stream()
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("no coordinator found"));
  }

  /*
   * Returns JDBC url to connect to one of the drilbit
   *
   * Used by ITTestShadedJar test through reflection!!!
   */
  public static String getJDBCURL() {
    CoordinationProtos.NodeEndpoint endpoint = getFirstCoordinatorEndpoint();
    return format("jdbc:dremio:direct=%s:%d", endpoint.getAddress(), endpoint.getUserPort());
  }

  public TestBuilder testBuilder() {
    return new TestBuilder(getTestAllocator());
  }

  @AfterClass
  public static void closeClient() throws Exception {
    if (client != null) {
      client.close();
      client = null;
    }

    if (nodes != null) {
      for (final SabotNode bit : nodes) {
        if (bit != null) {
          bit.close();
        }
      }
      nodes = null;
    }

    if (clusterCoordinator != null) {
      clusterCoordinator.close();
      clusterCoordinator = null;
    }
  }

  @AfterClass
  public static void resetNodeCount() {
    // some test classes assume this value to be 1 and will fail if run along other tests that
    // increase it
    nodeCount = 1;
  }

  public static void runSQL(String sql) throws Exception {
    final AwaitableUserResultsListener listener =
        new AwaitableUserResultsListener(new SilentListener());
    testWithListener(QueryType.SQL, sql, listener);
    listener.await();
  }

  public static void runSQL(String sql, Object... replacements) throws Exception {
    runSQL(String.format(sql, replacements));
  }

  protected static List<QueryDataBatch> testSqlWithResults(String sql) throws Exception {
    return testRunAndReturn(QueryType.SQL, sql);
  }

  protected static List<QueryDataBatch> testPhysicalWithResults(String physical) throws Exception {
    return testRunAndReturn(QueryType.PHYSICAL, physical);
  }

  public static List<QueryDataBatch> testRunAndReturn(QueryType type, Object query)
      throws Exception {
    if (type == QueryType.PREPARED_STATEMENT) {
      Preconditions.checkArgument(
          query instanceof PreparedStatementHandle,
          "Expected an instance of PreparedStatement as input query");
      return testPreparedStatement((PreparedStatementHandle) query);
    } else {
      Preconditions.checkArgument(query instanceof String, "Expected a string as input query");
      query = TestTools.replaceWorkingPathPlaceholders((String) query);
      return client.runQuery(type, (String) query);
    }
  }

  public static List<QueryDataBatch> testPreparedStatement(PreparedStatementHandle handle)
      throws Exception {
    return client.executePreparedStatement(handle, List.of());
  }

  public static int testRunAndPrint(final QueryType type, final String query) throws Exception {
    return QueryTestUtil.testRunAndPrint(client, type, query);
  }

  protected static void testWithListener(
      QueryType type, String query, UserResultsListener resultListener) {
    QueryTestUtil.testWithListener(client, type, query, resultListener);
  }

  /** prefer {@link #runSQL(String, Object...)} instead */
  public static void testNoResult(String query, Object... args) throws Exception {
    runSQL(query, args);
  }

  public static void test(String query, Object... args) throws Exception {
    test(String.format(query, args));
  }

  public static void test(final String query) throws Exception {
    QueryTestUtil.test(client, query);
  }

  public static AutoCloseable withOption(final BooleanValidator validator, boolean value) {
    return withOptionInternal(validator, value, false);
  }

  public static AutoCloseable withSystemOption(final BooleanValidator validator, boolean value) {
    return withOptionInternal(validator, value, true);
  }

  public static AutoCloseable withOption(final LongValidator validator, long value) {
    return withOptionInternal(validator, value, false);
  }

  public static AutoCloseable withSystemOption(final LongValidator validator, long value) {
    return withOptionInternal(validator, value, true);
  }

  public static AutoCloseable withOption(final DoubleValidator validator, double value) {
    return withOptionInternal(validator, value, false);
  }

  public static AutoCloseable withSystemOption(final DoubleValidator validator, double value) {
    return withOptionInternal(validator, value, true);
  }

  public static AutoCloseable withOption(final StringValidator validator, String value) {
    return withOptionInternal(validator, "'" + value + "'", false);
  }

  public static AutoCloseable withSystemOption(final StringValidator validator, String value) {
    return withOptionInternal(validator, "'" + value + "'", true);
  }

  public static <T extends Enum<T>> AutoCloseable withOption(
      final EnumValidator<T> validator, T value) {
    return withOptionInternal(validator, "'" + value + "'", false);
  }

  public static <T extends Enum<T>> AutoCloseable withSystemOption(
      final EnumValidator<T> validator, T value) {
    return withOptionInternal(validator, "'" + value + "'", true);
  }

  private static AutoCloseable withOptionInternal(
      final OptionValidator validator, Object value, boolean isSystem) {
    if (value.equals(validator.getDefault().getValue())) {
      logger.warn(
          "Test called withOptionInternal with default value: {}", validator.getOptionName());
    }
    final String optionScope = isSystem ? "SYSTEM" : "SESSION";
    try {
      testNoResult(
          String.format(
              "ALTER %s SET %s%s%s = %s",
              optionScope, SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE, value));
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to set " + optionScope + " option " + validator.getOptionName() + " to " + value,
          e);
    }
    return () ->
        testNoResult(
            String.format(
                "ALTER %s RESET %s%s%s",
                optionScope, SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE));
  }

  protected static int testLogical(String query) throws Exception {
    return testRunAndPrint(QueryType.LOGICAL, query);
  }

  protected static int testPhysical(String query) throws Exception {
    return testRunAndPrint(QueryType.PHYSICAL, query);
  }

  protected static int testSql(String query) throws Exception {
    return testRunAndPrint(QueryType.SQL, query);
  }

  protected static int testPhysicalFromFile(String file) throws Exception {
    return testPhysical(getFile(file));
  }

  protected static List<QueryDataBatch> testPhysicalFromFileWithResults(String file)
      throws Exception {
    return testRunAndReturn(QueryType.PHYSICAL, getFile(file));
  }

  /**
   * Utility method which tests given query produces a {@link UserException} and the exception
   * message contains the given message.
   *
   * @param testSqlQuery Test query
   * @param expectedErrorMsg Expected error message.
   */
  protected static void errorMsgTestHelper(
      final String testSqlQuery, final String expectedErrorMsg) {
    assertThatThrownBy(() -> test(testSqlQuery))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(expectedErrorMsg);
  }

  /**
   * Utility method which tests given query produces a {@link UserException} with the expected
   * errorType.
   *
   * @param testSqlQuery Test query
   * @param expectedErrorType Expected error type
   */
  protected static void errorTypeTestHelper(
      final String testSqlQuery, final ErrorType expectedErrorType) {
    errorMsgWithTypeTestHelper(testSqlQuery, expectedErrorType, "");
  }

  /**
   * Utility method which tests given query produces a {@link UserException} with the expected
   * errorType and exception message.
   *
   * @param testSqlQuery Test query
   * @param expectedErrorType Expected error type
   * @param expectedErrorMsg Expected error message.
   */
  protected static void errorMsgWithTypeTestHelper(
      final String testSqlQuery, final ErrorType expectedErrorType, final String expectedErrorMsg) {
    ObjectAssert<UserRemoteException> ure =
        assertThatThrownBy(() -> test(testSqlQuery))
            .isInstanceOf(UserRemoteException.class)
            .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class));

    ure.extracting(UserRemoteException::getErrorType).isEqualTo(expectedErrorType);

    ure.extracting(UserRemoteException::getOriginalMessage)
        .asInstanceOf(InstanceOfAssertFactories.STRING)
        .contains(expectedErrorMsg);
  }

  protected static String getFile(String resource) {
    return readResourceAsString(resource);
  }

  public void writeDir(java.nio.file.Path baseDir, java.nio.file.Path dest, String srcDirName) {
    URL resource = Resources.getResource(baseDir.resolve(srcDirName).toString());
    try (Stream<java.nio.file.Path> fileStream =
        java.nio.file.Files.walk(Paths.get(resource.getPath()))) {
      fileStream.forEach(
          inputFile -> {
            if (!java.nio.file.Files.isDirectory(inputFile)) {
              writeFile(baseDir, dest, inputFile.getFileName());
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void writeFile(
      java.nio.file.Path baseDir, java.nio.file.Path dest, java.nio.file.Path srcFileName) {
    URL resource =
        Resources.getResource(baseDir.resolve(dest.getFileName()).resolve(srcFileName).toString());
    try {
      java.nio.file.Files.write(dest.resolve(srcFileName), Resources.toByteArray(resource));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Copy the resource (ex. file on classpath) to a physical file on FileSystem.
   *
   * @param resource
   * @return the file path
   * @throws IOException
   */
  public static String getPhysicalFileFromResource(final String resource) throws IOException {
    final File file = File.createTempFile("tempfile", ".txt");
    file.deleteOnExit();
    final PrintWriter printWriter = new PrintWriter(file);
    printWriter.write(BaseTestQuery.getFile(resource));
    printWriter.close();

    return file.getPath();
  }

  /**
   * Create a temp parent directory to store the given directory with name {@code dirName}. Does
   * <b>NOT</b> create the directory itself.
   *
   * @param dirName
   * @return Full path including temp parent directory and given directory name.
   */
  public static String getTempDir(final String dirName) {
    final File dir = Files.createTempDir();
    dir.deleteOnExit();
    return dir.getAbsolutePath() + File.separator + dirName;
  }

  /**
   * Create a temp directory with name {@code dirName}.
   *
   * @param dirName
   * @return Full path including temp parent directory and given directory name.
   */
  public static File createTempDirWithName(String dirName) {
    final File dir = Files.createTempDir();
    File file = new File(dir, dirName);
    file.mkdirs();
    file.deleteOnExit();
    return file;
  }

  /**
   * Create a temp directory with name {@code dirName} in dfs_test.
   *
   * @param dirName
   * @return Full path including temp parent directory and given directory name.
   */
  public static File createDfsTestTableDirWithName(String dirName) {
    File file = new File(getDfsTestTmpSchemaLocation(), dirName);
    file.mkdirs();
    file.deleteOnExit();
    return file;
  }

  protected static void copy(java.nio.file.Path source, java.nio.file.Path dest) {
    try {
      java.nio.file.Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      throw new RuntimeException("Failed to copy file from: " + source + " to: " + dest, e);
    }
  }

  public static void copyFromJar(String sourceElement, final java.nio.file.Path target)
      throws URISyntaxException, IOException {
    URI resource = Resources.getResource(sourceElement).toURI();

    if (resource.getScheme().equals("jar")) {
      // if resources are in a jar and not exploded on disk, we need to use a filesystem wrapper on
      // top of the jar to enumerate files in the table directory
      try (java.nio.file.FileSystem fileSystem =
          FileSystems.newFileSystem(resource, Collections.emptyMap())) {
        sourceElement = !sourceElement.startsWith("/") ? "/" + sourceElement : sourceElement;
        java.nio.file.Path srcDir = fileSystem.getPath(sourceElement);
        try (Stream<java.nio.file.Path> stream = java.nio.file.Files.walk(srcDir)) {
          stream.forEach(
              source -> {
                java.nio.file.Path dest =
                    target.resolve(Paths.get(srcDir.relativize(source).toString()));
                copy(source, dest);
              });
        }
      }
    } else {
      java.nio.file.Path srcDir = java.nio.file.Paths.get(resource);
      try (Stream<java.nio.file.Path> stream = java.nio.file.Files.walk(srcDir)) {
        stream.forEach(source -> copy(source, target.resolve(srcDir.relativize(source))));
      }
    }
  }

  /**
   * @deprecated use {@code withOption} instead
   */
  @Deprecated
  protected static void resetSessionOption(final OptionValidator option) {
    String sql =
        String.format(
            "ALTER SESSION RESET %s%s%s", SqlUtils.QUOTE, option.getOptionName(), SqlUtils.QUOTE);
    try {
      runSQL(sql);
    } catch (Exception e) {
      throw new RuntimeException("resetSessionOption failed: " + sql, e);
    }
  }

  /**
   * @deprecated use {@link #withOption(BooleanValidator, boolean)} instead
   */
  @Deprecated
  public static void setSessionOption(BooleanValidator option, boolean value) {
    setSessionOption(option, Boolean.toString(value));
  }

  /**
   * @deprecated use {@link #withOption(LongValidator, long)} instead
   */
  @Deprecated
  public static void setSessionOption(LongValidator option, long value) {
    setSessionOption(option, Long.toString(value));
  }

  /**
   * @deprecated use {@link #withOption(DoubleValidator, double)} instead
   */
  @Deprecated
  public static void setSessionOption(DoubleValidator option, double value) {
    setSessionOption(option, Double.toString(value));
  }

  /**
   * @deprecated use {@link #withOption(StringValidator, String)} instead
   */
  @Deprecated
  public static void setSessionOption(StringValidator option, String value) {
    setSessionOption((OptionValidator) option, "'" + value + "'");
  }

  /**
   * @deprecated use {@link #withOption(EnumValidator, Enum)} instead
   */
  @Deprecated
  public static <T extends Enum<T>> void setSessionOption(EnumValidator<T> option, T value) {
    setSessionOption((OptionValidator) option, "'" + value + "'");
  }

  @Deprecated
  private static void setSessionOption(OptionValidator option, final String value) {
    if (value.equals(option.getDefault().getValue())) {
      logger.warn("Test called setSessionOption with default value: {}", option.getOptionName());
    }
    String sql =
        String.format(
            "ALTER SESSION SET %1$s%2$s%1$s = %3$s", SqlUtils.QUOTE, option.getOptionName(), value);
    try {
      runSQL(sql);
    } catch (Exception e) {
      throw new RuntimeException("setSessionOption failed: " + sql, e);
    }
  }

  protected static String getSystemOptionQueryString(final String option, final String value) {
    return String.format("ALTER SYSTEM SET %1$s%2$s%1$s = %3$s", SqlUtils.QUOTE, option, value);
  }

  /**
   * @deprecated use {@link #withSystemOption(BooleanValidator, boolean)} instead
   */
  @Deprecated
  public static void setSystemOption(BooleanValidator option, boolean value) {
    setSystemOption(option, Boolean.toString(value));
  }

  /**
   * @deprecated use {@link #withSystemOption(LongValidator, long)} instead
   */
  @Deprecated
  public static void setSystemOption(LongValidator option, long value) {
    setSystemOption(option, Long.toString(value));
  }

  /**
   * @deprecated use {@link #withSystemOption(DoubleValidator, double)} instead
   */
  @Deprecated
  public static void setSystemOption(DoubleValidator option, double value) {
    setSystemOption(option, Double.toString(value));
  }

  /**
   * @deprecated use {@link #withSystemOption(StringValidator, String)} instead
   */
  @Deprecated
  public static void setSystemOption(StringValidator option, String value) {
    setSystemOption((OptionValidator) option, "'" + value + "'");
  }

  /**
   * @deprecated use {@link #withSystemOption(EnumValidator, Enum)} instead
   */
  @Deprecated
  public static <T extends Enum<T>> void setSystemOption(EnumValidator<T> option, T value) {
    setSystemOption((OptionValidator) option, "'" + value + "'");
  }

  private static void setSystemOption(OptionValidator option, String value) {
    if (value.equals(option.getDefault().getValue())) {
      logger.warn("Test called setSystemOption with default value: {}", option.getOptionName());
    }
    String sql = getSystemOptionQueryString(option.getOptionName(), value);
    try {
      runSQL(sql);
    } catch (Exception e) {
      throw new RuntimeException("setSystemOption failed: " + sql, e);
    }
  }

  /**
   * @deprecated use {@code withOption} instead
   */
  @Deprecated
  protected static void resetSystemOption(OptionValidator option) {
    String sql =
        String.format("ALTER SYSTEM RESET %1$s%2$s%1$s", SqlUtils.QUOTE, option.getOptionName());
    try {
      runSQL(sql);
    } catch (Exception e) {
      throw new RuntimeException("resetSystemOption failed: " + sql, e);
    }
  }

  protected static void resetSessionSettings() {
    String sql = "ALTER SESSION RESET ALL";
    try {
      runSQL(sql);
    } catch (Exception e) {
      throw new RuntimeException("resetSessionSettings failed: " + sql, e);
    }
  }

  protected static AutoCloseable enableHiveAsync() {
    return withSystemOption(ExecConstants.ENABLE_HIVE_ASYNC, true);
  }

  protected static AutoCloseable enableUseSyntax() {
    return withSystemOption(ExecConstants.ENABLE_USE_VERSION_SYNTAX, true);
  }

  protected static AutoCloseable disableIcebergSortOrder() {
    return withSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER, false);
  }

  protected static AutoCloseable disablePartitionPruning() {
    return withSystemOption(PlannerSettings.ENABLE_PARTITION_PRUNING, false);
  }

  protected static AutoCloseable enableIcebergTablePropertiesSupportFlag() {
    return withSystemOption(ExecConstants.ENABLE_ICEBERG_TABLE_PROPERTIES, true);
  }

  protected static AutoCloseable enableJsonReadNumbersAsDouble() {
    return withSystemOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR, true);
  }

  protected static AutoCloseable enableJsonAllStrings() {
    return withSystemOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR, true);
  }

  protected static AutoCloseable disableExchanges() {
    return withSystemOption(PlannerSettings.DISABLE_EXCHANGES, true);
  }

  protected static AutoCloseable enableHiveParquetComplexTypes() {
    return withSystemOption(ExecConstants.HIVE_COMPLEXTYPES_ENABLED, true);
  }

  protected static AutoCloseable disableHiveParquetComplexTypes() {
    return withSystemOption(ExecConstants.HIVE_COMPLEXTYPES_ENABLED, false);
  }

  protected static AutoCloseable enableMapDataType() {
    return withSystemOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
  }

  protected static AutoCloseable enableComplexHiveType() {
    return withSystemOption(ExecConstants.ENABLE_COMPLEX_HIVE_DATA_TYPE, true);
  }

  protected static AutoCloseable enableTableOption(String table, String optionName)
      throws Exception {
    String setOptionQuery = setTableOptionQuery(table, optionName, "true");
    String unSetOptionQuery = setTableOptionQuery(table, optionName, "false");

    runSQL(setOptionQuery);
    return () -> runSQL(unSetOptionQuery);
  }

  protected static AutoCloseable disableParquetVectorization() {
    return withSystemOption(ExecConstants.PARQUET_READER_VECTORIZE, false);
  }

  protected static AutoCloseable setMaxLeafColumns(long newVal) {
    return withSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX, newVal);
  }

  protected static AutoCloseable enableJavaExecution() {
    setSystemOption(ExecConstants.QUERY_EXEC_OPTION, CodeGenOption.Java);
    return () -> setSystemOption(ExecConstants.QUERY_EXEC_OPTION, CodeGenOption.Gandiva);
  }

  protected static AutoCloseable enableSARGableFilterTransform() {
    return withSystemOption(PlannerSettings.SARGABLE_FILTER_TRANSFORM, true);
  }

  protected static AutoCloseable enableIcebergAutoCluster() {
    return withSystemOption(ExecConstants.ENABLE_ICEBERG_AUTO_CLUSTERING, true);
  }

  public static class SilentListener implements UserResultsListener {
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public void submissionFailed(UserException ex) {
      logger.debug("Query failed: " + ex.getMessage());
    }

    @Override
    public void queryCompleted(QueryState state) {
      logger.debug("Query completed successfully with row count: " + count.get());
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      final int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        count.addAndGet(rows);
      }
      result.release();
    }

    @Override
    public void queryIdArrived(QueryId queryId) {}
  }

  // captures queryId and state.
  public static class QueryIdCapturingListener extends SilentListener {
    private QueryId queryId;
    private QueryState queryState;

    @Override
    public void queryIdArrived(QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public void queryCompleted(QueryState queryState) {
      this.queryState = queryState;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public QueryState getQueryState() {
      return queryState;
    }
  }

  public static int getRecordCount(List<QueryDataBatch> result)
      throws UnsupportedEncodingException, SchemaChangeException {
    int i = 0;
    for (QueryDataBatch batch : result) {
      i += batch.getHeader().getRowCount();
      batch.release();
    }
    return i;
  }

  protected int printResult(List<QueryDataBatch> results) throws SchemaChangeException {
    int rowCount = 0;
    final RecordBatchLoader loader = new RecordBatchLoader(getTestAllocator());
    for (final QueryDataBatch result : results) {
      rowCount += result.getHeader().getRowCount();
      loader.load(result.getHeader().getDef(), result.getData());
      // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
      // SchemaChangeException, so check/clean throw clause above.
      VectorUtil.showVectorAccessibleContent(loader, columnWidths);
      loader.clear();
      result.release();
    }
    System.out.println("Total record count: " + rowCount);
    return rowCount;
  }

  protected String getResultString(List<QueryDataBatch> results, String delimiter)
      throws SchemaChangeException {
    return getResultString(results, delimiter, true);
  }

  protected String getResultString(
      List<QueryDataBatch> results, String delimiter, boolean includeHeader)
      throws SchemaChangeException {
    final StringBuilder formattedResults = new StringBuilder();
    final RecordBatchLoader loader = new RecordBatchLoader(getTestAllocator());
    for (final QueryDataBatch result : results) {
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() <= 0) {
        continue;
      }
      VectorUtil.appendVectorAccessibleContent(loader, formattedResults, delimiter, includeHeader);
      if (!includeHeader) {
        includeHeader = false;
      }
      loader.clear();
      result.release();
    }

    return formattedResults.toString();
  }

  protected static String getValueInFirstRecord(String sql, String columnName) throws Exception {
    final List<QueryDataBatch> results = testSqlWithResults(sql);
    final RecordBatchLoader loader = new RecordBatchLoader(getDremioRootAllocator());
    final StringBuilder builder = new StringBuilder();
    final boolean silent =
        config != null && config.getBoolean(QueryTestUtil.TEST_QUERY_PRINTING_SILENT);

    for (final QueryDataBatch b : results) {
      if (!b.hasData()) {
        continue;
      }

      loader.load(b.getHeader().getDef(), b.getData());

      final VectorWrapper<?> vw;
      try {
        vw =
            loader.getValueAccessorById(
                VarCharVector.class,
                loader.getValueVectorId(SchemaPath.getSimplePath(columnName)).getFieldIds());
      } catch (Throwable t) {
        throw new Exception(
            "Looks like you did not provide an explain plan query, please add EXPLAIN PLAN FOR to the beginning of your query.");
      }

      if (!silent) {
        System.out.println(vw.getValueVector().getField().getName());
      }
      final ValueVector vv = vw.getValueVector();
      for (int i = 0; i < vv.getValueCount(); i++) {
        final Object o = vv.getObject(i);
        builder.append(o);
        if (!silent) {
          System.out.println(o);
        }
      }
      loader.clear();
      b.release();
    }

    return builder.toString();
  }

  protected static IcebergModel getIcebergModel(String pluginName) {
    StoragePlugin plugin = getCatalogService().getSource(pluginName);
    if (plugin instanceof SupportsIcebergMutablePlugin) {
      SupportsIcebergMutablePlugin icebergMutablePlugin = (SupportsIcebergMutablePlugin) plugin;
      return icebergMutablePlugin.getIcebergModel(
          null,
          null,
          null,
          icebergMutablePlugin.createIcebergFileIO(localFs, null, null, null, null),
          null);
    } else {
      throw new UnsupportedOperationException(
          String.format("Plugin %s does not implement SupportsIcebergMutablePlugin", pluginName));
    }
  }

  public static Table getIcebergTable(IcebergModel icebergModel, File tableRoot) {
    return icebergModel.getIcebergTable(icebergModel.getTableIdentifier(tableRoot.getPath()));
  }

  public static Table getIcebergTable(File tableRoot, IcebergCatalogType catalogType) {
    IcebergModel icebergModel =
        getIcebergModel(
            catalogType == IcebergCatalogType.NESSIE ? TEMP_SCHEMA : TEMP_SCHEMA_HADOOP);
    return getIcebergTable(icebergModel, tableRoot);
  }

  public static Table getIcebergTable(File tableRoot) {
    return getIcebergTable(getIcebergModel(TEMP_SCHEMA), tableRoot);
  }

  public static FileSystemPlugin getMockedFileSystemPlugin() {
    try {

      FileSystemPlugin fileSystemPlugin = mock(FileSystemPlugin.class);
      FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
      when(fileSystemPlugin.getSystemUserFS()).thenReturn(fs);
      when(fileSystemPlugin.getFsConfCopy()).thenReturn(new Configuration());
      when(fileSystemPlugin.createIcebergFileIO(any(), any(), any(), any(), any()))
          .thenReturn(
              new DremioFileIO(
                  fs,
                  null,
                  null,
                  null,
                  null,
                  new HadoopFileSystemConfigurationAdapter(new Configuration())));
      return fileSystemPlugin;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  protected static org.apache.hadoop.fs.FileSystem setupLocalFS() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
    return org.apache.hadoop.fs.FileSystem.get(conf);
  }

  protected static String getMetadataJsonString(String tableName) throws IOException {
    String metadataLoc = getDfsTestTmpSchemaLocation() + String.format("/%s/metadata", tableName);
    File metadataFolder = new File(metadataLoc);
    String metadataJson = "";

    Assert.assertNotNull(metadataFolder.listFiles());
    for (File currFile : metadataFolder.listFiles()) {
      if (currFile.getName().endsWith("metadata.json")) {
        // We'll only have one metadata.json file as this is the first transaction for table
        // temp_table0
        metadataJson =
            new String(
                java.nio.file.Files.readAllBytes(Paths.get(currFile.getPath())),
                StandardCharsets.US_ASCII);
        break;
      }
    }
    Assert.assertNotNull(metadataJson);
    return metadataJson;
  }
}
