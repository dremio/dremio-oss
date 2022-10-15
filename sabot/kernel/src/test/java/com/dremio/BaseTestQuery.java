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
import static com.dremio.exec.store.iceberg.IcebergModelCreator.DREMIO_NESSIE_DEFAULT_NAMESPACE;
import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.PARQUET_SCHEMA_FALLBACK_DISABLED;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SabotNode;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieModel;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.exec.util.TestUtilities;
import com.dremio.exec.util.VectorUtil;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.options.OptionValidator;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.BindingCreator;
import com.dremio.service.BindingProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;

public class BaseTestQuery extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  protected static final String TEMP_SCHEMA = "dfs_test";
  protected static final String TEMP_SCHEMA_HADOOP = "dfs_test_hadoop";

  protected static final Set<String> SCHEMAS_FOR_TEST = Sets.newHashSet(TEMP_SCHEMA_HADOOP);

  protected static final int MAX_WIDTH_PER_NODE = 2;
  private static final Random random = new Random();
  protected static FileSystem localFs;

  protected static Properties defaultProperties = null;

  protected static String setTableOptionQuery(String table, String optionName, String optionValue) {
    return String.format("ALTER TABLE %s SET %s = %s", table, optionName, optionValue);
  }

  @SuppressWarnings("serial")
  private static final Properties TEST_CONFIGURATIONS = new Properties() {
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
    private static final Function<SabotProviderConfig, SabotNode> DEFAULT_PROVIDER =
        new Function<SabotProviderConfig, SabotNode>() {
          @Override
          public SabotNode apply(SabotProviderConfig input) {
            try {
              List<Module> modules = Collections.singletonList(SABOT_NODE_RULE.getModules());
              return new SabotNode(config, clusterCoordinator, CLASSPATH_SCAN_RESULT, input.allRoles(), modules);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        };

    private Function<SabotProviderConfig, SabotNode> provider = DEFAULT_PROVIDER;

    private Module module = null;

    @Override
    protected void before() throws Throwable {
      provider = DEFAULT_PROVIDER;
      module = null;
    }

    SabotNode newSabotNode(SabotProviderConfig config) {
      return provider.apply(config);
    }

    public Function<SabotProviderConfig, SabotNode>
    setSabotNodeProvider(Function<SabotProviderConfig, SabotNode> provider) {
      final Function<SabotProviderConfig, SabotNode> previous = this.provider;
      this.provider = Preconditions.checkNotNull(provider);
      return previous;
    }

    public void register(Module module) {
      this.module = module;
    }

    public Module getModules() {
      return module;
    }
  }

  @ClassRule
  public static final SabotNodeRule SABOT_NODE_RULE = new SabotNodeRule();

  protected static DremioClient client;
  protected static SabotNode[] nodes;
  protected static ClusterCoordinator clusterCoordinator;
  protected static SabotConfig config;

  /**
   * Number of nodes in test cluster. Default is 1.
   *
   * Tests can update the cluster size through {@link #updateTestCluster(int, SabotConfig)}
   */
  private static int nodeCount = 1;

  /**
   * Location of the dfs_test.tmp schema on local filesystem.
   */
  private static String dfsTestTmpSchemaLocation;

  private int[] columnWidths = new int[] { 8 };

  private static AbstractModule getDefaultModule() {

    SimpleJobRunner internalRefreshQueryRunner = (query, userName, queryType) -> {
      try {
        runSQL(query); // queries we get here are inner 'refresh dataset' queries
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    };

    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SimpleJobRunner.class).toInstance(internalRefreshQueryRunner);
      }
    };
  }

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    config = SabotConfig.create(TEST_CONFIGURATIONS);

    Module module = Optional.ofNullable(SABOT_NODE_RULE.module)
      .map(m -> Modules.combine(m, getDefaultModule())).orElse(getDefaultModule());

    SABOT_NODE_RULE.register(module);

    openClient();
    localFs = HadoopFileSystem.getLocal(new Configuration());

    // turns on the verbose errors in tests
    // sever side stacktraces are added to the message before sending back to the client
    setSessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, "true");
  }

  protected static void updateTestCluster(int newNodeCount, SabotConfig newConfig) {
    Preconditions.checkArgument(newNodeCount > 0, "Number of Nodes must be at least one");
    if (nodeCount != newNodeCount || config != null) {
      // TODO: Currently we have to shutdown the existing SabotNode cluster before starting a new one with the given
      // SabotNode count. Revisit later to avoid stopping the cluster.
      try {
        closeClient();
        nodeCount = newNodeCount;
        if (newConfig != null) {
          // For next test class, updated SabotConfig will be replaced by default SabotConfig in BaseTestQuery as part
          // of the @BeforeClass method of test class.
          config = newConfig;
        }
        openClient();
      } catch(Exception e) {
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
    Preconditions.checkState(nodes != null && nodes[0] != null, "Nodes are not setup.");
    return nodes[0].getContext();
  }

  protected static BindingCreator getBindingCreator(){
    return nodes[0].getBindingCreator();
  }

  protected static BindingProvider getBindingProvider(){
    return nodes[0].getBindingProvider();
  }

  protected static Injector getInjector(){
    return nodes[0].getInjector();
  }

  protected static LocalQueryExecutor getLocalQueryExecutor() {
    Preconditions.checkState(nodes != null && nodes[0] != null, "Nodes are not setup.");
    return nodes[0].getLocalQueryExecutor();
  }

  protected static Properties cloneDefaultTestConfigProperties() {
    final Properties props = new Properties();
    for(String propName : TEST_CONFIGURATIONS.stringPropertyNames()) {
      props.put(propName, TEST_CONFIGURATIONS.getProperty(propName));
    }

    return props;
  }

  protected static String getDfsTestTmpSchemaLocation() {
    return dfsTestTmpSchemaLocation;
  }

  protected static void openClient() throws Exception {
    clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

    nodes = new SabotNode[nodeCount];
    for(int i = 0; i < nodeCount; i++) {
      // first node has all roles, and all others are only executors
      nodes[i] = SABOT_NODE_RULE.newSabotNode(new SabotProviderConfig(i == 0));
      nodes[i].run();
      if(i == 0) {
        TestUtilities.addDefaultTestPlugins(nodes[i].getContext().getCatalogService(), dfsTestTmpSchemaLocation, true);
      }
    }

    client = QueryTestUtil.createClient(config,  clusterCoordinator, MAX_WIDTH_PER_NODE, defaultProperties);

    // turn off re-attempts, this needs to be set at the system level as many unit test will
    // reset the user session by restarting the client
    setEnableReAttempts(false);
  }

  protected static void setEnableReAttempts(boolean enabled) throws Exception {
    runSQL("ALTER SYSTEM SET " + SqlUtils.QUOTE + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + SqlUtils.QUOTE + " = " + enabled);
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
   * Close the current <i>client</i> and open a new client using the given <i>properties</i>. All tests executed
   * after this method call use the new <i>client</i>.
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

  /*
   * Returns JDBC url to connect to one of the drilbit
   *
   * Used by ITTestShadedJar test through reflection!!!
   */
  public static String getJDBCURL() {
    Collection<CoordinationProtos.NodeEndpoint> endpoints = clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR)
      .getAvailableEndpoints();
    if (endpoints.isEmpty()) {
      return null;
    }

    CoordinationProtos.NodeEndpoint endpoint = endpoints.iterator().next();
    return format("jdbc:dremio:direct=%s:%d", endpoint.getAddress(), endpoint.getUserPort());
  }

  public TestBuilder testBuilder() {
    return new TestBuilder(allocator);
  }

  @AfterClass
  public static void closeClient() throws Exception {
    if (client != null) {
      client.close();
      client = null;
    }

    if (nodes != null) {
      for(final SabotNode bit : nodes) {
        if (bit != null) {
          bit.close();
        }
      }
      nodes = null;
    }

    if(clusterCoordinator != null) {
      clusterCoordinator.close();
      clusterCoordinator = null;
    }
  }

  @AfterClass
  public static void resetNodeCount() {
    // some test classes assume this value to be 1 and will fail if run along other tests that increase it
    nodeCount = 1;
  }

  protected static void runSQL(String sql) throws Exception {
    final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(new SilentListener());
    testWithListener(QueryType.SQL, sql, listener);
    listener.await();
  }

  protected static List<QueryDataBatch> testSqlWithResults(String sql) throws Exception{
    return testRunAndReturn(QueryType.SQL, sql);
  }

  protected static List<QueryDataBatch> testPhysicalWithResults(String physical) throws Exception{
    return testRunAndReturn(QueryType.PHYSICAL, physical);
  }

  public static List<QueryDataBatch>  testRunAndReturn(QueryType type, Object query) throws Exception{
    if (type == QueryType.PREPARED_STATEMENT) {
      Preconditions.checkArgument(query instanceof PreparedStatementHandle,
          "Expected an instance of PreparedStatement as input query");
      return testPreparedStatement((PreparedStatementHandle)query);
    } else {
      Preconditions.checkArgument(query instanceof String, "Expected a string as input query");
      query = QueryTestUtil.normalizeQuery((String)query);
      return client.runQuery(type, (String)query);
    }
  }

  public static List<QueryDataBatch> testPreparedStatement(PreparedStatementHandle handle) throws Exception {
    return client.executePreparedStatement(handle);
  }

  public static int testRunAndPrint(final QueryType type, final String query) throws Exception {
    return QueryTestUtil.testRunAndPrint(client, type, query);
  }

  protected static void testWithListener(QueryType type, String query, UserResultsListener resultListener) {
    QueryTestUtil.testWithListener(client, type, query, resultListener);
  }

  public static void testNoResult(String query, Object... args) throws Exception {
    testNoResult(1, query, args);
  }

  protected static void testNoResult(int interation, String query, Object... args) throws Exception {
    query = String.format(query, args);
    logger.debug("Running query:\n--------------\n" + query);
    for (int i = 0; i < interation; i++) {
      final List<QueryDataBatch> results = client.runQuery(QueryType.SQL, query);
      for (final QueryDataBatch queryDataBatch : results) {
        queryDataBatch.release();
      }
    }
  }

  public static void test(String query, Object... args) throws Exception {
    QueryTestUtil.test(client, String.format(query, args));
  }

  public static void test(final String query) throws Exception {
    QueryTestUtil.test(client, query);
  }

  // run query with in CTAS (json table) and read CTAS output in bytes[].
  public static FileAttributes testAndGetResult(final String query, final String testName) throws Exception {
    final String tableName = format("%s%d", testName, random.nextInt(100000));
    final String ctasQuery = format("CREATE TABLE dfs_test.%s STORE AS (type => 'json', prettyPrint => false) WITH SINGLE WRITER AS %s", tableName, query);
    final Path tableDir = Path.of(getDfsTestTmpSchemaLocation()).resolve(tableName);
    test(ctasQuery);
      // find file
    try (DirectoryStream<FileAttributes> statuses = localFs.list(tableDir, PathFilters.endsWith(".json"))) {
      return statuses.iterator().next();
    }
  }

  public static AutoCloseable withOption(final BooleanValidator validator, boolean value) throws Exception{
    return withOptionInternal(validator, value, false);
  }

  public static AutoCloseable withSystemOption(final BooleanValidator validator, boolean value) throws Exception{
    return withOptionInternal(validator, value, true);
  }

  public static AutoCloseable withOption(final LongValidator validator, long value) throws Exception{
    return withOptionInternal(validator, value, false);
  }

  public static AutoCloseable withSystemOption(final LongValidator validator, long value) throws Exception{
    return withOptionInternal(validator, value, true);
  }

  public static AutoCloseable withOption(final DoubleValidator validator, double value) throws Exception{
    return withOptionInternal(validator, value, false);
  }

  public static AutoCloseable withSystemOption(final DoubleValidator validator, double value) throws Exception{
    return withOptionInternal(validator, value, true);
  }

  private static AutoCloseable withOption(final StringValidator validator, String value) throws Exception{
    testNoResult(String.format("ALTER SESSION SET %s%s%s = %s%s%s", SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE, SqlUtils.QUOTE, value, SqlUtils.QUOTE));
    return new AutoCloseable(){
      @Override
      public void close() throws Exception {
        testNoResult(String.format("ALTER SESSION RESET %s%s%s", SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE));
      }
    };
  }

  private static AutoCloseable withOptionInternal(final OptionValidator validator, Object value, boolean isSystem) throws Exception{
    final String optionScope = isSystem ? "SYSTEM":"SESSION";
    testNoResult(String.format("ALTER %s SET %s%s%s = %s", optionScope, SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE, value));
    return new AutoCloseable(){
      @Override
      public void close() throws Exception {
        testNoResult(String.format("ALTER %s RESET %s%s%s", optionScope, SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE));
      }
    };
  }

  protected static int testLogical(String query) throws Exception{
    return testRunAndPrint(QueryType.LOGICAL, query);
  }

  protected static int testPhysical(String query) throws Exception{
    return testRunAndPrint(QueryType.PHYSICAL, query);
  }

  protected static int testSql(String query) throws Exception{
    return testRunAndPrint(QueryType.SQL, query);
  }

  protected static int testPhysicalFromFile(String file) throws Exception{
    return testPhysical(getFile(file));
  }

  protected static List<QueryDataBatch> testPhysicalFromFileWithResults(String file) throws Exception {
    return testRunAndReturn(QueryType.PHYSICAL, getFile(file));
  }

  /**
   * Utility method which tests given query produces a {@link UserException} and the exception message contains
   * the given message.
   * @param testSqlQuery Test query
   * @param expectedErrorMsg Expected error message.
   */
  protected static void errorMsgTestHelper(final String testSqlQuery, final String expectedErrorMsg) {
      assertThatThrownBy(() -> test(testSqlQuery))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(expectedErrorMsg);
  }

  /**
   * Utility method which tests given query produces a {@link UserException} with the expected
   * errorType.
   * @param testSqlQuery Test query
   * @param expectedErrorType Expected error type
   */
  protected static void errorTypeTestHelper(final String testSqlQuery, final ErrorType expectedErrorType) {
    errorMsgWithTypeTestHelper(testSqlQuery, expectedErrorType, "");
  }

  /**
   * Utility method which tests given query produces a {@link UserException} with the expected
   * errorType and exception message.
   * @param testSqlQuery Test query
   * @param expectedErrorType Expected error type
   * @param expectedErrorMsg Expected error message.
   */
  protected static void errorMsgWithTypeTestHelper(final String testSqlQuery,
    final ErrorType expectedErrorType,
    final String expectedErrorMsg) {
    ObjectAssert<UserRemoteException> ure = assertThatThrownBy(
      () -> test(testSqlQuery))
      .isInstanceOf(UserRemoteException.class)
      .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class));

    ure.extracting(UserRemoteException::getErrorType)
      .isEqualTo(expectedErrorType);

    ure.extracting(UserRemoteException::getOriginalMessage)
      .asInstanceOf(InstanceOfAssertFactories.STRING)
      .contains(expectedErrorMsg);
  }

  public static String getFile(String resource) throws IOException{
    final URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  public void writeDir(java.nio.file.Path baseDir, java.nio.file.Path dest, String srcDirName) {
    URL resource = Resources.getResource(baseDir.resolve(srcDirName).toString());
    try (Stream<java.nio.file.Path> fileStream = java.nio.file.Files.walk(Paths.get(resource.getPath()))) {
      fileStream.forEach(inputFile -> {
        if (!java.nio.file.Files.isDirectory(inputFile)) {
          writeFile(baseDir, dest, inputFile.getFileName());
        }
      });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void writeFile(java.nio.file.Path baseDir, java.nio.file.Path dest, java.nio.file.Path srcFileName) {
    URL resource = Resources.getResource(baseDir.resolve(dest.getFileName()).resolve(srcFileName).toString());
    try {
      java.nio.file.Files.write(dest.resolve(srcFileName), Resources.toByteArray(resource));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Copy the resource (ex. file on classpath) to a physical file on FileSystem.
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
   * Create a temp parent directory to store the given directory with name {@code dirName}. Does <b>NOT</b> create
   * the directory itself.
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
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  protected static void copyFromJar(String sourceElement, final java.nio.file.Path target) throws URISyntaxException, IOException {
    URI resource = Resources.getResource(sourceElement).toURI();
    java.nio.file.Path srcDir = java.nio.file.Paths.get(resource);
    try (Stream<java.nio.file.Path> stream = java.nio.file.Files.walk(srcDir)) {
      stream.forEach(source -> copy(source, target.resolve(srcDir.relativize(source))));
    }
  }

  protected static void resetSessionOption(final OptionValidator option) {
    resetSessionOption(option.getOptionName());
  }

  protected static void resetSessionOption(final String option) {
    String str = String.format("ALTER SESSION RESET %s%s%s", SqlUtils.QUOTE, option, SqlUtils.QUOTE);
    try {
      runSQL(str);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to run %s", str), e);
    }
  }
  protected static void setSessionOption(final OptionValidator option, final String value) {
    setSessionOption(option.getOptionName(), value);
  }

  protected static void setSessionOption(final String option, final String value) {
    String str = String.format("alter session set %1$s%2$s%1$s = %3$s", SqlUtils.QUOTE, option, value);
    try {
      runSQL(str);
    } catch(final Exception e) {
      fail(String.format("Failed to run %s, Error: %s", str, e.toString()));
    }
  }

  protected static String getSystemOptionQueryString(final String option, final String value) {
    return String.format("alter system set %1$s%2$s%1$s = %3$s", SqlUtils.QUOTE, option, value);
  }

  protected static void setSystemOption(final OptionValidator option, final String value) {
    setSystemOption(option.getOptionName(), value);
  }

  protected static void setSystemOption(final String option, final String value) {
    String str = getSystemOptionQueryString(option, value);
    try {
      runSQL(str);
    } catch(final Exception e) {
      fail(String.format("Failed to run %s, Error: %s", str, e.toString()));
    }
  }

  protected static void resetSystemOption(final String option) {
    String str = String.format("alter system reset %1$s%2$s%1$s", SqlUtils.QUOTE, option);
    try {
      runSQL(str);
    } catch(final Exception e) {
      fail(String.format("Failed to run %s, Error: %s", str, e.toString()));
    }
  }

  protected static AutoCloseable enableHiveAsync() {
    setSystemOption(ExecConstants.ENABLE_HIVE_ASYNC, "true");
    return () ->
            setSystemOption(ExecConstants.ENABLE_HIVE_ASYNC,
                    ExecConstants.ENABLE_HIVE_ASYNC.getDefault().getBoolVal().toString());
  }

  public static AutoCloseable enableIcebergTables() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "true");
    setSystemOption(ExecConstants.CTAS_CAN_USE_ICEBERG, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_PARTITION_TRANSFORMS, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML, "true");
    return () -> {
      setSystemOption(ExecConstants.ENABLE_ICEBERG,
        ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
      setSystemOption(ExecConstants.CTAS_CAN_USE_ICEBERG,
        ExecConstants.CTAS_CAN_USE_ICEBERG.getDefault().getBoolVal().toString());
      setSystemOption(ExecConstants.ENABLE_ICEBERG_PARTITION_TRANSFORMS,
        ExecConstants.ENABLE_ICEBERG_PARTITION_TRANSFORMS.getDefault().getBoolVal().toString());
      setSystemOption(ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL,
        ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL.getDefault().getBoolVal().toString());
      setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML,
        ExecConstants.ENABLE_ICEBERG_ADVANCED_DML.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable disableIcebergFlag() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "false");
    return () -> {
      setSystemOption(ExecConstants.ENABLE_ICEBERG,
        ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable enableIcebergTablesNoCTAS() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "true");
    setSystemOption(ExecConstants.CTAS_CAN_USE_ICEBERG, "false");
    return () -> {
      setSystemOption(ExecConstants.ENABLE_ICEBERG,
        ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
      setSystemOption(ExecConstants.CTAS_CAN_USE_ICEBERG,
        ExecConstants.CTAS_CAN_USE_ICEBERG.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable enableUseSyntax() {
    setSystemOption(ExecConstants.ENABLE_USE_VERSION_SYNTAX, "true");
    return () -> {
      setSystemOption(ExecConstants.ENABLE_USE_VERSION_SYNTAX,
        ExecConstants.ENABLE_USE_VERSION_SYNTAX.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable disablePartitionPruning() {
    setSystemOption(PlannerSettings.ENABLE_PARTITION_PRUNING, "false");
    return () ->
      setSystemOption(PlannerSettings.ENABLE_PARTITION_PRUNING,
        PlannerSettings.ENABLE_PARTITION_PRUNING.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable disableUnlimitedSplitsSupportFlags() {
    setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT, "false");
    return () -> {
      setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT,
              PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable disableUnlimitedSplitsAndIcebergSupportFlags() {
    setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT, "false");
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "false");

    return () -> {
      setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT,
              PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getDefault().getBoolVal().toString());
      setSystemOption(ExecConstants.ENABLE_ICEBERG,
              ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable enableUnlimitedSplitsSupportFlags() {
    setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "true");

    return () -> {
      setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT,
        PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getDefault().getBoolVal().toString());
      setSystemOption(ExecConstants.ENABLE_ICEBERG,
        ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable enableIcebergDmlSupportFlag() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_DML, "true");

    return () -> {
      setSystemOption(ExecConstants.ENABLE_ICEBERG_DML,
        ExecConstants.ENABLE_ICEBERG_DML.getDefault().getBoolVal().toString());
    };
  }

  protected static AutoCloseable enableDeltaLake() {
    setSystemOption(PlannerSettings.ENABLE_DELTALAKE, "true");
    return () ->
      setSystemOption(PlannerSettings.ENABLE_DELTALAKE,
        PlannerSettings.ENABLE_DELTALAKE.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable enableJsonReadNumbersAsDouble() {
    setSystemOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE, "true");
    return () ->
            setSystemOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE,
                    ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable enableJsonAllStrings() {
    setSystemOption(ExecConstants.JSON_ALL_TEXT_MODE, "true");
    return () ->
            setSystemOption(ExecConstants.JSON_ALL_TEXT_MODE,
                    ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable disableExchanges() {
    setSystemOption(PlannerSettings.EXCHANGE, "true");
    return () ->
      setSystemOption(PlannerSettings.EXCHANGE,
        PlannerSettings.EXCHANGE.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable enableINPushDown() {
    setSystemOption(PlannerSettings.ENABLE_PARQUET_IN_EXPRESSION_PUSH_DOWN, "true");
    return () ->
      setSystemOption(PlannerSettings.ENABLE_PARQUET_IN_EXPRESSION_PUSH_DOWN,
        PlannerSettings.ENABLE_PARQUET_IN_EXPRESSION_PUSH_DOWN.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable enableMultipleConditionPushDown() {
    setSystemOption(PlannerSettings.ENABLE_PARQUET_MULTI_COLUMN_FILTER_PUSH_DOWN, "true");
    return () ->
      setSystemOption(PlannerSettings.ENABLE_PARQUET_MULTI_COLUMN_FILTER_PUSH_DOWN,
        PlannerSettings.ENABLE_PARQUET_MULTI_COLUMN_FILTER_PUSH_DOWN.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable treatScanAsBoost() {
    setSystemOption(ExecConstants.ENABLE_BOOSTING, "true");
    return () ->
            setSystemOption(ExecConstants.ENABLE_BOOSTING,
                    ExecConstants.ENABLE_BOOSTING.getDefault().getBoolVal().toString());
  }

  private static AutoCloseable setHiveParquetComplexTypes(String value) {
    setSystemOption(ExecConstants.HIVE_COMPLEXTYPES_ENABLED, value);
    return () ->
      setSystemOption(ExecConstants.HIVE_COMPLEXTYPES_ENABLED,
        ExecConstants.HIVE_COMPLEXTYPES_ENABLED.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable enableHiveParquetComplexTypes() {
    return setHiveParquetComplexTypes("true");
  }

  protected static AutoCloseable enableMapDataType() {
    setSystemOption(ExecConstants.ENABLE_MAP_DATA_TYPE, "true");
    return () ->
      setSystemOption(ExecConstants.ENABLE_MAP_DATA_TYPE,
        ExecConstants.ENABLE_MAP_DATA_TYPE.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable disableHiveParquetComplexTypes() {
    return setHiveParquetComplexTypes("false");
  }

  protected static AutoCloseable enableTableOption(String table, String optionName) throws Exception {
    String setOptionQuery = setTableOptionQuery(table, optionName, "true");
    String unSetOptionQuery = setTableOptionQuery(table, optionName, "false");

    runSQL(setOptionQuery);
    return () ->
            runSQL(unSetOptionQuery);
  }

  protected static AutoCloseable enableTableOptionWithUnlimitedSplitsDisabled(String table, String optionName) throws Exception {
    String setOptionQuery = setTableOptionQuery(table, optionName, "true");
    String unSetOptionQuery = setTableOptionQuery(table, optionName, "false");

    runSQL(setOptionQuery);
    AutoCloseable disableUnlimitedSplits = disableUnlimitedSplitsSupportFlags();
    return AutoCloseables.all(Arrays.asList(() -> runSQL(unSetOptionQuery), disableUnlimitedSplits));
  }

  protected static AutoCloseable setSystemOptionWithAutoReset(final String option, final String value ) {
    setSystemOption(option, value);
    return () ->
      resetSystemOption(option);
  }

  protected static AutoCloseable disableParquetVectorization() {
    setSystemOption(ExecConstants.PARQUET_READER_VECTORIZE, "false");
    return () ->
      setSystemOption(ExecConstants.PARQUET_READER_VECTORIZE,
        ExecConstants.PARQUET_READER_VECTORIZE.getDefault().getBoolVal().toString());
  }

  protected static AutoCloseable setMaxLeafColumns(long newVal) {
    setSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX, String.valueOf(newVal));

    return () -> {
      setSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX,
              CatalogOptions.METADATA_LEAF_COLUMN_MAX.getDefault().getValue().toString());
    };
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


  public static int getRecordCount(List<QueryDataBatch> result) throws UnsupportedEncodingException, SchemaChangeException {
    int i = 0;
    for (QueryDataBatch batch : result) {
      i += batch.getHeader().getRowCount();
      batch.release();
    }
    return i;
  }

  protected int printResult(List<QueryDataBatch> results) throws SchemaChangeException {
    int rowCount = 0;
    final RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(final QueryDataBatch result : results) {
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

  protected String getResultString(List<QueryDataBatch> results, String delimiter) throws SchemaChangeException {
    return getResultString(results, delimiter, true);
  }

  protected String getResultString(List<QueryDataBatch> results, String delimiter, boolean includeHeader)
      throws SchemaChangeException {
    final StringBuilder formattedResults = new StringBuilder();
    final RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(final QueryDataBatch result : results) {
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

  public static boolean compareFiles(FileAttributes f1, FileAttributes f2) throws Exception {
    byte[] original = new byte[(int)f1.size()];
    byte[] withDict = new byte[(int)f2.size()];

    try (FSInputStream in1 = localFs.open(f1.getPath()); FSInputStream in2 = localFs.open(f2.getPath());) {
      IOUtils.readFully(in1, original, 0, original.length);
      IOUtils.readFully(in2, withDict, 0, withDict.length);
    }

    return Arrays.equals(original, withDict);
  }

  public static void disableGlobalDictionary() throws Exception {
    setSessionOption(PlannerSettings.ENABLE_GLOBAL_DICTIONARY, "false");
  }

  public static void enableGlobalDictionary() throws Exception {
    setSessionOption(PlannerSettings.ENABLE_GLOBAL_DICTIONARY, "true");
  }

  public static void validateResults(final String query, final String tag) throws Exception {
    disableGlobalDictionary();
    final FileAttributes original = testAndGetResult(query, tag);
    enableGlobalDictionary();
    FileAttributes withDict = testAndGetResult(query, tag);
    // read file
    final boolean diff = compareFiles(original, withDict);
    if (!diff) {
      fail(format("Results do not match original data: [%s], with global dictionary: [%s]", original.getPath(), withDict.getPath()));
    } else {
      localFs.delete(original.getPath().getParent(), true);
      localFs.delete(withDict.getPath().getParent(), true);
    }
  }

  public static void validateResultsOutOfOrder(final String query, final String tag) throws Exception {
    disableGlobalDictionary();
    final FileAttributes original = testAndGetResult(query, tag);
    enableGlobalDictionary();
    FileAttributes withDict = testAndGetResult(query, tag);
    boolean diff = false;
    final Map<String, Void> lines = new HashMap<>();
    try (BufferedReader in1 = new BufferedReader(new InputStreamReader(localFs.open(original.getPath()), UTF_8))) {
      String line = null;
      while ((line = in1.readLine()) != null) {
        lines.put(line, null);
      }
    }
    try (BufferedReader in2 = new BufferedReader(new InputStreamReader(localFs.open(withDict.getPath()), UTF_8))) {
      String line = null;
      while ((line = in2.readLine()) != null) {
        if(!lines.containsKey(line)) {
          diff = true;
          break;
        }
      }
    }
    if (diff) {
      fail(format("Results do not match original data: [%s], with global dictionary: [%s]", original.getPath(), withDict.getPath()));
    } else {
      localFs.delete(original.getPath().getParent(), true);
      localFs.delete(withDict.getPath().getParent(), true);
    }
  }

  protected static String getValueInFirstRecord(String sql, String columnName)
      throws Exception {
    final List<QueryDataBatch> results = testSqlWithResults(sql);
    final RecordBatchLoader loader = new RecordBatchLoader(getSabotContext().getAllocator());
    final StringBuilder builder = new StringBuilder();
    final boolean silent = config != null && config.getBoolean(QueryTestUtil.TEST_QUERY_PRINTING_SILENT);

    for (final QueryDataBatch b : results) {
      if (!b.hasData()) {
        continue;
      }

      loader.load(b.getHeader().getDef(), b.getData());

      final VectorWrapper<?> vw;
      try {
          vw = loader.getValueAccessorById(
              VarCharVector.class,
              loader.getValueVectorId(SchemaPath.getSimplePath(columnName)).getFieldIds());
      } catch (Throwable t) {
        throw new Exception("Looks like you did not provide an explain plan query, please add EXPLAIN PLAN FOR to the beginning of your query.");
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

  public static void checkFirstRecordContains(String query, String column, String expected) throws Exception {
    assertThat(getValueInFirstRecord(query, column)).contains(expected);
  }

  protected static IcebergModel getIcebergModel(File tableRoot, IcebergCatalogType catalogType) {
    FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    IcebergModel icebergModel = null;
    switch (catalogType) {
      case UNKNOWN:
        break;
      case NESSIE:
        icebergModel = new IcebergNessieModel(DREMIO_NESSIE_DEFAULT_NAMESPACE, new Configuration(),
                getSabotContext().getNessieClientProvider(),
                null, null, new DatasetCatalogGrpcClient(getSabotContext().getDatasetCatalogBlockingStub().get()), fileSystemPlugin);

        when(fileSystemPlugin.getIcebergModel()).thenReturn(icebergModel);
        break;
      case HADOOP:

        icebergModel = new IcebergHadoopModel(new Configuration(), fileSystemPlugin);
        when(fileSystemPlugin.getIcebergModel()).thenReturn(icebergModel);
        break;
    }
    return icebergModel;
  }

  public static Table getIcebergTable(File tableRoot, IcebergCatalogType catalogType) {
    IcebergModel icebergModel = getIcebergModel(tableRoot, catalogType);
    return icebergModel.getIcebergTable(icebergModel.getTableIdentifier(tableRoot.getPath()));
  }

  public static Table getIcebergTable(File tableRoot) {
    return getIcebergTable(tableRoot, IcebergCatalogType.NESSIE);
  }

  protected static String getDfsTestTmpDefaultCtasFormat(String pluginName) {
    final FileSystemPlugin plugin = getSabotContext().getCatalogService().getSource(pluginName);
    return plugin.getDefaultCtasFormat();
  }

  public static FileSystemPlugin getMockedFileSystemPlugin() {
    FileSystemPlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    when(fileSystemPlugin.getHadoopFsSupplier(any(String.class), any(Configuration.class), any(String.class))).
      thenAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            Supplier<org.apache.hadoop.fs.FileSystem> fileSystemSupplier = getFileSystemSupplier(DremioHadoopUtils.toHadoopPath((String) args[0]).toUri(), (Configuration) args[1], (String) args[2]);
            return fileSystemSupplier;
          }
        });

    when(fileSystemPlugin.getHadoopFsSupplier(any(String.class), any(Configuration.class))).
      thenAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            Supplier<org.apache.hadoop.fs.FileSystem> fileSystemSupplier = getFileSystemSupplier(DremioHadoopUtils.toHadoopPath((String) args[0]).toUri(), (Configuration) args[1], SystemUser.SYSTEM_USERNAME);

            return fileSystemSupplier;
          }
        });
    return fileSystemPlugin;
  }

  private static Supplier<org.apache.hadoop.fs.FileSystem> getFileSystemSupplier(final URI uri, final Configuration conf, String user) {
    return () -> {
      try {
        return org.apache.hadoop.fs.FileSystem.get(uri, conf, user);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException();
      }
    };
  }

  public static boolean areAzureStorageG1CredentialsNull(){
    if (System.getenv("AZURE_STORAGE_G1_CLIENT_ID") == null || System.getenv("AZURE_STORAGE_G1_TENANT_ID") == null || System.getenv("AZURE_STORAGE_G1_CLIENT_SECRET") == null){
      return true;
    }
    return false;
  }

  public static boolean areAzureStorageG2CredentialsNull(){
    if (System.getenv("AZURE_STORAGE_G2_CLIENT_ID") == null || System.getenv("AZURE_STORAGE_G2_TENANT_ID") == null || System.getenv("AZURE_STORAGE_G2_CLIENT_SECRET") == null){
      return true;
    }
    return false;
  }

  public static boolean areAzureStorageG2V1CredentialsNull(){
    if(System.getenv("AZURE_STORAGE_G2_V1_ACCOUNT_NAME") == null || System.getenv("AZURE_STORAGE_G2_V1_ACCOUNT_KEY") == null){
      return true;
    }
    return false;
  }

  public static boolean areAzureStorageG2V2CredentialsNull(){
    if(System.getenv("AZURE_STORAGE_G2_V2_ACCOUNT_NAME") == null || System.getenv("AZURE_STORAGE_G2_V2_ACCOUNT_KEY") == null){
      return true;
    }
    return false;
  }

  public static boolean areAzureStorageG2V2NonHierCredentialsNull(){
    if(System.getenv("AZURE_STORAGE_G2_V2_NON_HIER_ACCOUNT_NAME") == null || System.getenv("AZURE_STORAGE_G2_V2_NON_HIER_ACCOUNT_KEY") == null){
      return true;
    }
    return false;
  }
}
