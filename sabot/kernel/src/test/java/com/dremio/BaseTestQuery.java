/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static java.lang.String.format;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SabotNode;
import com.dremio.options.OptionValidator;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.exec.util.TestUtilities;
import com.dremio.exec.util.VectorUtil;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.BindingCreator;
import com.dremio.service.BindingProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class BaseTestQuery extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  protected static final String TEMP_SCHEMA = "dfs_test";

  protected static final int MAX_WIDTH_PER_NODE = 2;
  private static final Random random = new Random();
  protected static FileSystem localFs;

  protected static Properties defaultProperties = null;

  @SuppressWarnings("serial")
  private static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      put(ExecConstants.HTTP_ENABLE, "false");
    }
  };

  public final TestRule resetWatcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      try {
        resetClientAndBit();
      } catch (Exception e1) {
        throw new RuntimeException("Failure while resetting client.", e1);
      }
    }
  };

  public static final class BinderRule extends ExternalResource {
    private static class Holder<T> {
      private final Class<T> iface;
      private final T impl;

      private Holder(Class<T> iface, T impl) {
        this.iface = iface;
        this.impl = impl;
      }

      void replace(BindingCreator bindingCreator) {
        bindingCreator.replace(iface, impl);
      }
    }
    private final Set<Holder<?>> registry = new HashSet<>();

    @Override
    protected void before() throws Throwable {
      registry.clear();
    }

    public <T> void bind(Class<T> iface, T implementation) {
      registry.add(new Holder<>(iface, implementation));
    }

    private void updateBindingCreator(BindingCreator bindingCreator) {
      for(Holder<?> holder: registry) {
        holder.replace(bindingCreator);
      }
    }
  }

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
              return new SabotNode(config, clusterCoordinator, CLASSPATH_SCAN_RESULT, input.allRoles());
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        };

    private Function<SabotProviderConfig, SabotNode> provider = DEFAULT_PROVIDER;

    @Override
    protected void before() throws Throwable {
      provider = DEFAULT_PROVIDER;
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
  }

  @ClassRule
  public static final BinderRule BINDER_RULE = new BinderRule();

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


  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    config = SabotConfig.create(TEST_CONFIGURATIONS);
    openClient();
    localFs = FileSystem.getLocal(new Configuration());
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

  private static void resetClientAndBit() throws Exception{
    closeClient();
    openClient();
  }

  private static void openClient() throws Exception {
    clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

    nodes = new SabotNode[nodeCount];
    for(int i = 0; i < nodeCount; i++) {
      // first node has all roles, and all others are only executors
      nodes[i] = SABOT_NODE_RULE.newSabotNode(new SabotProviderConfig(i == 0));
      BINDER_RULE.updateBindingCreator(nodes[i].getBindingCreator());
      nodes[i].run();
      if(i == 0) {
        TestUtilities.addDefaultTestPlugins(nodes[i].getContext().getCatalogService(), dfsTestTmpSchemaLocation);
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

  /**
   * Close the current <i>client</i> and open a new client using the given <i>properties</i>. All tests executed
   * after this method call use the new <i>client</i>.
   *
   * @param properties
   */
  public static void updateClient(Properties properties) throws Exception {
    Preconditions.checkState(nodes != null && nodes[0] != null, "Nodes are not setup.");
    if (client != null) {
      client.close();
      client = null;
    }

    client = QueryTestUtil.createClient(config, clusterCoordinator, MAX_WIDTH_PER_NODE, properties);
  }

  /*
   * Close the current <i>client</i> and open a new client for the given user. All tests executed
   * after this method call use the new <i>client</i>.
   * @param user
   */
  public static void updateClient(String user) throws Exception {
    updateClient(user, null);
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
    Collection<NodeEndpoint> endpoints = clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR)
        .getAvailableEndpoints();
    if (endpoints.isEmpty()) {
      return null;
    }

    NodeEndpoint endpoint = endpoints.iterator().next();
    return format("jdbc:dremio:direct=%s:%d", endpoint.getAddress(), endpoint.getUserPort());
  }

  public TestBuilder testBuilder() {
    return new TestBuilder(allocator);
  }

  @AfterClass
  public static void closeClient() throws Exception {
    if (client != null) {
      client.close();
    }

    if (nodes != null) {
      for(final SabotNode bit : nodes) {
        if (bit != null) {
          bit.close();
        }
      }
    }

    if(clusterCoordinator != null) {
      clusterCoordinator.close();
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
  public static FileStatus testAndGetResult(final String query, final String testName) throws Exception {
    final String tableName = format("%s%d", testName, random.nextInt(100000));
    final String ctasQuery = format("CREATE TABLE dfs_test.%s STORE AS (type => 'json', prettyPrint => false) WITH SINGLE WRITER AS %s", tableName, query);
    final Path tableDir = new Path(getDfsTestTmpSchemaLocation(), tableName);
    test(ctasQuery);
      // find file
    final FileStatus fileStatus = localFs.listStatus(tableDir, new GlobFilter("*.json"))[0];
    return fileStatus;
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

  protected static void testLogicalFromFile(String file) throws Exception{
    testLogical(getFile(file));
  }

  protected static void testSqlFromFile(String file) throws Exception{
    test(getFile(file));
  }

  /**
   * Utility method which tests given query produces a {@link UserException} and the exception message contains
   * the given message.
   * @param testSqlQuery Test query
   * @param expectedErrorMsg Expected error message.
   */
  protected static void errorMsgTestHelper(final String testSqlQuery, final String expectedErrorMsg) throws Exception {
    try {
      test(testSqlQuery);
      fail("Expected a UserException when running " + testSqlQuery);
    } catch (final Exception actualException) {
      try {
        Assert.assertThat(actualException.getMessage(), CoreMatchers.containsString(expectedErrorMsg));
      } catch (AssertionError e) {
        e.addSuppressed(actualException);
        throw e;
      }
    }
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
  protected static void errorMsgWithTypeTestHelper(final String testSqlQuery, final ErrorType expectedErrorType,
      final String expectedErrorMsg) {
    try {
      test(testSqlQuery);
      fail("Query expected to fail");
    } catch (Exception ex) {
      Assert.assertTrue("UserRemoteException expected", ex instanceof UserRemoteException);
      UserRemoteException uex = ((UserRemoteException) ex);
      Assert.assertEquals("Invalid ErrorType. Expected " + expectedErrorType + " but got " + uex.getErrorType(),
        expectedErrorType, uex.getErrorType());
      Assert.assertTrue("Expected message to contain " + expectedErrorMsg + " but was "
        + uex.getOriginalMessage() + " instead", uex.getOriginalMessage().contains(expectedErrorMsg));
    }
  }

  public static String getFile(String resource) throws IOException{
    final URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
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
   * Create a temp directory to store the given <i>dirName</i>
   * @param dirName
   * @return Full path including temp parent directory and given directory name.
   */
  public static String getTempDir(final String dirName) {
    final File dir = Files.createTempDir();
    dir.deleteOnExit();

    return dir.getAbsolutePath() + File.separator + dirName;
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

  public static int getRecordCount(List<QueryDataBatch> result) throws UnsupportedEncodingException, SchemaChangeException {
    int i = 0;
    for (QueryDataBatch batch : result) {
      i += batch.getHeader().getRowCount();
      batch.release();
    }
    return i;
  }

  protected void setColumnWidth(int columnWidth) {
    this.columnWidths = new int[] { columnWidth };
  }

  protected void setColumnWidths(int[] columnWidths) {
    this.columnWidths = columnWidths;
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

  public static boolean compareFiles(FileStatus f1, FileStatus f2) throws Exception {
    byte[] original = new byte[(int)f1.getLen()];
    byte[] withDict = new byte[(int)f2.getLen()];

    try (FSDataInputStream in1 = localFs.open(f1.getPath()); FSDataInputStream in2 = localFs.open(f2.getPath());) {
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
    final FileStatus original = testAndGetResult(query, tag);
    enableGlobalDictionary();
    FileStatus withDict = testAndGetResult(query, tag);
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
    final FileStatus original = testAndGetResult(query, tag);
    enableGlobalDictionary();
    FileStatus withDict = testAndGetResult(query, tag);
    boolean diff = false;
    final HashMap<String, Void> lines = new HashMap<>();
    try (FSDataInputStream in1 = localFs.open(original.getPath())) {
      String line = null;
      while ((line = in1.readLine()) != null) {
        lines.put(line, null);
      }
    }
    try (FSDataInputStream in2 = localFs.open(withDict.getPath())) {
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
}
