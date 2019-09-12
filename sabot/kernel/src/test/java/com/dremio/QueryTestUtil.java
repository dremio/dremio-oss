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

import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.BaseTestQuery.SilentListener;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.client.PrintingResultsListener;
import com.dremio.exec.client.QuerySubmitter.Format;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.service.coordinator.ClusterCoordinator;

/**
 * Utilities useful for tests that issue SQL queries.
 */
public class QueryTestUtil {

  public static final String TEST_QUERY_PRINTING_SILENT = "dremio.test.query.printing.silent";

  /**
   * Constructor. All methods are static.
   */
  private QueryTestUtil() {
  }

  /**
   * Create a DremioClient that can be used to query a dremio cluster.
   *
   * @param config
   * @param remoteServiceSet remote service set
   * @param maxWidth maximum width per node
   * @param props Connection properties contains properties such as "user", "password", "schema" etc
   * @return the newly created client
   * @throws RpcException if there is a problem setting up the client
   */
  public static DremioClient createClient(final SabotConfig config, final ClusterCoordinator clusterCoordinator,
      final int maxWidth, final Properties props) throws RpcException, OutOfMemoryException {
    final DremioClient dremioClient = new DremioClient(config, clusterCoordinator);
    dremioClient.connect(props);

    final List<QueryDataBatch> results = dremioClient.runQuery(
        QueryType.SQL, String.format("alter session set %1$s%2$s%1$s = %3$d",
            SqlUtils.QUOTE, ExecConstants.MAX_WIDTH_PER_NODE_KEY, maxWidth));
    for (QueryDataBatch queryDataBatch : results) {
      queryDataBatch.release();
    }

    return dremioClient;
  }
  /**
   * Create a DremioClient that can be used to query a dremio cluster.
   * The only distinction between this function and createClient is that
   * the latter runs a query to alter the default parsing behavior to double quotes.
   *
   * @param config
   * @param remoteServiceSet remote service set
   * @param props Connection properties contains properties such as "user", "password", "schema" etc
   * @return the newly created client
   * @throws RpcException if there is a problem setting up the client
   */
  public static DremioClient createCleanClient(final SabotConfig config, final ClusterCoordinator clusterCoordinator,
     final Properties props) throws RpcException, OutOfMemoryException {

    final DremioClient dremioClient = new DremioClient(config, clusterCoordinator);
    dremioClient.connect(props);

    return dremioClient;
  }

  /**
   * Normalize the query relative to the test environment.
   *
   * <p>Looks for "${WORKING_PATH}" in the query string, and replaces it the current
   * working patch obtained from {@link com.dremio.common.util.TestTools#getWorkingPath()}.
   *
   * @param query the query string
   * @return the normalized query string
   */
  public static String normalizeQuery(final String query) {
    if (query.contains("${WORKING_PATH}")) {
      return query.replaceAll(Pattern.quote("${WORKING_PATH}"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
    } else if (query.contains("[WORKING_PATH]")) {
      return query.replaceAll(Pattern.quote("[WORKING_PATH]"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
    }
    return query;
  }

  /**
   * Execute a SQL query, and print the results.
   *
   * @param client Dremio client to use
   * @param type type of the query
   * @param queryString query string
   * @return number of rows returned
   * @throws Exception
   */
  public static int testRunAndPrint(
      final DremioClient client, final QueryType type, final String queryString) throws Exception {
    final String query = normalizeQuery(queryString);
    SabotConfig config = client.getConfig();
    AwaitableUserResultsListener resultListener =
        new AwaitableUserResultsListener(
            config.getBoolean(TEST_QUERY_PRINTING_SILENT) ?
                new SilentListener() :
                new PrintingResultsListener(config, Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH)
        );
    client.runQuery(type, query, resultListener);
    return resultListener.await();
  }

  /**
   * Execute one or more queries separated by semicolons, and print the results.
   *
   * @param client Dremio client to use
   * @param queryString the query string
   * @throws Exception
   */
  public static void test(final DremioClient client, final String queryString) throws Exception{
    final String query = normalizeQuery(queryString);
    String[] queries = query.split(";");
    for (String q : queries) {
      final String trimmedQuery = q.trim();
      if (trimmedQuery.isEmpty()) {
        continue;
      }
      testRunAndPrint(client, QueryType.SQL, trimmedQuery);
    }
  }

  /**
   * Execute one or more queries separated by semicolons, and print the results, with the option to
   * add formatted arguments to the query string.
   *
   * @param client Dremio client to use
   * @param query the query string; may contain formatting specifications to be used by
   *   {@link String#format(String, Object...)}.
   * @param args optional args to use in the formatting call for the query string
   * @throws Exception
   */
  public static void test(final DremioClient client, final String query, Object... args) throws Exception {
    test(client, String.format(query, args));
  }

  /**
   * Execute a single query with a user supplied result listener.
   *
   * @param client Dremio client to use
   * @param type type of query
   * @param queryString the query string
   * @param resultListener the result listener
   */
  public static void testWithListener(final DremioClient client, final QueryType type,
      final String queryString, final UserResultsListener resultListener) {
    final String query = QueryTestUtil.normalizeQuery(queryString);
    client.runQuery(type, query, resultListener);
  }
}
