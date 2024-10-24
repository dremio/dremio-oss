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

import com.dremio.BaseTestQuery.SilentListener;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.client.PrintingResultsListener;
import com.dremio.exec.client.QuerySubmitter.Format;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.util.VectorUtil;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.coordinator.ClusterCoordinator;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.memory.OutOfMemoryException;

/** Utilities useful for tests that issue SQL queries. */
public class QueryTestUtil {

  public static final String TEST_QUERY_PRINTING_SILENT = "dremio.test.query.printing.silent";

  /** Constructor. All methods are static. */
  private QueryTestUtil() {}

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
  public static DremioClient createClient(
      final SabotConfig config,
      final ClusterCoordinator clusterCoordinator,
      final int maxWidth,
      Properties props)
      throws RpcException, OutOfMemoryException {
    final DremioClient dremioClient = new DremioClient(config, clusterCoordinator);

    dremioClient.connect(checkAndAddAnonymousUser(props));

    final List<QueryDataBatch> results =
        dremioClient.runQuery(
            QueryType.SQL,
            String.format(
                "alter session set %1$s%2$s%1$s = %3$d",
                SqlUtils.QUOTE, GroupResourceInformation.MAX_WIDTH_PER_NODE_KEY, maxWidth));
    for (QueryDataBatch queryDataBatch : results) {
      queryDataBatch.release();
    }

    return dremioClient;
  }

  /**
   * Checks if a user is present, and if not adds an anonymous user to the given properties.
   *
   * @param props The properties to check for a user.
   * @return Properties guaranteed to have either a user, or an anonymous user.
   */
  public static Properties checkAndAddAnonymousUser(Properties props) {
    if (null == props) {
      props =
          new Properties() {
            {
              put(UserSession.USER, "anonymous");
            }
          };
    } else if (null == props.getProperty(UserSession.USER)) {
      // Detect a null username and replace with anonymous to ensure tests that don't specify
      // username work.
      props.put(UserSession.USER, "anonymous");
    }
    return props;
  }

  /**
   * Create a DremioClient that can be used to query a dremio cluster. The only distinction between
   * this function and createClient is that the latter runs a query to alter the default parsing
   * behavior to double quotes.
   *
   * @param config
   * @param remoteServiceSet remote service set
   * @param props Connection properties contains properties such as "user", "password", "schema" etc
   * @return the newly created client
   * @throws RpcException if there is a problem setting up the client
   */
  public static DremioClient createCleanClient(
      final SabotConfig config, final ClusterCoordinator clusterCoordinator, final Properties props)
      throws RpcException, OutOfMemoryException {

    final DremioClient dremioClient = new DremioClient(config, clusterCoordinator);
    dremioClient.connect(props);

    return dremioClient;
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
    SabotConfig config = client.getConfig();
    AwaitableUserResultsListener resultListener =
        new AwaitableUserResultsListener(
            config.getBoolean(TEST_QUERY_PRINTING_SILENT)
                ? new SilentListener()
                : new PrintingResultsListener(config, Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH));
    testWithListener(client, type, queryString, resultListener);
    return resultListener.await();
  }

  /**
   * Execute one or more queries separated by semicolons, and print the results.
   *
   * @param client Dremio client to use
   * @param queryString the query string
   * @throws Exception
   */
  public static void test(final DremioClient client, final String queryString) throws Exception {
    String[] queries = queryString.split(";");
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
   * @param query the query string; may contain formatting specifications to be used by {@link
   *     String#format(String, Object...)}.
   * @param args optional args to use in the formatting call for the query string
   * @throws Exception
   */
  public static void test(final DremioClient client, final String query, Object... args)
      throws Exception {
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
  public static void testWithListener(
      final DremioClient client,
      final QueryType type,
      final String queryString,
      final UserResultsListener resultListener) {
    final String query = TestTools.replaceWorkingPathPlaceholders(queryString);
    client.runQuery(type, query, resultListener);
  }
}
