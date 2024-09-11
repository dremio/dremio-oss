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
package com.dremio.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserProtos.QueryPlanFragments;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

/** Class to test different planning use cases (separate from query execution) */
public class DremioSeparatePlanningTest extends BaseTestQuery {

  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioSeparatePlanningTest.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  // final String query = "SELECT sales_city, COUNT(*) cnt FROM cp.\"region.json\" GROUP BY
  // sales_city";
  // final String query = "SELECT * FROM cp.\"employee.json\" where  employee_id > 1 and
  // employee_id < 1000";
  // final String query = "SELECT o_orderkey, o_custkey FROM dfs.tmp.\"multilevel\" where dir0 =
  // 1995 and o_orderkey > 100 and o_orderkey < 1000 limit 5";
  // final String query = "SELECT sum(o_totalprice) FROM dfs.tmp.\"multilevel\" where dir0 = 1995
  // and o_orderkey > 100 and o_orderkey < 1000";
  // final String query = "SELECT o_orderkey FROM dfs.tmp.\"multilevel\" order by o_orderkey";
  // final String query = "SELECT dir1, sum(o_totalprice) FROM dfs.tmp.\"multilevel\" where dir0 =
  // 1995 group by dir1 order by dir1";
  // final String query = String.format("SELECT dir0, sum(o_totalprice) FROM
  // dfs.\"%s/multilevel/json\" group by dir0 order by dir0", TEST_RES_PATH);

  @Ignore("DX-4181")
  @Test(timeout = 30000)
  public void testSingleFragmentQuery() throws Exception {
    final String query =
        "SELECT * FROM cp.\"employee.json\" where  employee_id > 1 and  employee_id < 1000";

    QueryPlanFragments planFragments = getFragmentsHelper(query);

    assertNotNull(planFragments);

    PlanFragmentSet set = planFragments.getFragmentSet();
    assertEquals(1, set.getMinorCount());
    assertTrue(set.getMajor(0).getLeafFragment());

    getResultsHelper(planFragments);
  }

  @Ignore("DX-4181")
  @Test(timeout = 30000)
  public void testMultiMinorFragmentSimpleQuery() throws Exception {
    final String query =
        String.format("SELECT o_orderkey FROM dfs.\"%s/multilevel/json\"", TEST_RES_PATH);

    QueryPlanFragments planFragments = getFragmentsHelper(query);

    assertNotNull(planFragments);

    PlanFragmentSet set = planFragments.getFragmentSet();
    assertTrue((set.getMinorCount() > 1));

    for (PlanFragmentMajor planFragment : set.getMajorList()) {
      assertTrue(planFragment.getLeafFragment());
    }

    getResultsHelper(planFragments);
  }

  @Ignore("DX-4181")
  @Test(timeout = 30000)
  public void testMultiMinorFragmentComplexQuery() throws Exception {
    final String query =
        String.format(
            "SELECT dir0, sum(o_totalprice) FROM dfs.\"%s/multilevel/json\" group by dir0 order by dir0",
            TEST_RES_PATH);

    QueryPlanFragments planFragments = getFragmentsHelper(query);

    assertNotNull(planFragments);

    PlanFragmentSet set = planFragments.getFragmentSet();
    assertTrue((set.getMinorCount() > 1));

    for (PlanFragmentMajor planFragment : set.getMajorList()) {
      assertTrue(planFragment.getLeafFragment());
    }

    getResultsHelper(planFragments);
  }

  private QueryPlanFragments getFragmentsHelper(final String query)
      throws InterruptedException, ExecutionException, RpcException {
    updateTestCluster(2, config);

    List<QueryDataBatch> results =
        client.runQuery(QueryType.SQL, "alter session set \"planner.slice_target\"=1");
    for (QueryDataBatch batch : results) {
      batch.release();
    }

    RpcFuture<QueryPlanFragments> queryFragmentsFutures =
        client.planQuery(QueryType.SQL, query, true);

    final QueryPlanFragments planFragments = queryFragmentsFutures.get();

    PlanFragmentSet set = planFragments.getFragmentSet();
    for (PlanFragmentMajor fragment : set.getMajorList()) {
      try {
        System.out.println(
            PhysicalPlanReader.toString(fragment.getFragmentJson(), fragment.getFragmentCodec()));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return planFragments;
  }

  private void getResultsHelper(final QueryPlanFragments planFragments) throws Exception {
    PlanFragmentSet set = planFragments.getFragmentSet();
    List<PlanFragmentFull> fullFragments = new ArrayList<>();

    // Create a map of major fragments.
    Map<Integer, PlanFragmentMajor> map =
        FluentIterable.from(set.getMajorList())
            .uniqueIndex(major -> major.getHandle().getMajorFragmentId());

    // Build the full fragments.
    set.getMinorList()
        .forEach(
            minor -> {
              PlanFragmentMajor major = map.get(minor.getMajorFragmentId());

              fullFragments.add(new PlanFragmentFull(major, minor));
            });

    for (PlanFragmentFull fragment : fullFragments) {
      NodeEndpoint assignedNode = fragment.getAssignment();
      DremioClient fragmentClient = new DremioClient(true);
      Properties props = new Properties();
      props.setProperty("direct", assignedNode.getAddress() + ":" + assignedNode.getUserPort());
      fragmentClient.connect(props);

      ShowResultsUserResultsListener myListener =
          new ShowResultsUserResultsListener(getTestAllocator());
      AwaitableUserResultsListener listenerBits = new AwaitableUserResultsListener(myListener);
      fragmentClient.runQuery(
          QueryType.SQL,
          "select hostname, user_port from sys.nodes where \"current\"=true",
          listenerBits);
      int row = listenerBits.await();
      assertEquals(1, row);
      List<Map<String, String>> records = myListener.getRecords();
      assertEquals(1, records.size());
      Map<String, String> rec = records.get(0);
      assertEquals(2, rec.size());
      Iterator<Entry<String, String>> iter = rec.entrySet().iterator();
      Entry<String, String> entry;
      String host = null;
      String port = null;
      for (int i = 0; i < 2; i++) {
        entry = iter.next();
        if (entry.getKey().equalsIgnoreCase("hostname")) {
          host = entry.getValue();
        } else if (entry.getKey().equalsIgnoreCase("user_port")) {
          port = entry.getValue();
        } else {
          fail("Unknown field: " + entry.getKey());
        }
      }
      assertTrue(props.getProperty("direct").equalsIgnoreCase(host + ":" + port));

      PlanFragmentSet.Builder setSingleBuilder = PlanFragmentSet.newBuilder();
      setSingleBuilder.addMajor(fragment.getMajor());
      setSingleBuilder.addMinor(fragment.getMinor());
      setSingleBuilder.addAllEndpointsIndex(set.getEndpointsIndexList());

      // AwaitableUserResultsListener listener =
      //     new AwaitableUserResultsListener(new PrintingResultsListener(client.getConfig(),
      // Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH));
      AwaitableUserResultsListener listener =
          new AwaitableUserResultsListener(new SilentListener());
      fragmentClient.runQuery(QueryType.EXECUTION, setSingleBuilder.build(), listener);
      int rows = listener.await();
      fragmentClient.close();
    }
  }

  private void getCombinedResultsHelper(final QueryPlanFragments planFragments) throws Exception {
    ShowResultsUserResultsListener myListener =
        new ShowResultsUserResultsListener(getTestAllocator());
    AwaitableUserResultsListener listenerBits = new AwaitableUserResultsListener(myListener);

    // AwaitableUserResultsListener listener =
    //     new AwaitableUserResultsListener(new PrintingResultsListener(client.getConfig(),
    // Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH));
    AwaitableUserResultsListener listener = new AwaitableUserResultsListener(new SilentListener());
    client.runQuery(QueryType.EXECUTION, planFragments.getFragmentSet(), listener);
    int rows = listener.await();
  }

  /** Helper class to get results */
  static class ShowResultsUserResultsListener implements UserResultsListener {

    private QueryId queryId;
    private final RecordBatchLoader loader;
    private final BufferAllocator allocator;
    private UserException ex;
    private List<Map<String, String>> records = Lists.newArrayList();

    public ShowResultsUserResultsListener(BufferAllocator allocator) {
      this.loader = new RecordBatchLoader(allocator);
      this.allocator = allocator;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public List<Map<String, String>> getRecords() {
      return records;
    }

    public UserException getEx() {
      return ex;
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public void submissionFailed(UserException ex) {
      AutoCloseables.closeNoChecked(allocator);
      this.ex = ex;
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      QueryData queryHeader = result.getHeader();
      int rows = queryHeader.getRowCount();
      try {
        if (result.hasData()) {
          ArrowBuf data = result.getData();
          loader.load(queryHeader.getDef(), data);
          for (int i = 0; i < rows; i++) {
            Map<String, String> rec = Maps.newHashMap();
            for (VectorWrapper<?> vw : loader) {
              final String field = vw.getValueVector().getField().getName();
              final ValueVector vv = vw.getValueVector();
              final Object value = i < vv.getValueCount() ? vv.getObject(i) : null;
              final String display = value == null ? null : value.toString();
              rec.put(field, display);
            }
            records.add(rec);
          }
          loader.clear();
        }
        result.release();
      } catch (SchemaChangeException e) {
        fail(e.getMessage());
      }
    }

    @Override
    public void queryCompleted(QueryState state) {}
  }
}
