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
package com.dremio.exec.physical.impl;

import static com.dremio.exec.planner.physical.HashPrelUtil.HASH_EXPR_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.maestro.AbstractMaestroObserver;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.config.HashToRandomExchange;
import com.dremio.exec.physical.config.UnorderedDeMuxExchange;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.fragment.PlanFragmentsIndex;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.SimpleParallelizer;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.util.Utilities;
import com.dremio.options.OptionList;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.execselector.ExecutorSelectionContext;
import com.dremio.service.execselector.ExecutorSelectionHandle;
import com.dremio.service.execselector.ExecutorSelectionHandleImpl;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * This test starts a Drill cluster with CLUSTER_SIZE nodes and generates data for test tables.
 *
 * Tests queries involve HashToRandomExchange (group by and join) and test the following.
 *   1. Plan that has mux and demux exchanges inserted
 *   2. Run the query and check the output record count
 *   3. Take the plan we got in (1), use SimpleParallelizer to get PlanFragments and test that the number of
 *   partition senders in a major fragment is not more than the number of SabotNode nodes in cluster and there exists
 *   at most one partition sender per SabotNode.
 */
@Ignore("DX-3475")
public class TestLocalExchange extends PlanTestBase {

  public static TemporaryFolder testTempFolder = new TemporaryFolder();

  private final static int CLUSTER_SIZE = 3;
  private final static String MUX_EXCHANGE = "\"unordered-mux-exchange\"";
  private final static String DEMUX_EXCHANGE = "\"unordered-demux-exchange\"";
  private final static String MUX_EXCHANGE_CONST = "unordered-mux-exchange";
  private final static String DEMUX_EXCHANGE_CONST = "unordered-demux-exchange";
  private static final String HASH_EXCHANGE = "hash-to-random-exchange";
  private final static UserSession USER_SESSION = UserSession.Builder.newBuilder()
    .withSessionOptionManager(
      new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
      getSabotContext().getOptionManager())
    .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
      .build();

  private SimpleParallelizer PARALLELIZER;

  private final static int NUM_DEPTS = 40;
  private final static int NUM_EMPLOYEES = 1000;
  private final static int NUM_MNGRS = 1;
  private final static int NUM_IDS = 1;

  private static String empTableLocation;
  private static String deptTableLocation;

  private static String groupByQuery;
  private static String joinQuery;

  private static String[] joinQueryBaselineColumns;
  private static String[] groupByQueryBaselineColumns;

  private static List<Object[]> groupByQueryBaselineValues;
  private static List<Object[]> joinQueryBaselineValues;

  @BeforeClass
  public static void setupClusterSize() {
    updateTestCluster(CLUSTER_SIZE, null);
  }

  @BeforeClass
  public static void setupTempFolder() throws IOException {
    testTempFolder.create();
  }

  /**
   * Generate data for two tables. Each table consists of several JSON files.
   */
  @BeforeClass
  public static void generateTestDataAndQueries() throws Exception {
    // Table 1 consists of two columns "emp_id", "emp_name" and "dept_id"
    empTableLocation = testTempFolder.newFolder().getAbsolutePath();

    // Write 100 records for each new file
    final int empNumRecsPerFile = 100;
    for(int fileIndex=0; fileIndex<NUM_EMPLOYEES/empNumRecsPerFile; fileIndex++) {
      File file = new File(empTableLocation + File.separator + fileIndex + ".json");
      PrintWriter printWriter = new PrintWriter(file);
      for (int recordIndex = fileIndex*empNumRecsPerFile; recordIndex < (fileIndex+1)*empNumRecsPerFile; recordIndex++) {
        String record = String.format("{ \"emp_id\" : %d, \"emp_name\" : \"Employee %d\", \"dept_id\" : %d, \"mng_id\" : %d, \"some_id\" : %d }",
            recordIndex, recordIndex, recordIndex % NUM_DEPTS, recordIndex % NUM_MNGRS, recordIndex % NUM_IDS);
        printWriter.println(record);
      }
      printWriter.close();
    }

    // Table 2 consists of two columns "dept_id" and "dept_name"
    deptTableLocation = testTempFolder.newFolder().getAbsolutePath();

    // Write 4 records for each new file
    final int deptNumRecsPerFile = 4;
    for(int fileIndex=0; fileIndex<NUM_DEPTS/deptNumRecsPerFile; fileIndex++) {
      File file = new File(deptTableLocation + File.separator + fileIndex + ".json");
      PrintWriter printWriter = new PrintWriter(file);
      for (int recordIndex = fileIndex*deptNumRecsPerFile; recordIndex < (fileIndex+1)*deptNumRecsPerFile; recordIndex++) {
        String record = String.format("{ \"dept_id\" : %d, \"dept_name\" : \"Department %d\" }",
            recordIndex, recordIndex);
        printWriter.println(record);
      }
      printWriter.close();
    }

    // Initialize test queries
    groupByQuery = String.format("SELECT dept_id, count(*) as numEmployees FROM dfs.\"%s\" GROUP BY dept_id", empTableLocation);
    joinQuery = String.format("SELECT e.emp_name, d.dept_name FROM dfs.\"%s\" e JOIN dfs.\"%s\" d ON e.dept_id = d.dept_id",
        empTableLocation, deptTableLocation);

    // Generate and store output data for test queries. Used when verifying the output of queries ran using different
    // configurations.

    groupByQueryBaselineColumns = new String[] { "dept_id", "numEmployees" };

    groupByQueryBaselineValues = Lists.newArrayList();
    // group Id is generated based on expression 'recordIndex % NUM_DEPTS' above. 'recordIndex' runs from 0 to
    // NUM_EMPLOYEES, so we expect each number of occurrance of each dept_id to be NUM_EMPLOYEES/NUM_DEPTS (1000/40 =
    // 25)
    final int numOccurrances = NUM_EMPLOYEES/NUM_DEPTS;
    for(int i = 0; i < NUM_DEPTS; i++) {
      groupByQueryBaselineValues.add(new Object[] { (long)i, (long)numOccurrances});
    }

    joinQueryBaselineColumns = new String[] { "emp_name", "dept_name" };

    joinQueryBaselineValues = Lists.newArrayList();
    for(int i = 0; i < NUM_EMPLOYEES; i++) {
      final String employee = String.format("Employee %d", i);
      final String dept = String.format("Department %d", i % NUM_DEPTS);
      joinQueryBaselineValues.add(new String[] { employee, dept });
    }
  }

  @Before
  public void initParallelizer() {
    PARALLELIZER = new SimpleParallelizer(
        1 /*parallelizationThreshold (slice_count)*/,
        6 /*maxWidthPerNode*/,
        1000 /*maxGlobalWidth*/,
        1.2, /*affinityFactor*/
        AbstractMaestroObserver.NOOP,
        true,
        1.5d,
        false);
  }

  public static void setupHelper(boolean isMuxOn, boolean isDeMuxOn) throws Exception {
    // set slice count to 1, so that we can have more parallelization for testing
    test("ALTER SESSION SET \"planner.slice_target\"=1");
    // disable the broadcast join to produce plans with HashToRandomExchanges.
    test("ALTER SESSION SET \"planner.enable_broadcast_join\"=false");
    test("ALTER SESSION SET \"planner.enable_mux_exchange\"=" + isMuxOn);
    test("ALTER SESSION SET \"planner.enable_demux_exchange\"=" + isDeMuxOn);
  }

  @Test
  public void testGroupByMultiFields() throws Exception {
    // Test multifield hash generation

    test("ALTER SESSION SET \"planner.slice_target\"=1");
    test("ALTER SESSION SET \"planner.enable_mux_exchange\"=" + true);
    test("ALTER SESSION SET \"planner.enable_demux_exchange\"=" + false);

    final String groupByMultipleQuery = String.format("SELECT dept_id, mng_id, some_id, count(*) as numEmployees FROM dfs.\"%s\" e GROUP BY dept_id, mng_id, some_id", empTableLocation);
    final String[] groupByMultipleQueryBaselineColumns = new String[] { "dept_id", "mng_id", "some_id", "numEmployees" };

    final int numOccurrances = NUM_EMPLOYEES/NUM_DEPTS;

    final String plan = getPlanInString("EXPLAIN PLAN FOR " + groupByMultipleQuery, JSON_FORMAT);

    jsonExchangeOrderChecker(plan, false, 1, "hash32asdouble\\(.*, hash32asdouble\\(.*, hash32asdouble\\(.*\\) \\) \\) ");

    // Run the query and verify the output
    final TestBuilder testBuilder = testBuilder()
        .sqlQuery(groupByMultipleQuery)
        .unOrdered()
        .baselineColumns(groupByMultipleQueryBaselineColumns);

    for(int i = 0; i < NUM_DEPTS; i++) {
      testBuilder.baselineValues(new Object[] { (long)i, (long)0, (long)0, (long)numOccurrances});
    }

    testBuilder.go();
  }

  @Test
  public void testGroupBy_NoMux_NoDeMux() throws Exception {
    testGroupByHelper(false, false);
  }

  @Test
  public void testJoin_NoMux_NoDeMux() throws Exception {
    testJoinHelper(false, false);
  }

  @Test
  public void testGroupBy_Mux_NoDeMux() throws Exception {
    testGroupByHelper(true, false);
  }

  @Test
  public void testJoin_Mux_NoDeMux() throws Exception {
    testJoinHelper(true, false);
  }

  @Test
  public void testGroupBy_NoMux_DeMux() throws Exception {
    testGroupByHelper(false, true);
  }

  @Test
  public void testJoin_NoMux_DeMux() throws Exception {
    testJoinHelper(false, true);
  }

  @Test
  public void testGroupBy_Mux_DeMux() throws Exception {
    testGroupByHelper(true, true);
  }

  @Test
  public void testJoin_Mux_DeMux() throws Exception {
    testJoinHelper(true, true);
  }

  private void testGroupByHelper(boolean isMuxOn, boolean isDeMuxOn) throws Exception {
    testHelper(isMuxOn, isDeMuxOn, groupByQuery,
        isMuxOn ? 1 : 0, isDeMuxOn ? 1 : 0,
        groupByQueryBaselineColumns, groupByQueryBaselineValues);
  }

  public void testJoinHelper(boolean isMuxOn, boolean isDeMuxOn) throws Exception {
    testHelper(isMuxOn, isDeMuxOn, joinQuery,
        isMuxOn ? 2 : 0, isDeMuxOn ? 2 : 0,
        joinQueryBaselineColumns, joinQueryBaselineValues);
  }

  private void testHelper(boolean isMuxOn, boolean isDeMuxOn, String query,
      int expectedNumMuxes, int expectedNumDeMuxes, String[] baselineColumns, List<Object[]> baselineValues)
      throws Exception {
    setupHelper(isMuxOn, isDeMuxOn);

    String plan = getPlanInString("EXPLAIN PLAN FOR " + query, JSON_FORMAT);
    System.out.println("Plan: " + plan);

    if ( isMuxOn ) {
      // # of hash exchanges should be = # of mux exchanges + # of demux exchanges
      assertEquals("HashExpr on the hash column should not happen", 2*expectedNumMuxes+expectedNumDeMuxes, StringUtils.countMatches(plan, HASH_EXPR_NAME));
      jsonExchangeOrderChecker(plan, isDeMuxOn, expectedNumMuxes, "hash32asdouble\\(.*\\) ");
    } else {
      assertEquals("HashExpr on the hash column should not happen", 0, StringUtils.countMatches(plan, HASH_EXPR_NAME));
    }

    // Make sure the plan has mux and demux exchanges (TODO: currently testing is rudimentary,
    // need to move it to sophisticated testing once we have better planning test tools are available)
    assertEquals("Wrong number of MuxExchanges are present in the plan",
        expectedNumMuxes, StringUtils.countMatches(plan, MUX_EXCHANGE));

    assertEquals("Wrong number of DeMuxExchanges are present in the plan",
        expectedNumDeMuxes, StringUtils.countMatches(plan, DEMUX_EXCHANGE));

    // Run the query and verify the output
    TestBuilder testBuilder = testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(baselineColumns);

    for(Object[] baselineRecord : baselineValues) {
      testBuilder.baselineValues(baselineRecord);
    }

    testBuilder.go();

    testHelperVerifyPartitionSenderParallelization(plan, isMuxOn, isDeMuxOn);
  }

  private static void jsonExchangeOrderChecker(String plan, boolean isDemuxEnabled, int expectedNumMuxes, String hashExprPattern) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode planObj = (ObjectNode) mapper.readTree(plan);
    assertNotNull("Corrupted query plan: null", planObj);
    final ArrayNode graphArray = (ArrayNode) planObj.get("graph");
    assertNotNull("No graph array present", graphArray);
    int i = 0;
    int k = 0;
    int prevExprsArraySize = 0;
    boolean foundExpr = false;
    int muxesCount = 0;
    for (JsonNode object : graphArray) {
      final ObjectNode popObj = (ObjectNode) object;
      if ( popObj.has("pop") && popObj.get("pop").asText().equals("project")) {
        if ( popObj.has("exprs")) {
          final ArrayNode exprsArray = (ArrayNode) popObj.get("exprs");
          for (JsonNode exprObj : exprsArray) {
            final ArrayNode expr = (ArrayNode) exprObj;
            if ( expr.has("ref") && expr.get("ref").asText().equals("`"+ HASH_EXPR_NAME +"`")) {
              // found a match. Let's see if next one is the one we need
              final String hashField = expr.get("expr").asText();
              assertNotNull("HashExpr field can not be null", hashField);
              assertTrue("HashExpr field does not match pattern",hashField.matches(hashExprPattern));
              k = i;
              foundExpr = true;
              muxesCount++;
              break;
            }
          }
          if ( foundExpr ) {
            // will be reset to prevExprsArraySize-1 on the last project of the whole stanza
            prevExprsArraySize = exprsArray.size();
          }
        }
      }
      if ( !foundExpr ) {
        continue;
      }
      // next after project with hashexpr
      if ( k == i-1) {
        assertTrue("UnorderedMux should follow Project with HashExpr",
            popObj.has("pop") && popObj.get("pop").asText().equals(MUX_EXCHANGE_CONST));
      }
      if ( k == i-2) {
        assertTrue("HashToRandomExchange should follow UnorderedMux which should follow Project with HashExpr",
            popObj.has("pop") && popObj.get("pop").asText().equals(HASH_EXCHANGE));
        // is HashToRandom is using HashExpr
        assertTrue("HashToRandomExchnage should use hashExpr",
            popObj.has("expr") && popObj.get("expr").asText().equals("`"+ HASH_EXPR_NAME +"`"));
      }
      // if Demux is enabled it also should use HashExpr
      if ( isDemuxEnabled && k == i-3) {
        assertTrue("UnorderdDemuxExchange should follow HashToRandomExchange",
            popObj.has("pop") && popObj.get("pop").equals(DEMUX_EXCHANGE_CONST));
        // is HashToRandom is using HashExpr
        assertTrue("UnorderdDemuxExchange should use hashExpr",
            popObj.has("expr") && popObj.get("expr").equals("`"+HASH_EXPR_NAME +"`"));
      }
      if ( (isDemuxEnabled && k == i-4) || (!isDemuxEnabled && k == i-3) ) {
        // it should be a project without hashexpr, check if number of exprs is 1 less then in first project
        assertTrue("Should be project without hashexpr", popObj.has("pop") && popObj.get("pop").asText().equals("project"));
        final ArrayNode exprsArray = (ArrayNode) popObj.get("exprs");
        assertNotNull("Project should have some fields", exprsArray);
        assertEquals("Number of fields in closing project should be one less then in starting project",
            prevExprsArraySize, exprsArray.size());

        // Now let's reset all the counters, flags if we are going to have another batch of those exchanges
        k = 0;
        foundExpr = false;
        prevExprsArraySize = 0;
      }
      i++;
    }
    assertEquals("Number of Project/Mux/HashExchange/... ", expectedNumMuxes, muxesCount);
  }

  // Verify the number of partition senders in a major fragments is not more than the cluster size and each endpoint
  // in the cluster has at most one fragment from a given major fragment that has the partition sender.
  private void testHelperVerifyPartitionSenderParallelization(
      String plan, boolean isMuxOn, boolean isDeMuxOn) throws Exception {

    final SabotContext sabotContext = getSabotContext();
    final PhysicalPlanReader planReader = sabotContext.getPlanReader();
    final Fragment rootFragment = PopUnitTestBase.getRootFragmentFromPlanString(planReader, plan);

    final List<Integer> deMuxFragments = Lists.newLinkedList();
    final List<Integer> htrFragments = Lists.newLinkedList();
    final PlanningSet planningSet = new PlanningSet();

    // Create a planningSet to get the assignment of major fragment ids to fragments.
    PARALLELIZER.initFragmentWrappers(rootFragment, planningSet);
    PARALLELIZER.setExecutorSelectionService(new ExecutorSelectionService() {
      @Override
      public ExecutorSelectionHandle getExecutors(int desiredNumExecutors, ExecutorSelectionContext executorSelectionContext) {
        return new ExecutorSelectionHandleImpl(sabotContext.getExecutors());
      }

      @Override
      public ExecutorSelectionHandle getAllActiveExecutors(ExecutorSelectionContext executorSelectionContext) {
        return new ExecutorSelectionHandleImpl(sabotContext.getExecutors());
      }

      @Override
      public void start() throws Exception {
      }

      @Override
      public void close() throws Exception {
      }
    });

    findFragmentsWithPartitionSender(rootFragment, planningSet, deMuxFragments, htrFragments);

    final QueryContextInformation queryContextInfo = Utilities.createQueryContextInfo("dummySchemaName");
    List<PlanFragmentFull> fragments = PARALLELIZER.getFragments(new OptionList(), sabotContext.getEndpoint(),
        QueryId.getDefaultInstance(),
        planReader, rootFragment,
        new PlanFragmentsIndex.Builder(),
        USER_SESSION, queryContextInfo, Mockito.mock(FunctionLookupContext.class));

    // Make sure the number of minor fragments with HashPartitioner within a major fragment is not more than the
    // number of SabotNodes in cluster
    ArrayListMultimap<Integer, NodeEndpoint> partitionSenderMap = ArrayListMultimap.create();
    for(PlanFragmentFull planFragment : fragments) {
      // Our parallelizer doesn't compress fragments
      if (planFragment.getMajor().getFragmentJson().toStringUtf8().contains("hash-partition-sender")) {
        int majorFragmentId = planFragment.getHandle().getMajorFragmentId();
        NodeEndpoint assignedEndpoint = planFragment.getAssignment();
        partitionSenderMap.get(majorFragmentId).add(assignedEndpoint);
      }
    }

    if (isMuxOn) {
      verifyAssignment(htrFragments, partitionSenderMap);
    }

    if (isDeMuxOn) {
      verifyAssignment(deMuxFragments, partitionSenderMap);
    }
  }

  /**
   * Helper method to find the major fragment ids of fragments that have PartitionSender.
   * A fragment can have PartitionSender if sending exchange of the current fragment is a
   *   1. DeMux Exchange -> goes in deMuxFragments
   *   2. HashToRandomExchange -> goes into htrFragments
   */
  private static void findFragmentsWithPartitionSender(Fragment currentRootFragment, PlanningSet planningSet,
      List<Integer> deMuxFragments, List<Integer> htrFragments) {

    if (currentRootFragment != null) {
      final Exchange sendingExchange = currentRootFragment.getSendingExchange();
      if (sendingExchange != null) {
        final int majorFragmentId = planningSet.get(currentRootFragment).getMajorFragmentId();
        if (sendingExchange instanceof UnorderedDeMuxExchange) {
          deMuxFragments.add(majorFragmentId);
        } else if (sendingExchange instanceof HashToRandomExchange) {
          htrFragments.add(majorFragmentId);
        }
      }

      for(ExchangeFragmentPair e : currentRootFragment.getReceivingExchangePairs()) {
        findFragmentsWithPartitionSender(e.getNode(), planningSet, deMuxFragments, htrFragments);
      }
    }
  }

  /** Helper method to verify the number of PartitionSenders in a given fragment endpoint assignments */
  private static void verifyAssignment(List<Integer> fragmentList,
      ArrayListMultimap<Integer, NodeEndpoint> partitionSenderMap) {

    // We expect at least one entry the list
    assertTrue(fragmentList.size() > 0);

    for(Integer majorFragmentId : fragmentList) {
      // we expect the fragment that has DeMux/HashToRandom as sending exchange to have parallelization with not more
      // than the number of nodes in the cluster and each node in the cluster can have at most one assignment
      List<NodeEndpoint> assignments = partitionSenderMap.get(majorFragmentId);
      assertNotNull(assignments);
      assertTrue(assignments.size() > 0);
      assertTrue(String.format("Number of partition senders in major fragment [%d] is more than expected", majorFragmentId), CLUSTER_SIZE >= assignments.size());

      // Make sure there are no duplicates in assigned endpoints (i.e at most one partition sender per endpoint)
      assertTrue("Some endpoints have more than one fragment that has ParitionSender", ImmutableSet.copyOf(assignments).size() == assignments.size());
    }
  }

  @AfterClass
  public static void cleanupTempFolder() throws IOException {
    testTempFolder.delete();
  }
}
