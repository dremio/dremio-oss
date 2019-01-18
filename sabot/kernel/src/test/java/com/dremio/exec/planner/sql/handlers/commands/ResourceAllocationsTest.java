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
package com.dremio.exec.planner.sql.handlers.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.options.OptionValue;
import com.dremio.resource.ResourceAllocation;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.basic.BasicResourceAllocator;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.DirectProvider;
import com.google.common.collect.Lists;

/**
 * To test BasicResourceAllocator Foreman interactions
 */
public class ResourceAllocationsTest extends BaseTestQuery {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResourceAllocationsTest.class);

  private static String TEST_PATH = TestTools.getWorkingPath() + "/src/test/resources";
  private static File tblPath = null;

  @BeforeClass
  public static void createTable() throws Exception {
    updateTestCluster(10, config);

    tblPath = new File(getDfsTestTmpSchemaLocation(), "yelp");
    FileUtils.deleteQuietly(tblPath);
    FileUtils.copyFileToDirectory(new File(TEST_PATH + "/yelp_business.json"), tblPath);
    FileUtils.moveFile(new File(tblPath + "/yelp_business.json"), new File(tblPath + "/1.json"));
    FileUtils.copyFile(new File(tblPath + "/1.json"), new File(tblPath + "/2.json"));
  }

  @AfterClass
  public static void cleanUpTable() throws Exception {
    FileUtils.deleteQuietly(tblPath);
  }

  @Test
  public void resourceAllocationTest() throws Exception {
    final String yelpTable = TEMP_SCHEMA + ".\"yelp\"";
    final String sql = "SELECT nested_0.review_id AS review_id, nested_0.user_id AS user_id, nested_0.votes AS votes," +
      " nested_0.stars AS stars, join_business.business_id AS business_id0, join_business.neighborhoods AS neighborhoods, join_business.city AS city, join_business.latitude AS latitude, join_business.review_count AS review_count, join_business.full_address AS full_address, join_business.stars AS stars0, join_business.categories AS categories, join_business.state AS state, join_business.longitude AS longitude\n" +
      "FROM (\n" +
      "  SELECT review_id, user_id, votes, stars, business_id\n" +
      "  FROM cp.\"yelp_review.json\" where 1 = 0\n" +
      ") nested_0\n" +
      " FULL JOIN " + yelpTable + " AS join_business ON nested_0.business_id = join_business.business_id";

    final SabotContext context = getSabotContext();
    context.getOptionManager().setOption(
      OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.slice_target", 1)
    );
    context.getOptionManager().setOption(
      OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.width.max_per_node", 10)
    );
    context.getOptionManager().setOption(
      OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "planner.enable_mux_exchange", true)
    );
    context.getOptionManager().setOption(
      OptionValue.createLong(OptionValue.OptionType.SYSTEM, "exec.queue.memory.small", 4096)
    );

    context.getOptionManager().setOption(
        OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, ExecConstants.USE_NEW_MEMORY_BOUNDED_BEHAVIOR.getOptionName(), false)
      );
    final QueryContext queryContext = new QueryContext(session(), context, UserBitShared.QueryId.getDefaultInstance());
    final AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    final SqlConverter converter = new SqlConverter(
      queryContext.getPlannerSettings(),
      queryContext.getOperatorTable(),
      queryContext,
      queryContext.getMaterializationProvider(),
      queryContext.getFunctionRegistry(),
      queryContext.getSession(),
      observer,
      queryContext.getCatalog(),
      queryContext.getSubstitutionProviderFactory(),
      queryContext.getConfig(),
      queryContext.getScanResult());
    final SqlNode node = converter.parse(sql);
    final SqlHandlerConfig config = new SqlHandlerConfig(queryContext, converter, observer, null);

    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, node);
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();

    final Rel drel = PrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);

    final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
    final Prel prel = convertToPrel.getKey();

    final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
    final PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);

    final PhysicalPlanReader pPlanReader = new PhysicalPlanReader(
      DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT, new LogicalPlanPersistence(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT),
      CoordinationProtos.NodeEndpoint.getDefaultInstance(),
      DirectProvider.wrap(Mockito.mock(CatalogService.class)), context);

    final BasicResourceAllocator resourceAllocator = new BasicResourceAllocator(DirectProvider.wrap(clusterCoordinator));
    resourceAllocator.start();

    final AsyncCommand asyncCommand = new AsyncCommand(queryContext, resourceAllocator, observer) {


      @Override
      public double plan() throws Exception {
        return 0;
      }

      @Override
      public Object execute() throws Exception {
        return null;
      }

      @Override
      public String getDescription() {
        return null;
      }
    };

    final PlanningSet planningSet = asyncCommand.allocateResourcesBasedOnPlan(plan);

    final ExecutionPlan exec = ExecutionPlanCreator.getExecutionPlan(queryContext, pPlanReader, observer, plan,
      asyncCommand.getResources(), planningSet);
    List<CoordExecRPC.PlanFragment> fragments  = exec.getFragments();

    logger.info("Fragments size: " + fragments.size());
    for (CoordExecRPC.PlanFragment fragment : fragments) {
      logger.info(fragment.getHandle().getMajorFragmentId() + " : " +fragment.getHandle().getMinorFragmentId()
      + " : " + fragment.getAssignment().getUserPort() + ", "
      + fragment.getAssignment().getFabricPort());
      assertEquals(4096, fragment.getMemMax());
    }

    // try to cheat to redo parallelization
    List<ResourceAllocation> allocations = asyncCommand.getResources().getResourceAllocations();
    allocations.sort(Comparator.comparing(ResourceAllocation::getMajorFragment));

    logger.info("Allocations: " + allocations.size());

    List<ResourceAllocation> copyAllocations = Lists.newArrayList();

    int prevMajorFragmentId = -1;
    for (ResourceAllocation allocation : allocations) {
      int majorFragmentId = allocation.getMajorFragment();
      if (majorFragmentId != prevMajorFragmentId) {
        copyAllocations.add(resourceAllocator.createAllocation(nodes[0].getContext().getEndpoint(), allocation
          .getMemory(), allocation.getMajorFragment()));
        prevMajorFragmentId = majorFragmentId;
      }
    }

    ResourceSet copyResourceSet = new ResourceSet() {

      private boolean isSet = false;
      @Override
      public List<ResourceAllocation> getResourceAllocations() {
        return isSet ? asyncCommand.getResources().getResourceAllocations() : copyAllocations;
      }

      @Override
      public void reassignMajorFragments(Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> majorToEndpoinsMap) {
        asyncCommand.getResources().reassignMajorFragments(majorToEndpoinsMap);
        isSet = true;
      }

      @Override
      public void close() throws IOException {

      }
    };

    // copyAllocations should have one NodeEndPoint per major fragment
    final ExecutionPlan execUpdated = ExecutionPlanCreator.getExecutionPlan(queryContext, pPlanReader, observer, plan,
      copyResourceSet, planningSet);

    fragments  = execUpdated.getFragments();

    logger.info("After reparallelization");
    logger.info("Fragments size: " + fragments.size());

    for (CoordExecRPC.PlanFragment fragment : fragments) {
      logger.info(fragment.getHandle().getMajorFragmentId() + " : " +fragment.getHandle().getMinorFragmentId()
        + " : " + fragment.getAssignment().getUserPort() + ", "
        + fragment.getAssignment().getFabricPort());
      assertEquals(4096, fragment.getMemMax());
    }

    List<ResourceAllocation> copyAllocations2 = Lists.newArrayList();

    final String bogusAddress = "111.111.111.111";
    final Random random = new Random();
    for (ResourceAllocation allocation : allocations) {
      // 50% chance of having a bogus node
      if (random.nextInt(2) == 0) {
        // the bogus node should be rejected when creating the execution plan (asserted below)
        CoordinationProtos.NodeEndpoint bogusEndpoint =
          CoordinationProtos.NodeEndpoint.newBuilder(nodes[0].getContext().getEndpoint())
            .setAddress(bogusAddress)
            .build();
        copyAllocations2.add(resourceAllocator.createAllocation(bogusEndpoint, allocation.getMemory(), allocation.getMajorFragment()));
      } else {
        copyAllocations2.add(resourceAllocator.createAllocation(nodes[random.nextInt(5)].getContext().getEndpoint(),
          allocation.getMemory(), allocation.getMajorFragment()));
      }
    }

    ResourceSet copyResourceSet2 = new ResourceSet() {

      private boolean isSet = false;

      @Override
      public List<ResourceAllocation> getResourceAllocations() {
        return isSet ? asyncCommand.getResources().getResourceAllocations() : copyAllocations2;
      }

      @Override
      public void reassignMajorFragments(Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> majorToEndpoinsMap) {
        asyncCommand.getResources().reassignMajorFragments(majorToEndpoinsMap);
        isSet = true;
      }

      @Override
      public void close() throws IOException {

      }
    };

    // copyAllocations should have one NodeEndPoint per major fragment
    final ExecutionPlan execUpdated2 = ExecutionPlanCreator.getExecutionPlan(queryContext, pPlanReader, observer, plan,
      copyResourceSet2, planningSet);

    fragments  = execUpdated2.getFragments();

    logger.info("After reparallelization");
    logger.info("Fragments size: " + fragments.size());

    for (CoordExecRPC.PlanFragment fragment : fragments) {
      logger.info(fragment.getHandle().getMajorFragmentId() + " : " +fragment.getHandle().getMinorFragmentId()
        + " : " + fragment.getAssignment().getUserPort() + ", "
        + fragment.getAssignment().getFabricPort());
      assertEquals(4096, fragment.getMemMax());
      assertNotEquals(fragment.getAssignment().getAddress(), bogusAddress);
    }

    asyncCommand.getResources().close();
    for (ResourceAllocation resourceAllocation : copyAllocations) {
      resourceAllocation.close();
    }
    for (ResourceAllocation resourceAllocation : copyAllocations2) {
      resourceAllocation.close();
    }
    resourceAllocator.close();
  }

  private static UserSession session() {
    return UserSession.Builder.newBuilder()
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
      .withOptionManager(getSabotContext().getOptionManager())
      .setSupportComplexTypes(true)
      .build();
  }
}
