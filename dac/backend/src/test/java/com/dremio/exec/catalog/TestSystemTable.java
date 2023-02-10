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
package com.dremio.exec.catalog;

import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.LocalDateTime;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.dac.service.flight.FlightCloseableBindableService;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.sys.ServicesIterator;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.sysflight.SysFlightProducer;
import com.dremio.service.sysflight.SystemTableManagerImpl;
import com.dremio.test.DremioTest;
import com.google.inject.AbstractModule;

public class TestSystemTable extends BaseTestQuery {
  @ClassRule
  public static final TestSysFlightResource SYS_FLIGHT_RESOURCE = new TestSysFlightResource();

  private static final int NUM_NODES = 3;

  @BeforeClass
  public static void setupMultiNodeCluster() throws Exception {
    // register the SysFlight service on conduit
    // and inject it in SabotNode.
    SABOT_NODE_RULE.register(new AbstractModule() {
      @Override
      protected void configure() {
        final ConduitServiceRegistry conduitServiceRegistry = new ConduitServiceRegistryImpl();
        BufferAllocator rootAllocator = RootAllocatorFactory.newRoot(DremioTest.DEFAULT_SABOT_CONFIG);
        BufferAllocator testAllocator = rootAllocator.newChildAllocator("test-sysflight-Plugin", 0, Long.MAX_VALUE);
        FlightCloseableBindableService flightService = new FlightCloseableBindableService(testAllocator,
          new SysFlightProducer(() -> new SystemTableManagerImpl(testAllocator, SYS_FLIGHT_RESOURCE::getTablesProvider)), null, null);
        conduitServiceRegistry.registerService(flightService);
        bind(ConduitServiceRegistry.class).toInstance(conduitServiceRegistry);
      }
    });
    updateTestCluster(NUM_NODES, null);
    TestSysFlightResource.addSysFlightPlugin(nodes[0]);
  }

  @Test
  public void testJobsTable() throws Exception {
    LocalDateTime dateTime = new LocalDateTime(1970, 01, 01, 00, 00, 00, 000);

    testBuilder()
      .sqlQuery("select * from sys.jobs")
      .unOrdered()
      .baselineColumns("job_id", "status", "query_type", "user_name", "queried_datasets", "scanned_datasets",
        "attempt_count", "submitted_ts", "attempt_started_ts", "metadata_retrieval_ts", "planning_start_ts",
        "query_enqueued_ts", "engine_start_ts", "execution_planning_ts", "execution_start_ts", "final_state_ts",
        "submitted_epoch_millis", "attempt_started_epoch_millis", "metadata_retrieval_epoch_millis",
        "planning_start_epoch_millis", "query_enqueued_epoch_millis", "engine_start_epoch_millis",
        "execution_planning_epoch_millis", "execution_start_epoch_millis", "final_state_epoch_millis",
        "planner_estimated_cost", "rows_scanned", "bytes_scanned", "rows_returned", "bytes_returned",
        "accelerated", "queue_name", "engine", "error_msg", "query")
      .baselineValues("1", "RUNNING", "UI_RUN", "user", "", "", 0, dateTime, dateTime, dateTime, dateTime, dateTime,
        dateTime, dateTime, dateTime, dateTime, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0.0d, 0L, 0L, 0L, 0L, false, "", "", "err", "")
      .baselineValues("", "", "", "", "", "", 0, dateTime, dateTime, dateTime, dateTime, dateTime, dateTime, dateTime,
        dateTime, dateTime, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0.0d, 0L, 0L, 0L, 0L, false, "", "", "", "")
      .build()
      .run();
  }

  @Test
  public void alterSessionOption() throws Exception {

    testBuilder() //
      .sqlQuery("select bool_val as bool from sys.options where name = '%s' order by type desc", ExecConstants.JSON_ALL_TEXT_MODE)
      .baselineColumns("bool")
      .ordered()
      .baselineValues(false)
      .go();

    test("alter session set \"%s\" = true", ExecConstants.JSON_ALL_TEXT_MODE);

    testBuilder() //
      .sqlQuery("select bool_val as bool from sys.options where name = '%s' order by type desc ", ExecConstants.JSON_ALL_TEXT_MODE)
      .baselineColumns("bool")
      .ordered()
      .baselineValues(false)
      .baselineValues(true)
      .go();
  }

  // DRILL-2670
  @Test
  public void optionsOrderBy() throws Exception {
    test("select * from sys.options order by name");
  }

  @Test
  public void differentCase() throws Exception {
    test("select * from SYS.OPTIONS");
  }

  @Test
  public void threadsTable() throws Exception {
    test("select * from sys.threads");
  }

  @Test
  public void memoryTable() throws Exception {
    test("select * from sys.memory");
  }

  @Test
  public void fragmentsTable() throws Exception {
    test("select * from sys.fragments");

    // test existence of memory_grant column
    testBuilder()
      .sqlQuery("select memory_grant from sys.fragments")
      .schemaBaseLine(Collections.singletonList(Pair.of(SchemaPath.getSimplePath("memory_grant"),
        Types.optional(TypeProtos.MinorType.BIGINT))))
      .go();
  }

  @Test
  public void verifyNumNodes() throws Exception {
    testBuilder()
        .sqlQuery("SELECT COUNT(*) AS num_nodes FROM SYS.NODES")
        .ordered()
        .baselineColumns("num_nodes")
        .baselineValues((long) NUM_NODES)
        .go();
  }

  /**
   * @return the configured max width per node, at the SYSTEM level
   */
  private long getConfiguredMaxWidthPerNode() throws Exception {
    final String fetchMaxWidthQuery = String.format(
      "SELECT num_val FROM sys.options WHERE name='%s' AND type='SYSTEM'",
      GroupResourceInformation.MAX_WIDTH_PER_NODE_KEY);
    String maxWidthString = getResultString(
      testRunAndReturn(UserBitShared.QueryType.SQL, fetchMaxWidthQuery), "", false);

    return Long.parseLong(maxWidthString.substring(0, maxWidthString.indexOf('\n')));
  }

  @Test
  @Ignore
  public void nodesTable() throws Exception {
    long configuredMaxWidth = getConfiguredMaxWidthPerNode();

    // this test exposes an interesting "fact" about how cluster load is being computed
    // WorkManager computes the cluster load using the system max_width_per_node
    // but when we query `sys.nodes` it returns the value, for 'configured_max_width'
    // that has been set at the session level

    final String query = "SELECT CAST((cluster_load*100) AS INTEGER) AS cluster_load, " +
      "configured_max_width, actual_max_width FROM sys.nodes";

    // the query will run one fragment per node
    int expectedClusterLoad = (int) Math.round(100.0 / configuredMaxWidth);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("cluster_load", "configured_max_width", "actual_max_width")
      .baselineValues(expectedClusterLoad, MAX_WIDTH_PER_NODE, MAX_WIDTH_PER_NODE)
      .baselineValues(expectedClusterLoad, MAX_WIDTH_PER_NODE, MAX_WIDTH_PER_NODE)
      .baselineValues(expectedClusterLoad, MAX_WIDTH_PER_NODE, MAX_WIDTH_PER_NODE)
      .go();
  }

  @Test
  public void servicesTable() throws Exception {
    String query = "SELECT * FROM sys.services";
    TestBuilder test = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineRecords(null)
      .baselineColumns("service", "hostname", "user_port", "fabric_port");

    ServicesIterator services = new ServicesIterator(getSabotContext());
    while (services.hasNext()) {
      ServicesIterator.ServiceSetInfo info = (ServicesIterator.ServiceSetInfo) services.next();
      test.baselineValues(info.service, info.hostname, info.user_port, info.fabric_port);
    }

    test.go();
  }

  @Test
  public void timezoneAbbreviationsTable() throws Exception {
    String query = "SELECT * from sys.timezone_abbrevs limit 1";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("timezone_abbrev", "tz_offset", "is_daylight_savings")
      .baselineValues("ACDT", "+10:30", true)
      .go();

    // test results are sorted by timezone_abbrev
    testBuilder()
      .sqlQuery("SELECT * from sys.timezone_abbrevs")
      .ordered()
      .sqlBaselineQuery("SELECT * from sys.timezone_abbrevs order by timezone_abbrev asc")
      .go();

    query = "SELECT tz_offset, is_daylight_savings from sys.timezone_abbrevs where timezone_abbrev = 'PDT'";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("tz_offset", "is_daylight_savings")
      .baselineValues("-07:00", true)
      .go();
  }

  @Test
  public void timezoneNames() throws Exception {
    testBuilder()
      .sqlQuery("SELECT * from sys.timezone_names limit 1")
      .ordered()
      .baselineColumns("timezone_name", "tz_offset", "offset_daylight_savings", "is_daylight_savings")
      .baselineValues("Africa/Abidjan", "+00:00", "+00:00", false)
      .go();

    testBuilder()
      .sqlQuery("SELECT * from sys.timezone_names")
      .ordered()
      .sqlBaselineQuery("SELECT * from sys.timezone_names order by timezone_name asc")
      .go();
  }

  @Test
  @Ignore
  public void timezoneNames2() throws Exception {
    testBuilder()
      .sqlQuery("SELECT * from sys.timezone_names where timezone_name = 'America/Los_Angeles'")
      .ordered()
      .baselineColumns("timezone_name", "tz_offset", "offset_daylight_savings", "is_daylight_savings")
      .baselineValues("America/Los_Angeles", "-08:00", "-07:00", true)
      .go();
  }

}
