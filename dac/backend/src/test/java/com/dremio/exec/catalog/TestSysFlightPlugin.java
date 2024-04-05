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

import com.dremio.BaseTestQuery;
import com.dremio.dac.service.flight.FlightCloseableBindableService;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.sysflight.SysFlightProducer;
import com.dremio.service.sysflight.SystemTableManagerImpl;
import com.dremio.test.DremioTest;
import com.google.inject.AbstractModule;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.joda.time.LocalDateTime;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests for SysFlightPlugin. */
public class TestSysFlightPlugin extends BaseTestQuery {
  @ClassRule
  public static final TestSysFlightResource SYS_FLIGHT_RESOURCE = new TestSysFlightResource();

  @BeforeClass
  public static final void setupDefaultTestCluster() throws Exception {
    // register the SysFlight service on conduit
    // and inject it in SabotNode.
    SABOT_NODE_RULE.register(
        new AbstractModule() {
          @Override
          protected void configure() {
            final ConduitServiceRegistry conduitServiceRegistry = new ConduitServiceRegistryImpl();
            BufferAllocator rootAllocator =
                RootAllocatorFactory.newRoot(DremioTest.DEFAULT_SABOT_CONFIG);
            BufferAllocator testAllocator =
                rootAllocator.newChildAllocator("test-sysflight-Plugin", 0, Long.MAX_VALUE);
            FlightCloseableBindableService flightService =
                new FlightCloseableBindableService(
                    testAllocator,
                    new SysFlightProducer(
                        () ->
                            new SystemTableManagerImpl(
                                testAllocator, SYS_FLIGHT_RESOURCE::getTablesProvider)),
                    null,
                    null);
            conduitServiceRegistry.registerService(flightService);
            bind(ConduitServiceRegistry.class).toInstance(conduitServiceRegistry);
          }
        });
    BaseTestQuery.setupDefaultTestCluster();
    TestSysFlightResource.addSysFlightPlugin(nodes[0]);
  }

  @Test
  public void testJobsTable() throws Exception {
    LocalDateTime dateTime = new LocalDateTime(1970, 01, 01, 00, 00, 00, 000);

    testBuilder()
        .sqlQuery("select * from sys.jobs")
        .unOrdered()
        .baselineColumns(
            "job_id",
            "status",
            "query_type",
            "user_name",
            "queried_datasets",
            "scanned_datasets",
            "attempt_count",
            "submitted_ts",
            "attempt_started_ts",
            "metadata_retrieval_ts",
            "planning_start_ts",
            "query_enqueued_ts",
            "engine_start_ts",
            "execution_planning_ts",
            "execution_start_ts",
            "final_state_ts",
            "submitted_epoch_millis",
            "attempt_started_epoch_millis",
            "metadata_retrieval_epoch_millis",
            "planning_start_epoch_millis",
            "query_enqueued_epoch_millis",
            "engine_start_epoch_millis",
            "execution_planning_epoch_millis",
            "execution_start_epoch_millis",
            "final_state_epoch_millis",
            "planner_estimated_cost",
            "rows_scanned",
            "bytes_scanned",
            "rows_returned",
            "bytes_returned",
            "accelerated",
            "queue_name",
            "engine",
            "error_msg",
            "query")
        .baselineValues(
            "1", "RUNNING", "UI_RUN", "user", "", "", 0, dateTime, dateTime, dateTime, dateTime,
            dateTime, dateTime, dateTime, dateTime, dateTime, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
            0.0d, 0L, 0L, 0L, 0L, false, "", "", "err", "")
        .baselineValues(
            "", "", "", "", "", "", 0, dateTime, dateTime, dateTime, dateTime, dateTime, dateTime,
            dateTime, dateTime, dateTime, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0.0d, 0L, 0L, 0L, 0L,
            false, "", "", "", "")
        .build()
        .run();
  }

  @Test
  public void testProjection() throws Exception {
    testBuilder()
        .sqlQuery("select Status, User_Name, Rows_Scanned from sys.jobs")
        .unOrdered()
        .baselineColumns("Status", "User_Name", "Rows_Scanned")
        .baselineValues("RUNNING", "user", 0L)
        .baselineValues("", "", 0L)
        .build()
        .run();
  }

  // Test scenario: If no column is required to be read from the storage plugin
  @Test
  public void testSkipQuery() throws Exception {
    testBuilder()
        .sqlQuery("select count(*) from sys.jobs")
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(2L)
        .go();
  }

  @Test
  public void testFilterPushdownFallBack() throws Exception {
    testBuilder()
        .sqlQuery("select job_id, status from sys.jobs where job_id='1'")
        .unOrdered()
        .baselineColumns("job_id", "status")
        .baselineValues("1", "RUNNING")
        .build()
        .run();
  }

  @Test
  public void testMaterializatonsTable() throws Exception {
    LocalDateTime defaultTime = new LocalDateTime(1970, 01, 01, 00, 00, 00, 000);

    testBuilder()
        .sqlQuery("select * from sys.materializations")
        .unOrdered()
        .baselineColumns(
            "reflection_id",
            "materialization_id",
            "created",
            "expires",
            "size_bytes",
            "series_id",
            "init_refresh_job_id",
            "series_ordinal",
            "join_analysis",
            "state",
            "failure_msg",
            "data_partitions",
            "last_refresh_from_pds",
            "last_refresh_finished",
            "last_refresh_duration_millis")
        .baselineValues(
            "",
            "",
            defaultTime,
            defaultTime,
            0L,
            0L,
            "",
            0,
            "",
            "",
            "",
            "",
            defaultTime,
            defaultTime,
            0L)
        .build()
        .run();
  }

  @Test
  public void testReflectionsTable() throws Exception {
    LocalDateTime defaultTime = new LocalDateTime(1970, 01, 01, 00, 00, 00, 000);

    testBuilder()
        .sqlQuery("select * from sys.reflections")
        .unOrdered()
        .baselineColumns(
            "reflection_id",
            "reflection_name",
            "type",
            "created_at",
            "updated_at",
            "status",
            "dataset_id",
            "dataset_name",
            "dataset_type",
            "sort_columns",
            "partition_columns",
            "distribution_columns",
            "dimensions",
            "measures",
            "display_columns",
            "external_reflection",
            "num_failures",
            "last_failure_message",
            "last_failure_stack",
            "arrow_cache",
            "refresh_status",
            "acceleration_status",
            "record_count",
            "current_footprint_bytes",
            "total_footprint_bytes",
            "last_refresh_duration_millis",
            "last_refresh_from_table",
            "refresh_method",
            "available_until",
            "considered_count",
            "matched_count",
            "accelerated_count")
        .baselineValues(
            "reflectionId",
            "",
            "",
            defaultTime,
            defaultTime,
            "",
            "datasetId",
            "datasetName",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            0,
            "",
            "",
            false,
            "",
            "",
            0L,
            0L,
            0L,
            0L,
            defaultTime,
            "",
            defaultTime,
            0,
            0,
            0)
        .baselineValues(
            "",
            "",
            "",
            defaultTime,
            defaultTime,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            0,
            "",
            "",
            false,
            "",
            "",
            0L,
            0L,
            0L,
            0L,
            defaultTime,
            "",
            defaultTime,
            0,
            0,
            0)
        .build()
        .run();
  }

  @Test
  public void testReflectionDependenciesTable() throws Exception {
    testBuilder()
        .sqlQuery("select * from sys.reflection_dependencies")
        .unOrdered()
        .baselineColumns("reflection_id", "dependency_id", "dependency_type", "dependency_path")
        .baselineValues("", "", "", "")
        .build()
        .run();
  }

  @Test
  public void testLowerLikeFilterReflectionsTable() throws Exception {
    testBuilder()
        .sqlQuery(
            "select reflection_id, dataset_id, dataset_name from sys.reflections r "
                + "where lower(r.dataset_name) like '%dataset%'")
        .unOrdered()
        .baselineColumns("reflection_id", "dataset_id", "dataset_name")
        .baselineValues("reflectionId", "datasetId", "datasetName")
        .build()
        .run();
  }

  @Test
  public void testLowerLikeFilterReflectionsTable2() throws Exception {
    testBuilder()
        .sqlQuery(
            "select reflection_id, dataset_id, dataset_name from sys.reflections r "
                + "where lower(r.dataset_name) like '%random%'")
        .unOrdered()
        .baselineColumns("reflection_id", "dataset_id", "dataset_name")
        .expectsEmptyResultSet()
        .build()
        .run();
  }

  @Test
  public void testUpperLikeFilterReflectionsTable() throws Exception {
    testBuilder()
        .sqlQuery(
            "select reflection_id, dataset_id, dataset_name from sys.reflections r "
                + "where upper(r.dataset_name) like '%DATASET%'")
        .unOrdered()
        .baselineColumns("reflection_id", "dataset_id", "dataset_name")
        .baselineValues("reflectionId", "datasetId", "datasetName")
        .build()
        .run();
  }

  @Test
  public void testUpperLikeFilterReflectionsTable2() throws Exception {
    testBuilder()
        .sqlQuery(
            "select reflection_id, dataset_id, dataset_name from sys.reflections r "
                + "where upper(r.dataset_name) like '%dataset%'")
        .unOrdered()
        .baselineColumns("reflection_id", "dataset_id", "dataset_name")
        .expectsEmptyResultSet()
        .build()
        .run();
  }

  @Test
  public void testUpperReverseLikeFilterReflectionsTable() throws Exception {
    testBuilder()
        .sqlQuery(
            "select reflection_id, dataset_id, dataset_name from sys.reflections r "
                + "where reverse(upper(r.dataset_name)) like '%TESATAD%'")
        .unOrdered()
        .baselineColumns("reflection_id", "dataset_id", "dataset_name")
        .baselineValues("reflectionId", "datasetId", "datasetName")
        .build()
        .run();
  }
}
