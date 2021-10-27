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

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.joda.time.LocalDateTime;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.dac.service.flight.FlightCloseableBindableService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.plugins.sysflight.SysFlightPluginConf;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.sysflight.SysFlightProducer;
import com.dremio.service.sysflight.SystemTableManagerImpl;
import com.dremio.test.DremioTest;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;

/**
 * Tests for SysFlightPlugin.
 */
public class TestSysFlightPlugin extends BaseTestQuery {
  @ClassRule
  public static final TestSysFlightResource SYS_FLIGHT_RESOURCE = new TestSysFlightResource();

  @BeforeClass
  public static final void setupDefaultTestCluster() throws Exception {
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
    BaseTestQuery.setupDefaultTestCluster();
    final Injector injector = getInjector();
    final Provider<SabotContext> sabotContext = injector.getProvider(SabotContext.class);

    // create the sysFlight source
    SourceConfig c = new SourceConfig();
    SysFlightPluginConf conf = new SysFlightPluginConf();
    c.setType(conf.getType());
    c.setName("testSysFlight");
    c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    c.setConfig(conf.toBytesString());
    ((CatalogServiceImpl) sabotContext.get().getCatalogService()).getSystemUserCatalog().createSource(c);
  }

  @Test
  public void testJobsTable() throws Exception {
    LocalDateTime dateTime = new LocalDateTime(1970, 01, 01, 00, 00, 00, 000);

    testBuilder()
      .sqlQuery("select * from testSysFlight.jobs")
      .unOrdered()
      .baselineColumns("job_id", "status", "query_type", "user_name", "accelerated", "submit", "queue", "start", "finish", "queue_name",
        "engine", "query_planning_time", "queue_time", "execution_planning_time", "execution_time", "row_count", "error_msg")
      .baselineValues("1", "RUNNING", "UI_RUN", "user", false, dateTime, dateTime, dateTime, dateTime, "", "", 0L, 0L, 0L, 0L, 0L, "err")
      .baselineValues("", "", "", "", false, dateTime, dateTime, dateTime, dateTime, "", "", 0L, 0L, 0L, 0L, 0L, "")
      .build()
      .run();
  }

  @Test
  public void testProjection() throws Exception {
    testBuilder()
      .sqlQuery("select Status, User_Name, Row_Count from testSysFlight.jobs")
      .unOrdered()
      .baselineColumns( "Status", "User_Name", "Row_Count")
      .baselineValues("RUNNING", "user", 0L)
      .baselineValues("", "", 0L)
      .build()
      .run();
  }

  @Test
  public void testFilterPushdownFallBack() throws Exception {
    testBuilder()
      .sqlQuery("select job_id, status from testSysFlight.jobs where job_id='1'")
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
      .sqlQuery("select * from testSysFlight.materializations")
      .unOrdered()
      .baselineColumns("reflection_id", "materialization_id", "created", "expires", "size_bytes", "series_id",
        "init_refresh_job_id", "series_ordinal", "join_analysis", "state", "failure_msg", "data_partitions", "last_refresh_from_pds")
      .baselineValues("", "", defaultTime, defaultTime, 0L, 0L, "", 0, "", "", "", "", defaultTime)
      .build()
      .run();
  }

  @Test
  public void testReflectionsTable() throws Exception {
    testBuilder()
      .sqlQuery("select * from testSysFlight.reflections")
      .unOrdered()
      .baselineColumns("reflection_id", "reflection_name", "type", "status", "dataset_id", "dataset_name", "dataset_type", "sort_columns", "partition_columns",
        "distribution_columns", "dimensions", "measures", "display_columns", "external_reflection", "num_failures", "arrow_cache")
      .baselineValues("", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, false)
      .build()
      .run();
  }

  @Test
  public void testReflectionDependenciesTable() throws Exception {
    testBuilder()
      .sqlQuery("select * from testSysFlight.reflection_dependencies")
      .unOrdered()
      .baselineColumns("reflection_id", "dependency_id", "dependency_type", "dependency_path")
      .baselineValues("", "", "", "")
      .build()
      .run();
  }
}
