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
import com.dremio.service.sysflight.TestSysFlightResource;
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
          new SysFlightProducer(testAllocator, SYS_FLIGHT_RESOURCE::getChronicleBlockingStub), null, null);
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
    c.setConfig(new SysFlightPluginConf().toBytesString());
    ((CatalogServiceImpl) sabotContext.get().getCatalogService()).getSystemUserCatalog().createSource(c);
  }

  @Test
  public void test() throws Exception {
    //Type of time stamp is currently bigInt.
    Long expectedDateTime = Long.valueOf(0);

    testBuilder()
      .sqlQuery("select * from testSysFlight.jobs")
      .unOrdered()
      .baselineColumns("job_id", "status", "error_msg", "user_name", "start", "finish", "query_type", "accelerated", "row_count")
      .baselineValues("1", "RUNNING","err","user", expectedDateTime, expectedDateTime, "UI_RUN", false, (long)0)
      .baselineValues("", "", "", "", expectedDateTime, expectedDateTime,"", false, (long)0)
      .build()
      .run();
  }

  @Test
  public void testSelectColumns() throws Exception {
    testBuilder()
      .sqlQuery("select Status, User_Name, Row_Count from testSysFlight.jobs")
      .unOrdered()
      .baselineColumns( "Status", "User_Name", "Row_Count")
      .baselineValues("RUNNING", "user", (long)0)
      .baselineValues("", "", (long)0)
      .build()
      .run();
  }

}
