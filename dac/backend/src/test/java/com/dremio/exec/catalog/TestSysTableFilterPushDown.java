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

import static org.junit.Assert.assertTrue;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.PlanTestBase;
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

public class TestSysTableFilterPushDown extends PlanTestBase {
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
    c.setConfig(new SysFlightPluginConf().toBytesString());
    ((CatalogServiceImpl) sabotContext.get().getCatalogService()).getSystemUserCatalog().createSource(c);
  }

  @Test
  public void testFilterPushdown_Equal() throws Exception {
    final String query = "SELECT * FROM testSysFlight.jobs WHERE user_name='hz'";
    final String scan = "query=[equals {   field: \"user_name\"   stringValue: \"hz\" } ]";
    testHelper(query, scan);
  }

  @Ignore("NOT not currently supported")
  @Test
  public void testFilterPushdown_NonEqual() throws Exception {
    final String query = "SELECT * FROM testSysFlight.jobs WHERE user_name <> 'da'";
    final String scan = "query=[type: NOT not {   clause {     type: TERM     term {       field: \"user_name\"       value: \"da\"     }   } } ]";

    testHelper(query, scan);
  }

  @Test
  public void testFilterPushdown_Like() throws Exception {
    final String query = "SELECT * FROM testSysFlight.jobs WHERE user_name LIKE '%z'";
    final String scan = "query=[like {   field: \"user_name\"   pattern: \"%z\" } ]";

    testHelper(query, scan);
  }

  @Test
  public void testFilterPushdown_LikeWithEscape() throws Exception {
    final String query = "SELECT * FROM testSysFlight.jobs WHERE user_name LIKE '%\\\\SCH%' ESCAPE '\\'";
    final String scan = "query=[like {   field: \"user_name\"   pattern: \"%\\\\\\\\SCH%\"   escape: \"\\\\\" } ])";

    testHelper(query, scan);
  }

  @Test
  public void testFilterPushdown_And() throws Exception {
    final String query = "SELECT * FROM testSysFlight.jobs WHERE " +
      "user_name = 'hz' AND " +
      "row_count = '0'";
    final String scan = "query=[and {   clauses {     equals {       field: \"user_name\"       stringValue: \"hz\"     }   }   clauses {     equals {       field: \"row_count\"       stringValue: \"0\"     }   } } ]";

    testHelper(query, scan);
  }

  @Ignore("NOT not currently supported")
  @Test
  public void testFilterPushdown_Or() throws Exception {
    final String query = "SELECT * FROM testSysFlight.jobs WHERE " +
      "user_name = 'sys' OR " +
      "job_id <> 'version' OR " +
      "row_count like '%sdfgjk%'";
    final String scan = "query=[type: BOOLEAN boolean {   op: OR   clauses {     type: TERM     term {       field: \"user_name\"       value: \"sys\"     }   }   clauses {     type: NOT     not {       clause {         type: TERM         term {           field: \"job_id\"           value: \"version\"         }       }     }   }   clauses {     type: WILDCARD     wildcard {       field: \"row_count\"       value: \"*sdfgjk*\"     }   } } ]";

    testHelper(query, scan);
  }

  @Test
  public void testFilterPushdown_Having() throws Exception {
    final String query = "SELECT user_name,job_id FROM testSysFlight.jobs WHERE (user_name = 'hz') group by user_name,job_id"+
      " HAVING job_id LIKE 'ref%'"+
      " ORDER BY 1, 2 ASC";
    final String scan = "query=[and {   clauses {     equals {       field: \"user_name\"       stringValue: \"hz\"     }   }   clauses {     like {       field: \"job_id\"       pattern: \"ref%\"     }   } } ]";
    testHelper(query, scan);
  }


  @Test
  public void testFilterPushdown_OrEqLike() throws Exception {
    final String query ="SELECT DISTINCT user_name,job_id FROM testSysFlight.jobs WHERE  "+
      "user_name = 'hz'  "+
      "OR user_name LIKE 'sys.%'ORDER BY 1, 2 ASC";
    final String scan = "query=[or {   clauses {     equals {       field: \"user_name\"       stringValue: \"hz\"     }   }   clauses {     like {       field: \"user_name\"       pattern: \"sys.%\"     }   } } ]";
    testHelper(query, scan);
  }

  @Test
  public void testFilterPushdown_OrIn() throws Exception {
    final String query ="SELECT DISTINCT user_name, job_id FROM testSysFlight.jobs WHERE "+
      "user_name IN ('hz','da') order by 1 ASC";

    final String scan = "query=[or {   clauses {     equals {       field: \"user_name\"       stringValue: \"hz\"     }   }   clauses {     equals {       field: \"user_name\"       stringValue: \"da\"     }   } } ]";
    testHelper(query, scan);
  }

  @Test
  public void testFilterPushDownWithProject_Equal() throws Exception {
    final String query = "SELECT job_id from testSysFlight.jobs WHERE user_name = 'hz'";
    final String scan = "query=[equals {   field: \"user_name\"   stringValue: \"hz\" } ]";
    testHelper(query, scan);
  }

  @Ignore("NOT not currently supported")
  @Test
  public void testFilterPushDownWithProject_NotEqual() throws Exception {
    final String query = "SELECT job_id from testSysFlight.jobs WHERE user_name <> 'hz'";
    final String scan = "query=[type: NOT not {   clause {     type: TERM     term {       field: \"user_name\"       value: \"TABLES\"     }   } } ]";
    testHelper(query, scan);
  }

  @Test
  public void testFilterPushDownWithProject_Like() throws Exception {
    final String query = "SELECT job_id from testSysFlight.jobs WHERE user_name LIKE '%hz%'";
    final String scan = "query=[like {   field: \"user_name\"   pattern: \"%hz%\" } ]";
    testHelper(query, scan);
  }


  private void testHelper(final String query, String filterInScan) throws Exception {
    final String plan = getPlanInString("EXPLAIN PLAN FOR " + query, OPTIQ_FORMAT);

    // check if the plan contains fall back
    assertTrue(String.format("Expected plan to contain filter and did not.\n %s", plan), plan.contains("Filter("));

    // Check for filter pushed into scan.
    assertTrue(String.format("Expected plan to contain %s, however it did not.\n %s", filterInScan, plan), plan.contains(filterInScan));

    // run the query
    test(query);
  }
}
