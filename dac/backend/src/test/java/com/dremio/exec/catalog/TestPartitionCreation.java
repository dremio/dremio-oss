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

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.dac.service.flight.FlightCloseableBindableService;
import com.dremio.exec.ExecConstants;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.sysflight.SysFlightProducer;
import com.dremio.service.sysflight.SystemTableManagerImpl;
import com.dremio.test.DremioTest;
import com.google.inject.AbstractModule;

public class TestPartitionCreation extends BaseTestQuery {
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
    TestSysFlightResource.addSysFlightPlugin(nodes[0]);
  }

  @Test
  public void testPartitionOnNullColumn() throws Exception {
    final String input = "cp.\"ctas_with_partition/null_partition.csv\"";
    final String select = "SELECT NULLIF(columns[0], '') c0, columns[1] c1 FROM " + input;
    final String ctas = "CREATE TABLE dfs_test.nulls PARTITION BY (c0) AS " + select;
    runSQL(ctas);

    // confirm that the data has been properly partitioned
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT COUNT(*) c FROM dfs_test.\"nulls/0_DREMIO_DEFAULT_NULL_PARTITION__\"")
      .baselineColumns("c")
      .baselineValues(3L)
      .go();
  }

  @Test
  public void testPartitionOnColumnWithNulls() throws Exception {
    final String input = "cp.\"ctas_with_partition/mixed_partition.parquet\"";
    final String ctas = "CREATE TABLE dfs_test.nulls_mixed PARTITION BY (A) AS SELECT * FROM " + input;
    runSQL(ctas);

    // confirm that the data has been properly partitioned
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT count(*) c FROM dfs_test.\"nulls_mixed/2_a\"")
      .baselineColumns("c")
      .baselineValues(1L)
      .go();
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT count(*) c FROM dfs_test.\"nulls_mixed/3_b\"")
      .baselineColumns("c")
      .baselineValues(1L)
      .go();
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT count(*) c FROM dfs_test.\"nulls_mixed/4_DREMIO_DEFAULT_NULL_PARTITION__\"")
      .baselineColumns("c")
      .baselineValues(1L)
      .go();
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT count(*) c FROM dfs_test.\"nulls_mixed/0_DREMIO_DEFAULT_EMPTY_VALUE_PARTITION__\"")
        .baselineColumns("c")
        .baselineValues(2L)
        .go();
  }

  @Test
  public void testPartitionOnIntColumnWithNulls() throws Exception {
    final String input = "cp.\"ctas_with_partition/null_int_partition.csv\"";
    final String select = "SELECT NULLIF(columns[0], '') c0, columns[1] c1 FROM " + input;
    final String ctas = "CREATE TABLE dfs_test.nulls_int PARTITION BY (c0) AS " + select;
    runSQL(ctas);

    // confirm that the data has been properly partitioned
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT count(*) c FROM dfs_test.\"nulls_int/0_0\"")
      .baselineColumns("c")
      .baselineValues(2L)
      .go();
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT count(*) c FROM dfs_test.\"nulls_int/2_DREMIO_DEFAULT_NULL_PARTITION__\"")
      .baselineColumns("c")
      .baselineValues(3L)
      .go();
  }


  @Test
  public void testPartitionNegativeValue() throws Exception {
    final String ctas = "create table dfs_test.ctas_with_negative_part partition by (float_val) as select name, -float_val as float_val from sys.options where float_val is not null";
    runSQL(ctas);
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT count(*) c FROM dfs_test.\"ctas_with_negative_part\"")
      .baselineColumns("c")
      .sqlBaselineQuery("select count(*) c from sys.options where float_val is not null")
      .go();
  }

  @Test
  public void testPartitionHash() throws Exception {
    try {
      setSessionOption(ExecConstants.SLICE_TARGET, "1");
      final String input = "sys.options";
      final String tableName = "dfs_test.hashpartitiontable";

      final String query = "CREATE TABLE " + tableName + " PARTITION BY (TYPE, KIND) as SELECT * FROM " + input;
      runSQL(query);

      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT name, kind, type, status, num_val, string_val FROM " + tableName)
        .sqlBaselineQuery("SELECT name, kind, type, status, num_val, string_val FROM " + input)
        .go();

      final String tableName2 = "dfs_test.hashpartitiontable2";
      final String query2 = "CREATE TABLE " + tableName2 + " HASH PARTITION BY (TYPE, KIND) as SELECT * FROM " + input;
      runSQL(query2);
      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT name, kind, type, status, num_val, string_val FROM " + tableName2)
        .sqlBaselineQuery("SELECT name, kind, type, status, num_val, string_val FROM " + input)
        .go();

    } finally {
      setSessionOption(ExecConstants.SLICE_TARGET, String.valueOf(ExecConstants.SLICE_TARGET_DEFAULT));
    }
  }

  @Test
  public void testPartitionRoundRobin() throws Exception {
    try {
      setSessionOption(ExecConstants.SLICE_TARGET, "1");
      final String input = "sys.options";
      final String tableName = "dfs_test.roundrobintable";

      final String query = "CREATE TABLE " + tableName + " PARTITION BY (TYPE, KIND) as SELECT * FROM " + input;
      runSQL(query);

      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT name, kind, type, status, num_val, string_val FROM " + tableName)
        .sqlBaselineQuery("SELECT name, kind, type, status, num_val, string_val FROM " + input)
        .go();

      final String tableName2 = "dfs_test.roundrobintable2";
      final String query2 = "CREATE TABLE " + tableName2 + " ROUNDROBIN PARTITION BY (TYPE, KIND) as SELECT * FROM " + input;
      runSQL(query2);
      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT name, kind, type, status, num_val, string_val FROM " + tableName2)
        .sqlBaselineQuery("SELECT name, kind, type, status, num_val, string_val FROM " + input)
        .go();

    } finally {
      setSessionOption(ExecConstants.SLICE_TARGET, String.valueOf(ExecConstants.SLICE_TARGET_DEFAULT));
    }
  }

  @Test
  public void testPartitionCreation() throws Exception {
    test("create table dfs_test.mypart0 PARTITION BY (TYPE, KIND) STORE AS (type => 'TEXT', fieldDelimiter => ',') as select * from sys.options");
    test("create table dfs_test.mypart1 DISTRIBUTE BY (kind) LOCALSORT BY (NAME) STORE AS (type => 'TEXT', fieldDelimiter => ',') as select * from sys.options");
    test("create table dfs_test.mypart2 PARTITION BY (TYPE) DISTRIBUTE BY (kind) LOCALSORT BY (NAME) STORE AS (type => 'TEXT', fieldDelimiter => ',') as select * from sys.options");
    test("create table dfs_test.mypart3 PARTITION BY (kind) STORE AS (type => 'TEXT', fieldDelimiter => ',') as select * from sys.options");
  }

  @Test
  public void testDistributionBuckets() throws Exception {
    List<QueryDataBatch> result = testSqlWithResults("create table dfs_test.options_name DISTRIBUTE BY (name) STORE AS (type => 'TEXT', fieldDelimiter => ',') as select * from sys.options");

    // Expect DISTRIBUTE BY to give multiple rows, i.e. multiple files
    int recordCount = 0;
    for (QueryDataBatch batch: result) {
      recordCount += batch.getHeader().getDef().getRecordCount();
      batch.close();
    }

    assertTrue(recordCount > 1);

  }

}
