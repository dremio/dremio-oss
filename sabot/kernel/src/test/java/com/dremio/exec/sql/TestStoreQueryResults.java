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
package com.dremio.exec.sql;

import static java.lang.String.format;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.CloseableByteBuf;
import com.dremio.common.DeferredException;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AbstractQueryObserver;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.WriterCommitterPrel;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.UserProtos.SubmissionSource;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.user.LocalExecutionConfig;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.proto.model.attempts.AttemptReason;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;

/**
 * Tests various types of queries and commands to make sure storing query results in a table works. Basically when
 * {@link PlannerSettings#STORE_QUERY_RESULTS} is enabled and {@link PlannerSettings#QUERY_RESULTS_STORE_TABLE} set.
 */
public class TestStoreQueryResults extends BaseTestQuery {

  private static class TestQueryObserver extends AbstractQueryObserver {
    private final CountDownLatch latch = new CountDownLatch(1);
    private final DeferredException exception = new DeferredException();
    private final boolean checkPlanWriterDistribution;
    private volatile AttemptId attemptId;

    TestQueryObserver(boolean checkPlanWriterDistribution) {
      this.checkPlanWriterDistribution = checkPlanWriterDistribution;
    }


    public AttemptId getAttemptId() {
      return attemptId;
    }

    @Override
    public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
      this.attemptId = attemptId;
      return new AbstractAttemptObserver() {
        @Override
        public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
          try {
            AutoCloseables.close(
              Arrays.stream(result.getBuffers())
                .map(CloseableByteBuf::new)
                .collect(ImmutableList.toImmutableList()));
          } catch (Exception e) {
            exception.addException(e);
          }
          outcomeListener.success(Acks.OK, null);
        }

        @Override
        public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after,
                                     long millisTaken, Map<String, Long> timeBreakdownPerRule) {
          if (phase == PlannerPhase.PHYSICAL) {
            if (checkPlanWriterDistribution) {
              // Visit the tree and check that all the WriterCommitter is a singleton and its input is also singleton
              // We check here in PHYSCIAL right before the visitors in convertToPrel since convertToPrel might get rid of unnecessary exchanges
              after.accept(new StatelessRelShuttleImpl() {
                @Override
                public RelNode visit(RelNode other) {
                  if (other instanceof WriterCommitterPrel) {
                    if ( (other.getTraitSet().getTrait(DistributionTraitDef.INSTANCE) != DistributionTrait.SINGLETON)
                      || ((WriterCommitterPrel) other).getInput().getTraitSet().getTrait(DistributionTraitDef.INSTANCE) != DistributionTrait.SINGLETON) {
                      exception.addException(new IllegalStateException(other + "(" + other.getTraitSet()+ ") and/or its child "
                        + ((WriterCommitterPrel) other).getInput() + "(" + ((WriterCommitterPrel) other).getInput().getTraitSet() + ") are not SINGLETON"));
                    }
                  }
                  return visitChildren(other);
                }
              });
            }
          }
        }
      };
    }

    @Override
    public void execCompletion(UserResult result) {
      if (result.hasException()) {
        exception.addException(result.getException());
      }
      latch.countDown();
    }

    void waitForCompletion() throws Exception {
      latch.await();
      exception.throwNoClearRuntime();
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    test("alter session set \"planner.slice_target\" = 1");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    test("alter session set \"planner.slice_target\" = " + ExecConstants.SLICE_TARGET_DEFAULT);
  }

  @Test
  public void simpleQuery() throws Exception {
    String storeTblName = "simpleQuery";
    String query = "SELECT n_nationkey, COUNT(*) AS \"total\" FROM cp.\"tpch/nation.parquet\" GROUP BY n_nationkey";

    String table = localQueryHelper(query, storeTblName);

    // Now try to query from the place where the above query results are stored
    testBuilder()
        .sqlQuery(
            format("SELECT * FROM TABLE(%s(type => 'arrow')) ORDER BY n_nationkey LIMIT 2",
                table))
        .unOrdered()
        .baselineColumns("n_nationkey", "total")
        .baselineValues(0, 1L)
        .baselineValues(1, 1L)
        .go();

    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), storeTblName));
  }

  @Test
  public void showTables() throws Exception {
    String storeTblName = "showTables";
    String query = "SHOW TABLES IN INFORMATION_SCHEMA";

    String showTables = localQueryHelper(query, storeTblName);

    // Now try to query from the place where the above query results are stored
    testBuilder()
        .sqlQuery(
            format("SELECT * FROM TABLE(%s(type => 'arrow')) ORDER BY TABLE_NAME", showTables))
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("INFORMATION_SCHEMA", "CATALOGS")
        .baselineValues("INFORMATION_SCHEMA", "COLUMNS")
        .baselineValues("INFORMATION_SCHEMA", "SCHEMATA")
        .baselineValues("INFORMATION_SCHEMA", "TABLES")
        .baselineValues("INFORMATION_SCHEMA", "VIEWS")
        .go();

    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), storeTblName));
  }

  @Test
  public void setOption() throws Exception {
    String storeTblName = "setOption";
    String query = format("ALTER SESSION SET \"%s\"=false", PlannerSettings.HASHAGG.getOptionName());

    String options = localQueryHelper(query, storeTblName);

    // Now try to query from the place where the above query results are stored
    testBuilder()
        .sqlQuery(
            format("SELECT * FROM TABLE(%s(type => 'arrow'))", options))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "planner.enable_hashagg updated.")
        .go();

    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), storeTblName));
  }

  @Test
  public void ctasAndDrop() throws Exception {
    String ctasStoreTblName = "ctas";
    String ctasTableName = "newTable";
    String ctasQuery = format("CREATE TABLE %s.%s AS SELECT * FROM cp.\"region.json\" ORDER BY region_id LIMIT 2", TEMP_SCHEMA, ctasTableName);

    final String ctas = localQueryHelper(ctasQuery, ctasStoreTblName);

    // Now try to query from the place where the above query results are stored
    testBuilder()
        .sqlQuery(
            format("SELECT count(*) as cnt FROM TABLE(%s(type => 'arrow'))", ctas))
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(1L)
        .go();

    String dropTableStoreTblName = "drop";
    String dropTableQuery = format("DROP TABLE %s.%s", TEMP_SCHEMA, ctasTableName);
    final String drop = localQueryHelper(dropTableQuery, dropTableStoreTblName);

    // Now try to query from the place where the above query results are stored
    testBuilder()
        .sqlQuery(
            format("SELECT * FROM TABLE(%s(type => 'arrow'))", drop))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Table [dfs_test.newTable] dropped")
        .go();

    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), ctasStoreTblName));
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), dropTableStoreTblName));
  }

  @Test
  public void explain() throws Exception {
    String storeTblName = "explain";
    String query = "EXPLAIN PLAN FOR SELECT * FROM INFORMATION_SCHEMA.CATALOGS";

    final String explain = localQueryHelper(query, storeTblName);

    // Now try to query from the place where the above query results are stored
    testBuilder()
        .sqlQuery(
            format("SELECT count(*) as cnt FROM TABLE(%s(type => 'arrow'))", explain))
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(1L)
        .go();

    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), storeTblName));
  }

  @Test
  public void withParallelWriters() throws Exception {
    final String newTblName = "ctasSingleCommitter";
    final String testWorkingPath = TestTools.getWorkingPath();
    final String parquetFiles = testWorkingPath + "/src/test/resources/parquet/4203_corrupt_dates/fewtypes_datepartition";
    final String ctasQuery = String.format("CREATE TABLE %s.%s AS SELECT * from dfs.\"" + parquetFiles + "\"", TEMP_SCHEMA, newTblName);

    localQueryHelper(ctasQuery, newTblName, true);
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
  }

  private static String localQueryHelper(String query, String storeTblName) throws Exception {
    return localQueryHelper(query, storeTblName, false);
  }

  private static String localQueryHelper(String query, String storeTblName, boolean checkWriterDistributionTrait) throws Exception {
    LocalQueryExecutor localQueryExecutor = getLocalQueryExecutor();

    RunQuery queryCmd = RunQuery
        .newBuilder()
        .setType(UserBitShared.QueryType.SQL)
        .setSource(SubmissionSource.LOCAL)
        .setPlan(query)
        .build();

    String queryResultsStorePath = format("%s.\"%s\"", TEMP_SCHEMA, storeTblName);
    LocalExecutionConfig config = LocalExecutionConfig.newBuilder()
        .setEnableLeafLimits(false)
        .setFailIfNonEmptySent(false)
        .setUsername(StandardSystemProperty.USER_NAME.value())
        .setSqlContext(Collections.<String>emptyList())
        .setInternalSingleThreaded(false)
        .setQueryResultsStorePath(queryResultsStorePath)
        .setAllowPartitionPruning(true)
        .setExposeInternalSources(false)
        .setSubstitutionSettings(SubstitutionSettings.of())
        .build();

    TestQueryObserver queryObserver = new TestQueryObserver(checkWriterDistributionTrait);
    localQueryExecutor.submitLocalQuery(ExternalIdHelper.generateExternalId(), queryObserver, queryCmd, false, config, false, null);

    queryObserver.waitForCompletion();

    return toTableName(TEMP_SCHEMA, storeTblName, queryObserver.getAttemptId());
  }

  private static String toTableName(String schema1, String schema2, final AttemptId id) {
    return PathUtils.constructFullPath(Arrays.asList(schema1, schema2, QueryIdHelper.getQueryId(id.toQueryId())));
  }
}
