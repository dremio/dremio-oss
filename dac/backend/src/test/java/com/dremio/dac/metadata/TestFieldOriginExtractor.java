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
package com.dremio.dac.metadata;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.planner.observer.RemoteAttemptObserver;
import com.dremio.exec.planner.observer.RemoteQueryObserverFactory;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.jobs.metadata.FieldOriginExtractor;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;

/**
 * Test the FieldOriginExtractor
 */
public class TestFieldOriginExtractor extends BaseTestQuery {

  public static final ImmutableList<String> REGION_PARQUET = ImmutableList.of("cp","tpch/region.parquet");
  public static final ImmutableList<String> NATION_PARQUET = ImmutableList.of("cp","tpch/nation.parquet");

  private static List<FieldOrigin> fields;

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    SABOT_NODE_RULE.register(new AbstractModule() {
      @Override
      protected void configure() {
        bind(QueryObserverFactory.class).toInstance(new QueryObserverFactory() {
          @Override
          public QueryObserver createNewQueryObserver(final ExternalId id, UserSession session, final UserResponseHandler connection) {
            return new RemoteQueryObserverFactory.RemoteQueryObserver(id, connection) {

              @Override
              public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
                return new RemoteAttemptObserver(id, connection) {
                  private RelDataType rowType;

                  @Override
                  public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
                    this.rowType = rowType;
                  }

                  @Override
                  public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after,
                                               long millisTaken, Map<String, Long> timeBreakdownPerRule) {
                    if (phase == PlannerPhase.LOGICAL) {
                      fields = FieldOriginExtractor.getFieldOrigins(before, rowType);
                    }
                  }
                };
              }
            };
          }
        });
      }
    });

    BaseTestQuery.setupDefaultTestCluster();
  }

  @Before
  public void beforeTest() {
    fields = null;
  }

  @Test
  public void parentExtraction() throws Exception {
    testNoResult("select * from  cp.\"tpch/region.parquet\" r1 join cp.\"tpch/region.parquet\" r2 on r1.r_regionkey = r2.r_regionkey");
    assertEquals(6, fields.size());

    validateField(0, "r_regionkey", REGION_PARQUET, "r_regionkey");
    validateField(1, "r_name", REGION_PARQUET, "r_name");
    validateField(2, "r_comment", REGION_PARQUET, "r_comment");
    validateField(3, "r_regionkey0", REGION_PARQUET, "r_regionkey");
    validateField(4, "r_name0", REGION_PARQUET, "r_name");
    validateField(5, "r_comment0", REGION_PARQUET, "r_comment");

  }

  private void validateField(int i, String name, List<String> table, String col) {
    assertEquals(name, fields.get(i).getName());
    Origin oi = fields.get(i).getOriginsList().iterator().next();
    assertEquals(col, oi.getColumnName());
    assertEquals(table, oi.getTableList());
  }

  @Test
  public void parentExtractionNotStar() throws Exception {
    testNoResult("select r1.r_name as name, r2.r_comment as comment from  cp.\"tpch/region.parquet\" r1 join cp.\"tpch/region.parquet\" r2 on r1.r_regionkey = r2.r_regionkey");
    assertEquals(2, fields.size());

    validateField(0, "name", REGION_PARQUET, "r_name");
    validateField(1, "comment", REGION_PARQUET, "r_comment");
  }

  @Test
  public void windowFunction() throws Exception {
    testNoResult("select N_REGIONKEY, AVG(N_NATIONKEY) OVER (PARTITION BY N_REGIONKEY) as w1," +
      "MIN(N_NATIONKEY) OVER (PARTITION BY N_REGIONKEY) as w2" +
      " FROM cp.\"tpch/nation" +
      ".parquet\"");
    assertEquals(3, fields.size());

    validateField(0, "N_REGIONKEY", NATION_PARQUET, "n_regionkey");
    validateField(1, "w1", NATION_PARQUET, "n_nationkey");
    validateField(2, "w2", NATION_PARQUET, "n_nationkey");
  }

  @Test
  public void windowMultiGroupFunction() throws Exception {
    testNoResult("select N_REGIONKEY, AVG(N_NATIONKEY) OVER (PARTITION BY N_REGIONKEY) as w1," +
      "MIN(N_REGIONKEY) OVER (PARTITION BY N_NATIONKEY) as w2" +
      " FROM cp.\"tpch/nation" +
      ".parquet\"");
    assertEquals(3, fields.size());

    validateField(0, "N_REGIONKEY", NATION_PARQUET, "n_regionkey");
    validateField(1, "w1", NATION_PARQUET, "n_nationkey");
    validateField(2, "w2", NATION_PARQUET, "n_regionkey");
  }

  @Test
  public void windowFunctionWithConstants() throws Exception {
    testNoResult("select N_REGIONKEY, AVG(N_NATIONKEY + 1) OVER (PARTITION BY N_REGIONKEY + 1) as w1 FROM cp.\"tpch/nation.parquet\"");
    assertEquals(2, fields.size());

    validateField(0, "N_REGIONKEY", NATION_PARQUET, "n_regionkey");
    validateField(1, "w1", NATION_PARQUET, "n_nationkey");
  }
}
