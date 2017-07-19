/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.exec.planner.PlannerPhase.JOIN_PLANNING;
import static com.dremio.exec.planner.PlannerPhase.LOGICAL;
import static com.dremio.service.job.proto.JoinType.Inner;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.planner.observer.RemoteAttemptObserver;
import com.dremio.exec.planner.observer.RemoteQueryObserverFactory;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.work.AttemptId;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.job.proto.JoinConditionInfo;
import com.dremio.service.job.proto.JoinInfo;
import com.dremio.service.jobs.metadata.JoinExtractor;
import com.google.common.collect.ImmutableList;

/**
 * this Tests the Join Extractor
 */
public class TestJoinExtractor extends BaseTestQuery {

  public static final ImmutableList<String> REGION_PARQUET = ImmutableList.of("cp","tpch/region.parquet");
  public static final ImmutableList<String> NATION_PARQUET = ImmutableList.of("cp","tpch/nation.parquet");

  private List<JoinInfo> definitions;

  @Before
  public void beforeTest() {
    definitions = new ArrayList<>();
    nodes[0].getBindingCreator().replace(QueryObserverFactory.class, new QueryObserverFactory() {
      @Override
      public QueryObserver createNewQueryObserver(final ExternalId id, UserSession session, final UserResponseHandler connection) {
        return new RemoteQueryObserverFactory.RemoteQueryObserver(id, connection) {

          @Override
          public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
            return new RemoteAttemptObserver(id, connection) {
              @Override
              public void planRelTransform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
                if (definitions.isEmpty()) {
                  if (phase == LOGICAL) {
                    definitions = JoinExtractor.getJoins(before);
                  } else if (phase == JOIN_PLANNING) {
                    definitions = JoinExtractor.getJoins(before);
                  }
                }
              }
            };
          }
        };
      }
    });
  }

  @Test
  public void joinExtractionTwoTableWithFilter() throws Exception {
    testNoResult("select * from  cp.\"tpch/region.parquet\" r join (select n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey = 1) n on r.r_regionkey = n.n_regionkey");
    // the column in the join condition can be reduced to a constant due to the filter in the subquery, thus we expect
    // the join extractor to not be able to find a mathcing set of columns from both tables to create a new recommendation
    assertEquals(0, definitions.size());
  }

  @Test
  public void joinExtractionTwoTable() throws Exception {
    testNoResult("select * from  cp.\"tpch/region.parquet\" r join cp.\"tpch/nation.parquet\" n on r.r_regionkey = n.n_regionkey");
    assertEquals(1, definitions.size());
    JoinInfo join = definitions.get(0);
    assertEquals(Inner, join.getJoinType());
    assertEquals(join.getConditionsList().toString(), 1, join.getConditionsList().size());
    JoinConditionInfo joinConditionInfo = join.getConditionsList().get(0);
    assertEquals(REGION_PARQUET, joinConditionInfo.getTableAList());
    assertEquals("r_regionkey", joinConditionInfo.getColumnA());
    assertEquals(NATION_PARQUET, joinConditionInfo.getTableBList());
    assertEquals("n_regionkey", joinConditionInfo.getColumnB());
  }

  @Test
  public void joinExtraction() throws Exception {
    testNoResult("select * from  cp.\"tpch/region.parquet\" r1 join cp.\"tpch/region.parquet\" r2 on r1.r_regionkey = r2.r_regionkey");
    assertEquals(1, definitions.size());
    JoinInfo join = definitions.get(0);
    assertEquals(Inner, join.getJoinType());
    assertEquals(join.getConditionsList().toString(), 1, join.getConditionsList().size());
    JoinConditionInfo joinConditionInfo = join.getConditionsList().get(0);
    assertEquals(REGION_PARQUET, joinConditionInfo.getTableAList());
    assertEquals("r_regionkey", joinConditionInfo.getColumnA());
    assertEquals(REGION_PARQUET, joinConditionInfo.getTableBList());
    assertEquals("r_regionkey", joinConditionInfo.getColumnB());
  }

  @Test
  public void joinExtractionAnd() throws Exception {
    testNoResult("select * from  cp.\"tpch/region.parquet\" r1 join cp.\"tpch/region.parquet\" r2 on r1.r_regionkey = r2.r_regionkey AND r1.r_name = r2.r_name ");
    assertEquals(1, definitions.size());
    JoinInfo join = definitions.get(0);
    assertEquals(Inner, join.getJoinType());
    assertEquals(join.getConditionsList().toString(), 2, join.getConditionsList().size());
    JoinConditionInfo joinConditionInfo0 = join.getConditionsList().get(0);
    assertEquals(REGION_PARQUET, joinConditionInfo0.getTableAList());
    assertEquals("r_regionkey", joinConditionInfo0.getColumnA());
    assertEquals(REGION_PARQUET, joinConditionInfo0.getTableBList());
    assertEquals("r_regionkey", joinConditionInfo0.getColumnB());
    JoinConditionInfo joinConditionInfo1 = join.getConditionsList().get(1);
    assertEquals(REGION_PARQUET, joinConditionInfo1.getTableAList());
    assertEquals("r_name", joinConditionInfo1.getColumnA());
    assertEquals(REGION_PARQUET, joinConditionInfo1.getTableBList());
    assertEquals("r_name", joinConditionInfo1.getColumnB());
  }

  @Test
  public void joinExtractionNotStar() throws Exception {
    testNoResult("select r1.r_name as name, r2.r_comment as comment  from  cp.\"tpch/region.parquet\" r1 join cp.\"tpch/region.parquet\" r2 on r1.r_regionkey = r2.r_regionkey");
    assertEquals(1, definitions.size());
    JoinInfo join = definitions.get(0);
    assertEquals(Inner, join.getJoinType());
    assertEquals(join.getConditionsList().toString(), 1, join.getConditionsList().size());
    JoinConditionInfo joinConditionInfo = join.getConditionsList().get(0);
    assertEquals(REGION_PARQUET, joinConditionInfo.getTableAList());
    assertEquals("r_regionkey", joinConditionInfo.getColumnA());
    assertEquals(REGION_PARQUET, joinConditionInfo.getTableBList());
    assertEquals("r_regionkey", joinConditionInfo.getColumnB());
  }

  @Test
  public void joinExtractionWhere() throws Exception {
    testNoResult("select * from  cp.\"tpch/region.parquet\" r, cp.\"tpch/nation.parquet\" n where r.r_regionkey = n.n_regionkey");
    assertEquals(1, definitions.size());
    JoinInfo join = definitions.get(0);
    assertEquals(Inner, join.getJoinType());
    assertEquals(join.getConditionsList().toString(), 1, join.getConditionsList().size());
    JoinConditionInfo joinConditionInfo = join.getConditionsList().get(0);
    assertEquals(REGION_PARQUET, joinConditionInfo.getTableAList());
    assertEquals("r_regionkey", joinConditionInfo.getColumnA());
    assertEquals(NATION_PARQUET, joinConditionInfo.getTableBList());
    assertEquals("n_regionkey", joinConditionInfo.getColumnB());
  }
}
