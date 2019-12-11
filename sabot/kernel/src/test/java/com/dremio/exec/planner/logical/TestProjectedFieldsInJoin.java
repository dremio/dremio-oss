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

package com.dremio.exec.planner.logical;

import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Pointer;
import com.google.common.collect.ImmutableList;

/**
 * Test ProjectedFields property in JoinRel
 */
public class TestProjectedFieldsInJoin extends BaseTestQuery {

  @Test
  public void testSelectStar() throws Exception {
    final String sql = "SELECT * from cp.\"tpch/customer.parquet\" c inner join cp.\"tpch/nation.parquet\" n on c.c_nationkey = n.n_nationkey";
    testQuery(sql, ImmutableList.of(ImmutableBitSet.range(0, 12)));
  }

  @Test
  public void testSimpleJoin() throws Exception {
    final String sql = "SELECT c.c_custkey, c.c_mktsegment from cp.\"tpch/customer.parquet\" c inner join cp.\"tpch/nation.parquet\" n on c.c_nationkey = n.n_nationkey";
    testQuery(sql, ImmutableList.of(ImmutableBitSet.of(0, 2)));
  }

  @Test
  public void testSimpleJoinWithLimit() throws Exception {
    final String sql = "SELECT c.c_custkey, c.c_mktsegment from cp.\"tpch/customer.parquet\" c inner join cp.\"tpch/nation.parquet\" n on c.c_nationkey = n.n_nationkey limit 10";
    testQuery(sql, ImmutableList.of(ImmutableBitSet.of(0, 2)));
  }

  @Test
  public void testSimpleJoinWithKeysNotUsed() throws Exception {
    final String sql = "SELECT c.c_custkey from cp.\"tpch/customer.parquet\" c inner join cp.\"tpch/nation.parquet\" n on c.c_nationkey = n.n_nationkey";
    testQuery(sql, ImmutableList.of(ImmutableBitSet.of(0)));
  }

  @Test
  public void testSimpleJoinWithPartialKeys1() throws Exception {
    final String sql = "SELECT c.c_custkey, c.c_nationkey from cp.\"tpch/customer.parquet\" c inner join cp.\"tpch/nation.parquet\" n on c.c_nationkey = n.n_nationkey";
    testQuery(sql, ImmutableList.of(ImmutableBitSet.of(0, 1)));
  }

  @Test
  public void testSimpleJoinWithPartialKeys2() throws Exception {
    final String sql = "SELECT c.c_custkey, n.n_nationkey from cp.\"tpch/customer.parquet\" c inner join cp.\"tpch/nation.parquet\" n on c.c_nationkey = n.n_nationkey";
    testQuery(sql, ImmutableList.of(ImmutableBitSet.of(0, 2)));
  }

  @Test
  public void testMultipleJoins() throws Exception {
    final String sql = "SELECT c.c_custkey, s.s_name from cp.\"tpch/customer.parquet\" c, cp.\"tpch/nation.parquet\" n, cp.\"tpch/supplier.parquet\" s where c.c_nationkey = n.n_nationkey and s.s_nationkey = n.n_nationkey";
    testQuery(sql, ImmutableList.of(ImmutableBitSet.of(0, 2), ImmutableBitSet.of(0, 2)));
  }

  private void testQuery(String sql, final List<ImmutableBitSet> expected) throws Exception {
    final SabotContext context = getSabotContext();

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
    Pointer<Integer> ind = new Pointer<>(0);
    drel.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof JoinRel) {
          JoinRel join = (JoinRel) other;
          Set<Integer> actualSet = join.getProjectedFields().asSet();
          Set<Integer> expectedSet = expected.get(ind.value).asSet();
          Assert.assertEquals(String.format("Unexpected projectFields. Expected: %s, Actual: %s", expectedSet.toString(), actualSet.toString()), expectedSet, actualSet);
          ind.value++;
        }
        return super.visit(other);
      }
    });
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
