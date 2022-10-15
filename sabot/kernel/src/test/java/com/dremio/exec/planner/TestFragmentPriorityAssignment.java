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
package com.dremio.exec.planner;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.exec.PlanOnlyTestBase;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.fragment.AssignFragmentPriorityVisitor;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionValue;

/**
 * Tests assignment priorities of queries with single and multiple fragments.
 */
public class TestFragmentPriorityAssignment extends PlanOnlyTestBase {
  @Test
  public void testSingleFragmentAssignment() throws Exception {
    final String sql = "SELECT city from cp.\"sample/samples-samples-dremio-com-zips-json.json\"";
    final PhysicalPlan plan = createPlan(sql);
    AssignFragmentPriorityVisitor priorityAssigner = new AssignFragmentPriorityVisitor();
    plan.getRoot().accept(priorityAssigner, null);
    assertEquals(1, priorityAssigner.getFragmentWeight(0));
    assertEquals(1, priorityAssigner.getFragmentWeight(1));
  }

  @Test
  public void testMultiFragmentAssignment() throws Exception {
    final String yelpTable = TEMP_SCHEMA + ".\"yelp\"";
    final String sql = "SELECT nested_0.review_id AS review_id, nested_0.user_id AS user_id, nested_0.votes AS votes," +
      " nested_0.stars AS stars, join_business.business_id AS business_id0, " +
      " join_business.neighborhoods AS neighborhoods, join_business.city AS city, " +
      " join_business.latitude AS latitude, join_business.review_count AS review_count, " +
      " join_business.full_address AS full_address, join_business.stars AS stars0, " +
      " join_business.categories AS categories, join_business.state AS state, " +
      " join_business.longitude AS longitude\n" +
      "FROM (\n" +
      "  SELECT review_id, user_id, votes, stars, business_id\n" +
      "  FROM cp.\"yelp_review.json\" where 1 = 0\n" +
      ") nested_0\n" +
      " FULL JOIN " + yelpTable + " AS join_business ON nested_0.business_id = join_business.business_id";
    final PhysicalPlan plan = createPlan(sql);
    AssignFragmentPriorityVisitor priorityAssigner = new AssignFragmentPriorityVisitor();
    plan.getRoot().accept(priorityAssigner, null);
    assertEquals(2, priorityAssigner.getFragmentWeight(1));
    assertEquals(1, priorityAssigner.getFragmentWeight(2));
    assertEquals(2, priorityAssigner.getFragmentWeight(3));
  }

  @Test
  public void testComplexMultiFragmentAssignment() throws Exception {
    final String yelpTable = TEMP_SCHEMA + ".\"yelp\"";
    final String sql = "SELECT * from " +
      "(SELECT review_id, user_id, stars, name, sumreview," +
      " rank() over (partition by user_id order by sumreview desc) rk" +
      " FROM (\n" +
      "  SELECT x.review_id as review_id, x.user_id as user_id, x.stars as stars, u.name as name, " +
      "  sum(coalesce(y.review_count*y.stars,0)) sumreview" +
      "  FROM cp.\"yelp_review.json\" x, " + yelpTable + " y, cp.\"yelp_user_data.json\" z, " +
      "  cp.\"user.json\" u \n" +
      " where x.business_id = y.business_id and x.user_id = z.user_id and z.name = u.name \n" +
      " group by  rollup(review_id, user_id, stars, name))dw1) dw2" +
      " where rk <= 100 " +
      " order by review_id, user_id, sumreview, rk" +
      " limit 100;";
    final PhysicalPlan plan = createPlan(sql);
    AssignFragmentPriorityVisitor priorityAssigner = new AssignFragmentPriorityVisitor();
    plan.getRoot().accept(priorityAssigner, null);
    assertEquals(5, priorityAssigner.getFragmentWeight(0));
    assertEquals(5, priorityAssigner.getFragmentWeight(7));
    assertEquals(1, priorityAssigner.getFragmentWeight(8));
  }

  private PhysicalPlan createPlan(String sql) throws Exception {
    final SabotContext context = createSabotContext(
      () -> OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.slice_target", 1),
      () -> OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "planner.enable_join_optimization", false),
      () -> OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "planner.assign_priority", true)
    );
    final QueryContext queryContext = createContext(context);
    return createPlan(sql, queryContext);
  }
}
