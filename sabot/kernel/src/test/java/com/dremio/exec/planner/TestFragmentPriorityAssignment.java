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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
    assertThat(priorityAssigner.getFragmentWeight(0), is(1));
    assertThat(priorityAssigner.getFragmentWeight(1), is(1));
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
    assertThat(priorityAssigner.getFragmentWeight(1), is(2));
    assertThat(priorityAssigner.getFragmentWeight(2), is(1));
    assertThat(priorityAssigner.getFragmentWeight(3), is(2));
  }

  private PhysicalPlan createPlan(String sql) throws Exception {
    final SabotContext context = createSabotContext(
      () -> OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.slice_target", 1),
      () -> OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "planner.assign_priority", true)
    );
    final QueryContext queryContext = createContext(context);
    return createPlan(sql, queryContext);
  }
}
