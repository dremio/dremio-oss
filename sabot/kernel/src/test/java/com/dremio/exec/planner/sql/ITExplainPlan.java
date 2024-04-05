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

package com.dremio.exec.planner.sql;

import org.junit.Test;

public class ITExplainPlan extends ITDmlQueryBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test
  // variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testMalformedExplainPlanQueries() throws Exception {
    ExplainPlanTests.testMalformedExplainPlanQueries(SOURCE);
  }

  @Test
  public void testExplainPhysicalPlanOnInsert() throws Exception {
    ExplainPlanTests.testExplainPhysicalPlanOnInsert(SOURCE);
  }

  @Test
  public void testExplainLogicalPlanOnInsert() throws Exception {
    ExplainPlanTests.testExplainLogicalPlanOnInsert(SOURCE);
  }

  @Test
  public void testExplainPlanWithDetailLevelOnInsert() throws Exception {
    ExplainPlanTests.testExplainPlanWithDetailLevelOnInsert(SOURCE);
  }

  @Test
  public void testExplainLogicalPlanOnDelete() throws Exception {
    ExplainPlanTests.testExplainLogicalPlanOnDelete(SOURCE);
  }

  @Test
  public void testExplainPhysicalPlanOnDelete() throws Exception {
    ExplainPlanTests.testExplainPhysicalPlanOnDelete(SOURCE);
  }

  @Test
  public void testExplainPlanWithDetailLevelOnDelete() throws Exception {
    ExplainPlanTests.testExplainPlanWithDetailLevelOnDelete(SOURCE);
  }

  @Test
  public void testExplainLogicalPlanOnUpdate() throws Exception {
    ExplainPlanTests.testExplainLogicalPlanOnUpdate(SOURCE);
  }

  @Test
  public void testExplainPhysicalPlanOnUpdate() throws Exception {
    ExplainPlanTests.testExplainPhysicalPlanOnUpdate(SOURCE);
  }

  @Test
  public void testExplainPlanWithDetailLevelOnUpdate() throws Exception {
    ExplainPlanTests.testExplainPlanWithDetailLevelOnUpdate(SOURCE);
  }

  @Test
  public void testExplainLogicalPlanOnMerge() throws Exception {
    ExplainPlanTests.testExplainLogicalPlanOnMerge(SOURCE);
  }

  @Test
  public void testExplainPhysicalPlanOnMerge() throws Exception {
    ExplainPlanTests.testExplainPhysicalPlanOnMerge(SOURCE);
  }

  @Test
  public void testExplainPlanWithDetailLevelOnMerge() throws Exception {
    ExplainPlanTests.testExplainPlanWithDetailLevelOnMerge(SOURCE);
  }
}
