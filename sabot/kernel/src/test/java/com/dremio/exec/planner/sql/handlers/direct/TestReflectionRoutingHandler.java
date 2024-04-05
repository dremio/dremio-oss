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
package com.dremio.exec.planner.sql.handlers.direct;

import static org.junit.Assert.assertTrue;

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserException;
import org.junit.Test;

/** Test reflection routing handler on CE */
public class TestReflectionRoutingHandler extends PlanTestBase {

  /** Should fail since CE does not support reflection routing */
  @Test
  public void testRoutingNotSupportedEE() throws Exception {
    try {
      test("ALTER TABLE T1 ROUTE ALL REFLECTIONS TO QUEUE Q1");
    } catch (UserException e) {
      assertTrue(
          e.getMessage().contains("This command is not supported in this edition of Dremio."));
    }
  }

  @Test
  public void testRoutingNotSupportedDCS() throws Exception {
    try {
      test("ALTER TABLE T1 ROUTE ALL REFLECTIONS TO ENGINE E1");
    } catch (UserException e) {
      assertTrue(
          e.getMessage().contains("This command is not supported in this edition of Dremio."));
    }
  }
}
