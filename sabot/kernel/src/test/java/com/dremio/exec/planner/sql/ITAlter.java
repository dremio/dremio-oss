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

import org.junit.Before;
import org.junit.Test;

/**
 * Runs test cases on the local filesystem-based Hadoop source.
 *
 * <p>Note: Contains all tests in AlterTests.
 */
public class ITAlter extends ITDmlQueryBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test
  // variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Override
  @Before
  public void before() throws Exception {
    test("USE %s", SOURCE);
  }

  @Test
  public void testAddColumnsNameWithDot() throws Exception {
    AlterTests.testAddColumnsNameWithDot(allocator, SOURCE);
  }

  @Test
  public void testChangeColumnNameWithDot() throws Exception {
    AlterTests.testChangeColumnNameWithDot(allocator, SOURCE);
  }
}
