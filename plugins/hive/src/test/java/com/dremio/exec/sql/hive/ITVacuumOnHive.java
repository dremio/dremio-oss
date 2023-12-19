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
package com.dremio.exec.sql.hive;

import static com.dremio.exec.store.hive.HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME;

import org.junit.Test;

import com.dremio.exec.planner.sql.VacuumTests;

/**
 * Runs test cases on the local Hive-based source.
 *
 * Note: Contains a basic test *and* (when required) with Hive-specific tests.
 */
public class ITVacuumOnHive extends DmlQueryOnHiveTestBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = HIVE_TEST_PLUGIN_NAME;

  @Test
  public void testSimpleExpireOlderThanRetainLastUsingEqual() throws Exception {
    VacuumTests.testSimpleExpireOlderThanRetainLastUsingEqual(allocator, SOURCE);
  }

  @Test
  public void testSimpleExpireOlderThan() throws Exception {
    VacuumTests.testSimpleExpireOlderThan(allocator, SOURCE);
  }

  @Test
  public void testExpireOnTableOneSnapshot() throws Exception {
    VacuumTests.testExpireOnTableOneSnapshot(allocator, SOURCE);
  }

  @Test
  public void testRetainMoreSnapshots() throws Exception {
    VacuumTests.testRetainMoreSnapshots(allocator, SOURCE);
  }

  @Test
  public void testRetainAllSnapshots() throws Exception {
    VacuumTests.testRetainAllSnapshots(allocator, SOURCE);
  }
}
