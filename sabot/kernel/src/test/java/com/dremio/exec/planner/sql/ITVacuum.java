
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
 * Note: Contains all tests in VacuumTests.
 */
public class ITVacuum extends ITDmlQueryBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Before
  public void before() throws Exception {
    test("USE %s", SOURCE);
  }

  @Test
  public void testMalformedVacuumQueries() throws Exception {
    VacuumTests.testMalformedVacuumQueries(SOURCE);
  }

  @Test
  public void testSimpleExpireOlderThan() throws Exception {
    VacuumTests.testSimpleExpireOlderThan(allocator, SOURCE);
  }

  @Test
  public void testExpireOlderThan() throws Exception {
    VacuumTests.testExpireOlderThan(allocator, SOURCE);
  }

  @Test
  public void testExpireRetainLast() throws Exception {
    VacuumTests.testExpireRetainLast(allocator, SOURCE);
  }

  @Test
  public void testRetainLastWithExpireOlderThan() throws Exception {
    VacuumTests.testRetainLastWithExpireOlderThan(allocator, SOURCE);
  }

  @Test
  public void testExpireDataFilesCleanup() throws Exception {
    VacuumTests.testExpireDataFilesCleanup(allocator, SOURCE);
  }

  @Test
  public void testExpireOlderThanWithRollback() throws Exception {
    VacuumTests.testExpireOlderThanWithRollback(allocator, SOURCE);
  }

  @Test
  public void testRetainZeroSnapshots() throws Exception {
    VacuumTests.testRetainZeroSnapshots(SOURCE);
  }

  @Test
  public void testInvalidTimestampLiteral() throws Exception {
    VacuumTests.testInvalidTimestampLiteral(SOURCE);
  }

  @Test
  public void testEmptyTimestamp() throws Exception {
    VacuumTests.testEmptyTimestamp(SOURCE);
  }

  @Test
  public void testExpireDatasetRefreshed() throws Exception {
    VacuumTests.testExpireDatasetRefreshed(allocator, SOURCE);
  }

  @Test
  public void testUnparseSqlVacuum() throws Exception {
    VacuumTests.testUnparseSqlVacuum(SOURCE);
  }
}
