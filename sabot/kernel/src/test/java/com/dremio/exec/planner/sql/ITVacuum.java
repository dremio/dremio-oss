
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

  @Override
  @Before
  public void before() throws Exception {
    test("USE %s", SOURCE);
  }

  @Test
  public void testMalformedVacuumQueries() throws Exception {
    VacuumTests.testMalformedVacuumQueries(SOURCE);
  }

  @Test
  public void testSimpleExpireOlderThanRetainLastUsingEqual() throws Exception {
    VacuumTests.testSimpleExpireOlderThanRetainLastUsingEqual(allocator, SOURCE);
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
  public void testExpireOnTableWithPartitions() throws Exception {
    VacuumTests.testExpireOnTableWithPartitions(allocator, SOURCE);
  }

  @Test
  public void testExpireOnEmptyTableNoSnapshots() throws Exception {
    VacuumTests.testExpireOnEmptyTableNoSnapshots(allocator, SOURCE);
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

  @Test
  public void testGCDisabled() throws Exception {
    VacuumTests.testGCDisabled(allocator, SOURCE);
  }

  @Test
  public void testMinSnapshotsTablePropOverride() throws Exception {
    VacuumTests.testMinSnapshotsTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testMinSnapshotsAggressiveTablePropOverride() throws Exception {
    VacuumTests.testMinSnapshotsAggressiveTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testSnapshotAgeTablePropOverride() throws Exception {
    VacuumTests.testSnapshotAgeTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testSnapshotAgeAggressiveTablePropOverride() throws Exception {
    VacuumTests.testSnapshotAgeAggressiveTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testExpireSnapshotsWithTensOfSnapshots() throws Exception {
    VacuumTests.testExpireSnapshotsWithTensOfSnapshots(allocator, SOURCE);
  }

  @Test
  public void testExpireSnapshotsOnNonMainBranch() throws Exception {
    VacuumTests.testExpireSnapshotsOnNonMainBranch(SOURCE, allocator);
  }

  @Test
  public void testExpireSnapshotsOnDifferentBranches() throws Exception {
    VacuumTests.testExpireSnapshotsOnDifferentBranches(SOURCE, allocator);
  }

  /** Remove orphan files tests */
  @Test
  public void testMalformedVacuumRemoveOrphanFileQueries() throws Exception {
    VacuumTests.testMalformedVacuumRemoveOrphanFileQueries(SOURCE);
  }

  @Test
  public void testSimpleRemoveOrphanFiles() throws Exception {
    VacuumTests.testSimpleRemoveOrphanFiles(allocator, SOURCE);
  }

  @Test
  public void testSimpleRemoveOrphanFilesUsingEqual() throws Exception {
    VacuumTests.testSimpleRemoveOrphanFilesUsingEqual(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesNotDeleteValidFiles() throws Exception {
    VacuumTests.testRemoveOrphanFilesNotDeleteValidFiles(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesDeleteOrphanFiles() throws Exception {
    VacuumTests.testRemoveOrphanFilesDeleteOrphanFiles(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesInvalidTimestampLiteral() throws Exception {
    VacuumTests.testRemoveOrphanFilesInvalidTimestampLiteral(SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesWithLocationClause() throws Exception {
    VacuumTests.testRemoveOrphanFilesWithLocationClause(allocator, SOURCE);
  }

  @Test
  public void testUnparseRemoveOrphanFilesQuery() throws Exception {
    VacuumTests.testUnparseRemoveOrphanFilesQuery(SOURCE);
  }

  @Test
  public void testVacuumOnExternallyCreatedTable() throws Exception {
    VacuumTests.testVacuumOnExternallyCreatedTable(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesHybridlyGeneratedTable() throws Exception {
    VacuumTests.testRemoveOrphanFilesHybridlyGeneratedTable(allocator, SOURCE);
  }
}
