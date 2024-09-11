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
 * <p>Note: Contains all tests in VacuumTests.
 */
public class ITVacuum extends ITDmlQueryBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test
  // variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Override
  @Before
  public void before() throws Exception {
    test("USE %s", SOURCE);
  }

  @Test
  public void testMalformedVacuumQueries() throws Exception {
    ExpireSnapshotsTests.testMalformedVacuumQueries(SOURCE);
  }

  @Test
  public void testSimpleExpireOlderThanRetainLastUsingEqual() throws Exception {
    ExpireSnapshotsTests.testSimpleExpireOlderThanRetainLastUsingEqual(allocator, SOURCE);
  }

  @Test
  public void testSimpleExpireOlderThan() throws Exception {
    ExpireSnapshotsTests.testSimpleExpireOlderThan(allocator, SOURCE);
  }

  @Test
  public void testExpireOlderThan() throws Exception {
    ExpireSnapshotsTests.testExpireOlderThan(allocator, SOURCE);
  }

  @Test
  public void testExpireRetainLast() throws Exception {
    ExpireSnapshotsTests.testExpireRetainLast(allocator, SOURCE);
  }

  @Test
  public void testRetainLastWithExpireOlderThan() throws Exception {
    ExpireSnapshotsTests.testRetainLastWithExpireOlderThan(allocator, SOURCE);
  }

  @Test
  public void testExpireDataFilesCleanup() throws Exception {
    ExpireSnapshotsTests.testExpireDataFilesCleanup(allocator, SOURCE);
  }

  @Test
  public void testExpireOlderThanWithRollback() throws Exception {
    ExpireSnapshotsTests.testExpireOlderThanWithRollback(allocator, SOURCE);
  }

  @Test
  public void testExpireOnTableWithPartitions() throws Exception {
    ExpireSnapshotsTests.testExpireOnTableWithPartitions(allocator, SOURCE);
  }

  @Test
  public void testExpireOnEmptyTableNoSnapshots() throws Exception {
    ExpireSnapshotsTests.testExpireOnEmptyTableNoSnapshots(allocator, SOURCE);
  }

  @Test
  public void testRetainZeroSnapshots() throws Exception {
    ExpireSnapshotsTests.testRetainZeroSnapshots(SOURCE);
  }

  @Test
  public void testInvalidTimestampLiteral() throws Exception {
    ExpireSnapshotsTests.testInvalidTimestampLiteral(SOURCE);
  }

  @Test
  public void testEmptyTimestamp() throws Exception {
    ExpireSnapshotsTests.testEmptyTimestamp(SOURCE);
  }

  @Test
  public void testExpireDatasetRefreshed() throws Exception {
    ExpireSnapshotsTests.testExpireDatasetRefreshed(allocator, SOURCE);
  }

  @Test
  public void testUnparseSqlVacuum() throws Exception {
    ExpireSnapshotsTests.testUnparseSqlVacuum(SOURCE);
  }

  @Test
  public void testExpireOnTableOneSnapshot() throws Exception {
    ExpireSnapshotsTests.testExpireOnTableOneSnapshot(allocator, SOURCE);
  }

  @Test
  public void testRetainMoreSnapshots() throws Exception {
    ExpireSnapshotsTests.testRetainMoreSnapshots(allocator, SOURCE);
  }

  @Test
  public void testRetainAllSnapshots() throws Exception {
    ExpireSnapshotsTests.testRetainAllSnapshots(allocator, SOURCE);
  }

  @Test
  public void testGCDisabled() throws Exception {
    ExpireSnapshotsTests.testGCDisabled(allocator, SOURCE);
  }

  @Test
  public void testMinSnapshotsTablePropOverride() throws Exception {
    ExpireSnapshotsTests.testMinSnapshotsTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testMinSnapshotsAggressiveTablePropOverride() throws Exception {
    ExpireSnapshotsTests.testMinSnapshotsAggressiveTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testSnapshotAgeTablePropOverride() throws Exception {
    ExpireSnapshotsTests.testSnapshotAgeTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testSnapshotAgeAggressiveTablePropOverride() throws Exception {
    ExpireSnapshotsTests.testSnapshotAgeAggressiveTablePropOverride(allocator, SOURCE);
  }

  @Test
  public void testExpireSnapshotsWithTensOfSnapshots() throws Exception {
    ExpireSnapshotsTests.testExpireSnapshotsWithTensOfSnapshots(allocator, SOURCE);
  }

  @Test
  public void testExpireSnapshotsOnNonMainBranch() throws Exception {
    ExpireSnapshotsTests.testExpireSnapshotsOnNonMainBranch(SOURCE, allocator);
  }

  @Test
  public void testExpireSnapshotsOnDifferentBranches() throws Exception {
    ExpireSnapshotsTests.testExpireSnapshotsOnDifferentBranches(SOURCE, allocator);
  }

  @Test
  public void testVacuumOnExternallyCreatedTable() throws Exception {
    ExpireSnapshotsTests.testVacuumOnExternallyCreatedTable(allocator, SOURCE);
  }

  /** Remove orphan files tests */
  @Test
  public void testMalformedVacuumRemoveOrphanFileQueries() throws Exception {
    RemoveOrphanFilesTests.testMalformedVacuumRemoveOrphanFileQueries(SOURCE);
  }

  @Test
  public void testSimpleRemoveOrphanFiles() throws Exception {
    RemoveOrphanFilesTests.testSimpleRemoveOrphanFiles(allocator, SOURCE);
  }

  @Test
  public void testSimpleRemoveOrphanFilesUsingEqual() throws Exception {
    RemoveOrphanFilesTests.testSimpleRemoveOrphanFilesUsingEqual(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesNotDeleteValidFiles() throws Exception {
    RemoveOrphanFilesTests.testRemoveOrphanFilesNotDeleteValidFiles(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesDeleteOrphanFiles() throws Exception {
    RemoveOrphanFilesTests.testRemoveOrphanFilesDeleteOrphanFiles(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesWithPartitions() throws Exception {
    RemoveOrphanFilesTests.testRemoveOrphanFilesWithPartitions(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesUsingDefaultMinFileAge() throws Exception {
    RemoveOrphanFilesTests.testRemoveOrphanFilesUsingDefaultMinFileAge(allocator, SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesInvalidTimestampLiteral() throws Exception {
    RemoveOrphanFilesTests.testRemoveOrphanFilesInvalidTimestampLiteral(SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesWithLocationClause() throws Exception {
    RemoveOrphanFilesTests.testRemoveOrphanFilesWithLocationClause(allocator, SOURCE);
  }

  @Test
  public void testUnparseRemoveOrphanFilesQuery() throws Exception {
    RemoveOrphanFilesTests.testUnparseRemoveOrphanFilesQuery(SOURCE);
  }

  @Test
  public void testRemoveOrphanFilesHybridlyGeneratedTable() throws Exception {
    RemoveOrphanFilesTests.testRemoveOrphanFilesHybridlyGeneratedTable(allocator, SOURCE);
  }
}
