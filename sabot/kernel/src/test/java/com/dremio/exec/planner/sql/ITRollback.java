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
 * Note: Contains all tests in RollbackTests.
 */
public class ITRollback extends ITDmlQueryBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Override
  @Before
  public void before() throws Exception {
    test("USE %s", SOURCE);
  }

  @Test
  public void testMalformedRollbackQueries() throws Exception {
    RollbackTests.testMalformedRollbackQueries(SOURCE);
  }

  @Test
  public void testSimpleRollbackToTimestamp() throws Exception {
    RollbackTests.testSimpleRollbackToTimestamp(allocator, SOURCE);
  }

  @Test
  public void testRollbackToSnapshot() throws Exception {
    RollbackTests.testRollbackToSnapshot(allocator, SOURCE);
  }

  @Test
  public void testRollbackWithPartialTablePath() throws Exception{
    RollbackTests.testRollbackWithPartialTablePath(allocator, SOURCE);
  }

  @Test
  public void testRollbackToTimestamp() throws Exception {
    RollbackTests.testRollbackToTimestamp(allocator, SOURCE);
  }

  @Test
  public void testRollbackToTimestampMatchSnapshot() throws Exception {
    RollbackTests.testRollbackToTimestampMatchSnapshot(allocator, SOURCE);
  }

  @Test
  public void testRollbackToCurrentSnapshot() throws Exception {
    RollbackTests.testRollbackToCurrentSnapshot(allocator, SOURCE);
  }

  @Test
  public void testRollbackToInvalidSnapshot() throws Exception {
    RollbackTests.testRollbackToInvalidSnapshot(SOURCE);
  }

  @Test
  public void testRollbackToTimestampBeforeFirstSnapshot() throws Exception {
    RollbackTests.testRollbackToTimestampBeforeFirstSnapshot(SOURCE);
  }

  @Test
  public void testRollbackToNonAncestorSnapshot() throws Exception {
    RollbackTests.testRollbackToNonAncestorSnapshot(allocator, SOURCE);
  }

  @Test
  public void testRollbackUseInvalidTablePath() throws Exception {
    RollbackTests.testRollbackUseInvalidTablePath(SOURCE);
  }

  @Test
  public void testUnparseSqlRollbackTable() throws Exception {
    RollbackTests.testUnparseSqlRollbackTable(SOURCE);
  }

  @Test
  public void testInvalidTimestampLiteral() throws Exception {
    RollbackTests.testInvalidTimestampLiteral(SOURCE);
  }

  @Test
  public void testInvalidSnapshotIdLiteral() throws Exception {
    RollbackTests.testInvalidSnapshotIdLiteral(SOURCE);
  }

  @Test
  public void testRollbackPartialPath() throws Exception {
    RollbackTests.testRollbackPartialPath(allocator, SOURCE);
  }
}
