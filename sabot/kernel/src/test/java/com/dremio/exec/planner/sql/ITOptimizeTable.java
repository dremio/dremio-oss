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

import com.dremio.BaseTestQuery;

/**
 * Test OPTIMIZE TABLE scenarios
 */
public class ITOptimizeTable extends BaseTestQuery {
  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testOnUnPartitioned() throws Exception {
    OptimizeTests.testOnUnPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOnPartitioned() throws Exception {
    OptimizeTests.testOnPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOnUnPartitionedMinInputFilesCriteria() throws Exception {
    OptimizeTests.testOnUnpartitionedMinInputFilesCriteria(SOURCE, allocator);
  }

  @Test
  public void testOnPartitionedMinInputFilesCriteria() throws Exception {
    OptimizeTests.testOnPartitionedMinInputFilesCriteria(SOURCE, allocator);
  }

  @Test
  public void testOnUnpartitionedMinFileSizeCriteria() throws Exception {
    OptimizeTests.testOnUnpartitionedMinFileSizeCriteria(SOURCE, allocator);
  }

  @Test
  public void testWithTargetFileSizeAlreadyOptimal() throws Exception {
    OptimizeTests.testWithTargetFileSizeAlreadyOptimal(SOURCE, allocator);
  }

  @Test
  public void testWithMixedSizes() throws Exception {
    OptimizeTests.testWithMixedSizes(SOURCE, allocator);
  }

  @Test
  public void testWithSingleFilePartitions() throws Exception {
    OptimizeTests.testWithSingleFilePartitions(SOURCE, allocator);
  }

  @Test
  public void testWithSingleFilePartitionsAndEvolvedSpec() throws Exception {
    OptimizeTests.testWithSingleFilePartitionsAndEvolvedSpec(SOURCE, allocator);
  }

  @Test
  public void testUnsupportedScenarios() throws Exception {
    OptimizeTests.testUnsupportedScenarios(SOURCE, allocator);
  }

  @Test
  public void testEvolvedPartitions() throws Exception {
    OptimizeTests.testEvolvedPartitions(SOURCE, allocator);
  }

  @Test
  public void testOptimizeDataOnlyUnPartitioned() throws Exception {
    OptimizeTests.testOptimizeDataFilesUnPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOptimizeDataOnlyPartitioned() throws Exception {
    OptimizeTests.testOptimizeDataOnPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOptimizeManifestsOnlyUnPartitioned() throws Exception {
    OptimizeTests.testOptimizeManifestsOnlyUnPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOptimizeManifestsOnlyPartitioned() throws Exception {
    OptimizeTests.testOptimizeManifestsOnlyPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOptimizeLargeManifests() throws Exception {
    OptimizeTests.testOptimizeLargeManifests(SOURCE, allocator);
  }

  @Test
  public void testOptimizeManifestsModesIsolations() throws Exception {
    OptimizeTests.testOptimizeManifestsModesIsolations(SOURCE, allocator);
  }

  @Test
  public void testOptimizeManifestsWithOptimalSize() throws Exception {
    OptimizeTests.testOptimizeManifestsWithOptimalSize(SOURCE, allocator);
  }

  @Test
  public void testOptimizeOnEmptyTableNoSnapshots() throws Exception {
    OptimizeTests.testOptimizeOnEmptyTableNoSnapshots(SOURCE, allocator);
  }

  @Test
  public void testOptimizeOnEmptyTableHollowSnapshot() throws Exception {
    OptimizeTests.testOptimizeOnEmptyTableHollowSnapshot(SOURCE, allocator);
  }

  @Test
  public void testOptimizeNoopOnResidualDataManifests() throws Exception {
    OptimizeTests.testOptimizeNoopOnResidualDataManifests(SOURCE, allocator);
  }

  @Test
  public void testRewriteManifestsForEvolvedPartitionSpec() throws Exception {
    OptimizeTests.testRewriteManifestsForEvolvedPartitionSpec(SOURCE, allocator);
  }
}
