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

import com.dremio.exec.planner.sql.OptimizeTests;
public class ITOptimizeOnHive extends DmlQueryOnHiveTestBase {
  private static final String SOURCE = HIVE_TEST_PLUGIN_NAME;
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
  public void testOptimizeManifestsModesIsolations() throws Exception {
    OptimizeTests.testOptimizeManifestsModesIsolations(SOURCE, allocator);
  }

  @Test
  public void testOptimizeOnEmptyTableHollowSnapshot() throws Exception {
    OptimizeTests.testOptimizeOnEmptyTableHollowSnapshot(SOURCE, allocator);
  }

  @Test
  public void testRewriteManifestsForEvolvedPartitionSpec() throws Exception {
    OptimizeTests.testRewriteManifestsForEvolvedPartitionSpec(SOURCE, allocator);
  }
}
