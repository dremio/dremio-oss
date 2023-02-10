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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionValue;

/**
 * Test OPTIMIZE TABLE
 */
public class ITOptimizeTable extends BaseTestQuery {
  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @BeforeClass
  public static void setUp() throws Exception {
    getSabotContext().getOptionManager().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM, ExecConstants.ENABLE_ICEBERG_OPTIMIZE.getOptionName(), true));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    getSabotContext().getOptionManager().setOption(ExecConstants.ENABLE_ICEBERG_OPTIMIZE.getDefault());
  }

  @Test
  public void testOnUnPartitioned() throws Exception {
    OptimizeTests.testOnUnPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOnPartitioned() throws Exception {
    OptimizeTests.testOnPartitioned(SOURCE, allocator);
  }

  @Test
  public void testOnUnpartitionedMinInputFilesCriteria() throws Exception {
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
}
