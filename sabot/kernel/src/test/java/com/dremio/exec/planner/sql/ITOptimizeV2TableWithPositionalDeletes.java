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

/**
 * Test OPTIMIZE TABLE scenarios for Iceberg V2 tables with row-level-delete files
 */
public class ITOptimizeV2TableWithPositionalDeletes extends BaseTestQuery {

  @BeforeClass
  public static void setup() throws Exception {
    OptimizeTestWithDeletes.setup();

    // Vectorized parquet read is not available in OSS
    setSystemOption(ExecConstants.PARQUET_READER_VECTORIZE, "false");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    setSystemOption(ExecConstants.PARQUET_READER_VECTORIZE,
      ExecConstants.PARQUET_READER_VECTORIZE.getDefault().getBoolVal().toString());
  }

  @Test
  public void testV2OptimizePartitioned() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizePartitioned(allocator);
  }

  @Test
  public void testV2OptimizeUnpartitioned() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizeUnpartitioned(allocator);
  }

  @Test
  public void testV2OptimizeMinInputFiles() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizeMinInputFiles(allocator);
  }

  @Test
  public void testV2OptimizeDeleteLinkedFilesOnlyUnpartitioned() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizeDeleteLinkedFilesOnlyUnpartitioned(allocator);
  }

  @Test
  public void testV2OptimizeDeleteLinkedFilesOnlyPartitioned() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizeDeleteLinkedFilesOnlyPartitioned(allocator);
  }

  @Test
  public void testV2OptimizeMultipleDeleteFiles() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizeMultipleDeleteFiles(allocator);
  }

  @Test
  public void testV2OptimizePartitionEvolution() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizePartitionEvolution(allocator);
  }

  @Test
  public void testV2OptimizeUpdateSequenceNumber() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizeUpdateSequenceNumber(allocator);
  }

  @Test
  public void testV2OptimizeUpdateSequenceNumberWithDeleteLink() throws Exception {
    OptimizeTestWithDeletes.testV2OptimizeUpdateSequenceNumberWithDeleteLink(allocator);
  }
}
