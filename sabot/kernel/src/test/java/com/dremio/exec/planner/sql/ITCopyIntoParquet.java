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

public class ITCopyIntoParquet extends ITDmlQueryBase {

  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testParquetMixedSchemas() throws Exception {
    CopyIntoParquetTests.testParquetMixedSchemas(allocator, SOURCE);
  }

  @Test
  public void testParquetSingleFile() throws Exception {
    CopyIntoParquetTests.testParquetSingleFile(allocator, SOURCE);
  }

  @Test
  public void testParquetBinaryToStringColumn() throws Exception {
    CopyIntoParquetTests.testParquetBinaryToStringColumn(allocator, SOURCE);
  }

  @Test
  public void testParquetMissingColumns() throws Exception {
    CopyIntoParquetTests.testParquetMissingColumns(allocator, SOURCE);
  }

  @Test
  public void testParquetExtraColumns() throws Exception {
    CopyIntoParquetTests.testParquetExtraColumns(allocator, SOURCE);
  }

  @Test
  public void testParquetColumnCaseInsensitive() throws Exception {
    CopyIntoParquetTests.testParquetColumnCaseInsensitive(allocator, SOURCE);
  }

  @Test
  public void testParquetNoColumnMatch() throws Exception {
    CopyIntoParquetTests.testParquetNoColumnMatch(allocator, SOURCE);
  }

  @Test
  public void testPaquetWithOnErrorContinue() throws Exception {
    CopyIntoParquetTests.testParquetWithOnErrorContinue(SOURCE);
  }
}
