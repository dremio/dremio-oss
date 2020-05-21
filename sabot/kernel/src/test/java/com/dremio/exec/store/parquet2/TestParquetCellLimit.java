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
package com.dremio.exec.store.parquet2;

import static com.dremio.exec.ExecConstants.LIMIT_FIELD_SIZE_BYTES;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;

/**
 * Tests the reading of parquet cell with a given limiting size.
 */
public class TestParquetCellLimit extends BaseTestQuery {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testCellSizeForVarcharFieldExceedingLimit() throws Exception {
    try(AutoCloseable c = withSystemOption(LIMIT_FIELD_SIZE_BYTES, 10L)) {
      final String query = "select l_shipmode, l_comment from cp.\"parquet/cell_limit.parquet\"";
      exception.expect(UserException.class);
      exception.expectMessage("UNSUPPORTED_OPERATION ERROR: Field exceeds the size limit");
      test(query);
    }
  }

  @Test
  public void testCellSizeForNonVarcharFieldExceedingLimit() throws Exception {
    try(AutoCloseable c = withSystemOption(LIMIT_FIELD_SIZE_BYTES, 10L)) {
      final String query = "select l_orderkey from cp.\"parquet/cell_limit.parquet\"";
      test(query);
    }
  }

  @Test
  public void testCellSizeForVarcharFieldWithinLimit() throws Exception {
    try(AutoCloseable c = withSystemOption(LIMIT_FIELD_SIZE_BYTES, 50L)) {
      final String query = "select l_comment from cp.\"parquet/cell_limit.parquet\"";
      test(query);
    }
  }

  @Test
  public void testCellSizeForNonVarcharFieldWithinLimit() throws Exception {
    try(AutoCloseable c = withSystemOption(LIMIT_FIELD_SIZE_BYTES, 50L)) {
      final String query = "select l_orderkey from cp.\"parquet/cell_limit.parquet\"";
      test(query);
    }
  }
}

