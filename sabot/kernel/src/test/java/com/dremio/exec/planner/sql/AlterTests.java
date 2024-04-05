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

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testQueryValidateStatusSummary;

import org.apache.arrow.memory.BufferAllocator;

/** Alter Iceberg table tests */
public class AlterTests extends ITDmlQueryBase {
  public static void testAddColumnsNameWithDot(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 1, 1)) {

      testQueryValidateStatusSummary(
          allocator,
          "ALTER TABLE %s ADD COLUMNS (\"column.with.dot\" int)",
          new Object[] {table.fqn},
          table,
          true,
          "New columns added.",
          null);
    }
  }

  public static void testChangeColumnNameWithDot(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 1, 1)) {

      testQueryValidateStatusSummary(
          allocator,
          "ALTER TABLE %s CHANGE COLUMN \"%s\" \"column.with.dot\" INT",
          new Object[] {table.fqn, table.columns[0]},
          table,
          true,
          String.format("Column [%s] modified", table.columns[0]),
          null);
    }
  }
}
