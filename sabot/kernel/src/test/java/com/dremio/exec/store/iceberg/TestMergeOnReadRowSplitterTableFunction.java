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
package com.dremio.exec.store.iceberg;

import static com.dremio.sabot.Fixtures.NULL_BIGINT;
import static com.dremio.sabot.Fixtures.NULL_INT;
import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static java.util.Map.entry;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.MergeOnReadRowSplitterTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

/**
 * tests the row splitter table function. Ensures the proper output given a specified input. The
 * main test is to ensure the row-split is properly handled when the outgoing batch is too full to
 * accept both rows. This case is handled with a buffer. We need to verify the data buffered is
 * properly passed to the next 'outgoing' batch, while also handling the indexing adjustments for
 * each row.
 */
public class TestMergeOnReadRowSplitterTableFunction extends BaseTestTableFunction {

  private static final String YEAR_T = "order_year_TARGET";

  private static final String ID = "order_id";
  private static final String YEAR = "order_year";

  private static final String ID_U = "order_id_UPDATE";
  private static final String YEAR_U = "order_year_UPDATE";

  private static final String FILE_PATH = ColumnUtils.FILE_PATH_COLUMN_NAME;
  private static final String ROW_INDEX = ColumnUtils.ROW_INDEX_COLUMN_NAME;

  private static final Map<String, Integer> UPDATE_COLUMNS_WITH_INDEX =
      Map.ofEntries(entry(ID, 0), entry(YEAR, 1));
  private static final Set<String> OUTDATED_TARGET_TABLE_COLUMNS = new HashSet<>(List.of(ID));
  private static final List<String> PARTITION_COLUMNS = new LinkedList<>(List.of(YEAR));

  public static BatchSchema V2_ORDERS =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable(ID, new ArrowType.Int(32, true)),
              Field.nullable(YEAR, new ArrowType.Int(32, true)),
              Field.nullable(FILE_PATH, new ArrowType.Utf8()),
              Field.nullable(ROW_INDEX, new ArrowType.Int(64, true))));

  // simple verification test on an INSERT row
  @Test
  public void testInsertRow() throws Exception {
    Table input =
        t(
            th(YEAR_T, FILE_PATH, ROW_INDEX, ID, YEAR, ID_U, YEAR_U),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 0, 2024, NULL_INT, NULL_INT));

    Table output = t(th(ID, YEAR, FILE_PATH, ROW_INDEX), tr(0, 2024, NULL_VARCHAR, NULL_BIGINT));

    validateSingle(getPop(2), TableFunctionOperator.class, input, output, 5);
  }

  // simple verification test on an UPDATE row
  @Test
  public void testRowSplit() throws Exception {
    Table input =
        t(
            th(YEAR_T, FILE_PATH, ROW_INDEX, ID, YEAR, ID_U, YEAR_U),
            tr(2020, "file_path_old", 0L, NULL_INT, NULL_INT, 0, 2024));

    Table output =
        t(
            th(ID, YEAR, FILE_PATH, ROW_INDEX),
            tr(0, 2024, NULL_VARCHAR, NULL_BIGINT),
            tr(NULL_INT, 2020, "file_path_old", 0L));

    validateSingle(getPop(0), TableFunctionOperator.class, input, output, 5);
  }

  // test multiple insert rows on a limited batch size
  @Test
  public void testInsertRowMaxLimitReached() throws Exception {
    Table input =
        t(
            th(YEAR_T, FILE_PATH, ROW_INDEX, ID, YEAR, ID_U, YEAR_U),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 0, 2020, NULL_INT, NULL_INT),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 1, 2021, NULL_INT, NULL_INT),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 2, 2022, NULL_INT, NULL_INT),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 3, 2023, NULL_INT, NULL_INT),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 4, 2024, NULL_INT, NULL_INT));

    Table output =
        t(
            th(ID, YEAR, FILE_PATH, ROW_INDEX),
            tr(0, 2020, NULL_VARCHAR, NULL_BIGINT),
            tr(1, 2021, NULL_VARCHAR, NULL_BIGINT),
            tr(2, 2022, NULL_VARCHAR, NULL_BIGINT),
            tr(3, 2023, NULL_VARCHAR, NULL_BIGINT),
            tr(4, 2024, NULL_VARCHAR, NULL_BIGINT));

    validateSingle(getPop(2), TableFunctionOperator.class, input, output, 2);
  }

  // MAIN TEST #1 of the class: test max record limit on a split row where the cap occurs at
  // the very end of the table. This triggers the noMoreToConsume() case.
  @Test
  public void testRowSplit_MergeWithUpdateInsert_outputExceedsTargetBatchSize_OnNoMoreToConsume()
      throws Exception {
    Table input =
        t(
            th(YEAR_T, FILE_PATH, ROW_INDEX, ID, YEAR, ID_U, YEAR_U),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 10, 2030, NULL_INT, NULL_INT),
            tr(2020, "file_path_old", 0L, NULL_INT, NULL_INT, 0, 2024));

    Table output =
        t(
            th(ID, YEAR, FILE_PATH, ROW_INDEX),
            tr(10, 2030, NULL_VARCHAR, NULL_BIGINT),
            tr(0, 2024, NULL_VARCHAR, NULL_BIGINT),
            tr(NULL_INT, 2020, "file_path_old", 0L));

    validateSingle(getPop(2), TableFunctionOperator.class, input, output, 2);
  }

  // MAIN TEST #2: test max record limit on a split row  where
  // the batch is filled up multiple times before reaching the end of the table,
  // including the end of the table, which also triggers the noMoreToConsume() case.
  @Test
  public void testRowSplit_MergeWithUpdateInsert_outputExceedsTargetBatchSize_MultipleTimes()
      throws Exception {
    Table input =
        t(
            th(YEAR_T, FILE_PATH, ROW_INDEX, ID, YEAR, ID_U, YEAR_U),
            tr(NULL_INT, NULL_VARCHAR, NULL_BIGINT, 10, 2040, NULL_INT, NULL_INT),
            tr(2020, "file_path_old_1", 0L, NULL_INT, NULL_INT, 0, 2030),
            tr(2021, "file_path_old_2", 1L, NULL_INT, NULL_INT, 1, 2031),
            tr(2022, "file_path_old_3", 2L, NULL_INT, NULL_INT, 2, 2032),
            tr(2023, "file_path_old_4", 3L, NULL_INT, NULL_INT, 3, 2033));

    Table output =
        t(
            th(ID, YEAR, FILE_PATH, ROW_INDEX),
            tr(10, 2040, NULL_VARCHAR, NULL_BIGINT),
            tr(0, 2030, NULL_VARCHAR, NULL_BIGINT),
            tr(NULL_INT, 2020, "file_path_old_1", 0L),
            tr(1, 2031, NULL_VARCHAR, NULL_BIGINT),
            tr(NULL_INT, 2021, "file_path_old_2", 1L),
            tr(2, 2032, NULL_VARCHAR, NULL_BIGINT),
            tr(NULL_INT, 2022, "file_path_old_3", 2L),
            tr(3, 2033, NULL_VARCHAR, NULL_BIGINT),
            tr(NULL_INT, 2023, "file_path_old_4", 3L));

    validateSingle(getPop(2), TableFunctionOperator.class, input, output, 2);
  }

  /**
   * Gets the Physical Operator with the specified pre-requisites to conduct the Row-splitter
   * table-function's lifecycle.
   */
  private TableFunctionPOP getPop(int insertColumnCount) {
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.MERGE_ON_READ_ROW_SPLITTER,
            true,
            new MergeOnReadRowSplitterTableFunctionContext(
                V2_ORDERS,
                V2_ORDERS,
                V2_ORDERS.getFields().stream()
                    .map(f -> SchemaPath.getSimplePath(f.getName()))
                    .collect(Collectors.toList()),
                insertColumnCount,
                UPDATE_COLUMNS_WITH_INDEX,
                OUTDATED_TARGET_TABLE_COLUMNS,
                PARTITION_COLUMNS)));
  }
}
