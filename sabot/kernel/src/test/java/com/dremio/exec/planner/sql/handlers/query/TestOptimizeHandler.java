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
package com.dremio.exec.planner.sql.handlers.query;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;

public class TestOptimizeHandler extends BaseTestQuery {

  private static SqlHandlerConfig mockConfig;
  private static OptionManager mockOptionManager;
  private static Catalog mockCatalog;
  private final Class<? extends Exception> exceptionType = IllegalArgumentException.class;

  @BeforeAll
  public static void setup() {
    mockConfig = Mockito.mock(SqlHandlerConfig.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);
    mockCatalog = Mockito.mock(Catalog.class);
    mockOptionManager = Mockito.mock(OptionManager.class);

    when(mockConfig.getContext()).thenReturn(mockQueryContext);
    when(mockQueryContext.getOptions()).thenReturn(mockOptionManager);
    when(mockQueryContext.getCatalog()).thenReturn(mockCatalog);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB)).thenReturn(256L);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO)).thenReturn(0.75);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO)).thenReturn(1.8);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES)).thenReturn(5L);
    when(mockOptionManager.getOption(ExecConstants.ENABLE_ICEBERG_OPTIMIZE)).thenReturn(true);
    when(mockOptionManager.getOption(ExecConstants.ENABLE_ICEBERG)).thenReturn(true);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "OPTIMIZE TABLE a.b.c (target_file_size_mb=5)",
    "OPTIMIZE TABLE a.b.c (target_file_size_mb=5, min_file_size_mb=1)",
    "OPTIMIZE TABLE a.b.c (target_file_size_mb=5, max_file_size_mb=6)",
    "OPTIMIZE TABLE a.b.c (min_file_size_mb=1, target_file_size_mb=5, max_file_size_mb=6)",
    "OPTIMIZE TABLE a.b.c (min_file_size_mb=200, max_file_size_mb=300)",
    "OPTIMIZE TABLE a.b.c (min_file_size_mb=0)"
  })

  void testValidOptions(String query) {
      assertDoesNotThrow(() -> getValidOptimizeOptions(query));
  }

  @ParameterizedTest
  @MethodSource("invalidOptionQueries")
  void testInvalidOptions(Pair<String, String> test) {
    assertThatThrownBy(() -> getValidOptimizeOptions(test.getKey())).isInstanceOf(exceptionType).hasMessage(test.getValue());
  }

  @Test
  void testOptimizeDisabled() throws Exception {
    SqlOptimize sqlOptimize = parseToSqlOptimizeNode("OPTIMIZE TABLE a.b.c");
    OptimizeHandler optimizeHandler = (OptimizeHandler) sqlOptimize.toPlanHandler();
    when(mockConfig.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_OPTIMIZE)).thenReturn(false);
    when(mockCatalog.resolveToDefault(any())).thenReturn(null);
    when(mockCatalog.getTableNoResolve(sqlOptimize.getPath())).thenReturn(Mockito.mock(DremioTable.class));
    assertThatThrownBy(() -> optimizeHandler.getPlan(mockConfig, "OPTIMIZE TABLE a.b.c", sqlOptimize))
      .isInstanceOf(UserException.class)
      .hasMessage("OPTIMIZE TABLE command is not supported.");
  }

  @Test
  void testNonexistentTable() throws Exception {
    SqlOptimize sqlOptimize = parseToSqlOptimizeNode("OPTIMIZE TABLE a.b.c");
    OptimizeHandler optimizeHandler = (OptimizeHandler) sqlOptimize.toPlanHandler();
    when(mockCatalog.getTableNoResolve(any())).thenReturn(null);
    assertThatThrownBy(() -> optimizeHandler.getPlan(mockConfig, "OPTIMIZE TABLE a.b.c", sqlOptimize))
      .isInstanceOf(UserException.class)
      .hasMessage("Table [a.b.c] does not exist.");
  }

  @Test
  void testV2Table() throws Exception {
    IcebergTestTables.Table table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
    String query = String.format("OPTIMIZE TABLE %s", table.getTableName());
    SqlOptimize sqlOptimize = parseToSqlOptimizeNode(query);
    NamespaceKey path = sqlOptimize.getPath();
    OptimizeHandler optimizeHandler = (OptimizeHandler) sqlOptimize.toPlanHandler();

    try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
      mockedStatic.when(() -> IcebergUtils.checkTableExistenceAndMutability(eq(mockCatalog), eq(mockConfig), eq(path), any(), eq(false))).thenReturn(null);
      DremioTable mockTable = Mockito.mock(DremioTable.class, Answers.RETURNS_DEEP_STUBS);
      when(mockCatalog.getTableNoResolve(path)).thenReturn(mockTable);
      when(mockTable.getPath()).thenReturn(path);
      when(mockTable.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getDeleteManifestStats().getRecordCount()).thenReturn(1L);

      assertThatThrownBy(() -> optimizeHandler.getPlan(mockConfig, query, sqlOptimize))
        .isInstanceOf(UserException.class)
        .hasMessage("OPTIMIZE TABLE command does not support tables with delete files.");
    }
  }

  static List<Pair<String, String>> invalidOptionQueries() {
    return Arrays.asList(
      Pair.of("OPTIMIZE TABLE a.b.c (target_file_size_mb=2, min_file_size_mb=3)", "Value of TARGET_FILE_SIZE_MB [2] cannot be less than MIN_FILE_SIZE_MB [3]."),
      Pair.of("OPTIMIZE TABLE a.b.c (max_file_size_mb=270, min_file_size_mb=269)", "Value of TARGET_FILE_SIZE_MB [256] cannot be less than MIN_FILE_SIZE_MB [269]."),
      Pair.of("OPTIMIZE TABLE a.b.c (target_file_size_mb=2, max_file_size_mb=1)", "Value of TARGET_FILE_SIZE_MB [2] cannot be greater than MAX_FILE_SIZE_MB [1]."),
      Pair.of("OPTIMIZE TABLE a.b.c (min_file_size_mb=2, max_file_size_mb=26)", "Value of TARGET_FILE_SIZE_MB [256] cannot be greater than MAX_FILE_SIZE_MB [26]."),
      Pair.of("OPTIMIZE TABLE a.b.c (max_file_size_mb=2, min_file_size_mb=5)", "Value of MIN_FILE_SIZE_MB [5] cannot be greater than MAX_FILE_SIZE_MB [2]."),
      Pair.of("OPTIMIZE TABLE a.b.c (target_file_size_mb=2, min_file_size_mb=5)", "Value of MIN_FILE_SIZE_MB [5] cannot be greater than MAX_FILE_SIZE_MB [3]."),
      Pair.of("OPTIMIZE TABLE a.b.c (max_file_size_mb=0)", "MAX_FILE_SIZE_MB [0] should be a positive integer value."),
      Pair.of("OPTIMIZE TABLE a.b.c (min_input_files=0)", "Value of MIN_INPUT_FILES [0] cannot be less than 1."),
      Pair.of("OPTIMIZE TABLE a.b.c (min_input_files=-2)", "Value of MIN_INPUT_FILES [-2] cannot be less than 1."),
      Pair.of("OPTIMIZE TABLE a.b.c (max_file_size_mb=-1200)", "MAX_FILE_SIZE_MB [-1200] should be a positive integer value."),
      Pair.of("OPTIMIZE TABLE a.b.c (min_file_size_mb=-1050)", "MIN_FILE_SIZE_MB [-1050] should be a non-negative integer value."),
      Pair.of("OPTIMIZE TABLE a.b.c (target_file_size_mb=-256)", "TARGET_FILE_SIZE_MB [-256] should be a positive integer value.")
    );
  }

  private static OptimizeOptions getValidOptimizeOptions(String toParse) throws Exception {
    SqlOptimize sqlOptimize = parseToSqlOptimizeNode(toParse);
    //return Optimize Options if all the inputs are valid else throw error.
    return new OptimizeOptions(mockConfig.getContext().getOptions(), sqlOptimize, true);
  }

  private static SqlOptimize parseToSqlOptimizeNode(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(toParse, config);
    return (SqlOptimize) parser.parseStmt();
  }
}
