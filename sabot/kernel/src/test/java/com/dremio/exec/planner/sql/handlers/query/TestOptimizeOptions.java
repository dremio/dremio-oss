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

import static com.dremio.exec.planner.sql.parser.TestParserUtil.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.options.OptionManager;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/** Tests for {@link OptimizeOptions} */
public class TestOptimizeOptions {
  private static OptionManager mockOptionManager;
  private final Class<? extends Exception> exceptionType = IllegalArgumentException.class;

  @Test
  public void testCreateInstanceFromNodeAllOptions() throws SqlParseException {
    SqlOptimize sqlNode =
        (SqlOptimize)
            parse(
                "OPTIMIZE TABLE a.b.c REWRITE DATA (TARGET_FILE_SIZE_MB=257, MIN_INPUT_FILES=10, MAX_FILE_SIZE_MB=300, MIN_FILE_SIZE_MB=100)");

    OptimizeOptions optimizeOptions = OptimizeOptions.createInstance(sqlNode);

    assertThat(optimizeOptions.isOptimizeManifestFiles()).isFalse();
    assertThat(optimizeOptions.isOptimizeDataFiles()).isTrue();
    assertThat(optimizeOptions.isOptimizeManifestsOnly()).isFalse();
    assertThat(optimizeOptions.isSingleDataWriter()).isFalse();

    assertThat(optimizeOptions.getTargetFileSizeBytes()).isEqualTo(257 * 1024 * 1024);
    assertThat(optimizeOptions.getMinInputFiles()).isEqualTo(10);
    assertThat(optimizeOptions.getMinFileSizeBytes()).isEqualTo(100 * 1024 * 1024);
    assertThat(optimizeOptions.getMaxFileSizeBytes()).isEqualTo(300 * 1024 * 1024);
  }

  @Test
  public void testCreateInstanceFromNodeAllDefaults() throws SqlParseException {
    SqlOptimize sqlNode = (SqlOptimize) parse("OPTIMIZE TABLE a.b.c");

    OptimizeOptions optimizeOptions = OptimizeOptions.createInstance(sqlNode);

    assertThat(optimizeOptions.isOptimizeManifestFiles()).isTrue();
    assertThat(optimizeOptions.isOptimizeDataFiles()).isTrue();
    assertThat(optimizeOptions.isOptimizeManifestsOnly()).isFalse();
    assertThat(optimizeOptions.isSingleDataWriter()).isFalse();

    assertThat(optimizeOptions.getTargetFileSizeBytes())
        .isEqualTo(
            ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB.getDefault().getNumVal() * 1024 * 1024);
    assertThat(optimizeOptions.getMinInputFiles())
        .isEqualTo(ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES.getDefault().getNumVal());

    long expectedMin =
        ((long)
                (ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB.getDefault().getNumVal()
                    * ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO
                        .getDefault()
                        .getFloatVal()))
            * 1024
            * 1024;
    assertThat(optimizeOptions.getMinFileSizeBytes()).isEqualTo(expectedMin);

    long expectedMax =
        ((long)
                (ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB.getDefault().getNumVal()
                    * ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO
                        .getDefault()
                        .getFloatVal()))
            * 1024
            * 1024;
    assertThat(optimizeOptions.getMaxFileSizeBytes()).isEqualTo(expectedMax);
  }

  @Test
  public void testCreateInstanceUsingSupportOptions() throws SqlParseException {
    SqlOptimize sqlNode = (SqlOptimize) parse("OPTIMIZE TABLE a.b.c");

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB)).thenReturn(1000L);
    when(optionManager.getOption(ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO))
        .thenReturn(2D);
    when(optionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO))
        .thenReturn(0.2D);
    when(optionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES)).thenReturn(10L);

    OptimizeOptions optimizeOptions = OptimizeOptions.createInstance(optionManager, sqlNode, true);

    assertThat(optimizeOptions.getMinInputFiles()).isEqualTo(10L);
    assertThat(optimizeOptions.getMinFileSizeBytes()).isEqualTo(200 * 1024 * 1024);
    assertThat(optimizeOptions.getMaxFileSizeBytes()).isEqualTo(2000 * 1024 * 1024);
    assertThat(optimizeOptions.getTargetFileSizeBytes()).isEqualTo(1000 * 1024 * 1024);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
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
    assertThatThrownBy(() -> getValidOptimizeOptions(test.getKey()))
        .isInstanceOf(exceptionType)
        .hasMessage(test.getValue());
  }

  static List<Pair<String, String>> invalidOptionQueries() {
    return Arrays.asList(
        Pair.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_mb=2, min_file_size_mb=3)",
            "Value of TARGET_FILE_SIZE_MB [2] cannot be less than MIN_FILE_SIZE_MB [3]."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (max_file_size_mb=270, min_file_size_mb=269)",
            "Value of TARGET_FILE_SIZE_MB [256] cannot be less than MIN_FILE_SIZE_MB [269]."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_mb=2, max_file_size_mb=1)",
            "Value of TARGET_FILE_SIZE_MB [2] cannot be greater than MAX_FILE_SIZE_MB [1]."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (min_file_size_mb=2, max_file_size_mb=26)",
            "Value of TARGET_FILE_SIZE_MB [256] cannot be greater than MAX_FILE_SIZE_MB [26]."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (max_file_size_mb=2, min_file_size_mb=5)",
            "Value of MIN_FILE_SIZE_MB [5] cannot be greater than MAX_FILE_SIZE_MB [2]."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_mb=2, min_file_size_mb=5)",
            "Value of MIN_FILE_SIZE_MB [5] cannot be greater than MAX_FILE_SIZE_MB [3]."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (max_file_size_mb=0)",
            "MAX_FILE_SIZE_MB [0] should be a positive integer value."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (min_input_files=0)",
            "Value of MIN_INPUT_FILES [0] cannot be less than 1."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (min_input_files=-2)",
            "Value of MIN_INPUT_FILES [-2] cannot be less than 1."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (max_file_size_mb=-1200)",
            "MAX_FILE_SIZE_MB [-1200] should be a positive integer value."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (min_file_size_mb=-1050)",
            "MIN_FILE_SIZE_MB [-1050] should be a non-negative integer value."),
        Pair.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_mb=-256)",
            "TARGET_FILE_SIZE_MB [-256] should be a positive integer value."));
  }

  private static OptimizeOptions getValidOptimizeOptions(String toParse) throws Exception {
    SqlOptimize sqlOptimize = parseToSqlOptimizeNode(toParse);
    // return Optimize Options if all the inputs are valid else throw error.
    mockOptionManager = Mockito.mock(OptionManager.class);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB)).thenReturn(256L);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO))
        .thenReturn(0.75);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO))
        .thenReturn(1.8);
    when(mockOptionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES)).thenReturn(5L);

    return OptimizeOptions.createInstance(mockOptionManager, sqlOptimize, true);
  }

  private static SqlOptimize parseToSqlOptimizeNode(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);
    SqlParser parser = SqlParser.create(toParse, config);
    return (SqlOptimize) parser.parseStmt();
  }
}
