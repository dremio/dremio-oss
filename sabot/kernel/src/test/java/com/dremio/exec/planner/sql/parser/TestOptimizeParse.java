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
package com.dremio.exec.planner.sql.parser;

import static com.dremio.exec.planner.sql.parser.TestParserUtil.parse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for optimize command */
public class TestOptimizeParse {

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of("OPTIMIZE TABLE a.b.c", true),
        Arguments.of("OPTIMIZE TABLE a.b.c USING BIN_PACK", true),
        Arguments.of("OPTIMIZE TABLE a.b.c (target_file_size_mb=2)", true),
        Arguments.of("OPTIMIZE TABLE a.b.c USING BIN_PACK (target_file_size_mb=2)", true),
        Arguments.of("OPTIMIZE TABLE a.b.c REWRITE DATA USING BIN_PACK", true),
        Arguments.of("OPTIMIZE a.b.c", false), // No table keyword
        Arguments.of("OPTIMIZE TABLE WHERE id=5", false), // No table name
        Arguments.of("OPTIMIZE TABLE a.b.c USING SORT", false), // SORT not supported
        Arguments.of("OPTIMIZE TABLE a.b.c WHERE id=5", false),
        Arguments.of("OPTIMIZE TABLE a.b.c FOR PARTITIONS (id=5)", true),
        Arguments.of("OPTIMIZE TABLE a.b.c (unknown_file_size=2)", false), // Invalid option
        Arguments.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_bytes=2)",
            false), // Old options should not work
        Arguments.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_mb=0.2)", false), // Options must be numeric
        Arguments.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_mb=COUNT(col_name))",
            false), // Options must be literal
        Arguments.of(
            "OPTIMIZE TABLE a.b.c (target_file_size_mb=2) WHERE id=5",
            false), // Where clause must be before options
        Arguments.of(
            "OPTIMIZE TABLE a.b.c REWRITE MANIFESTS WHERE id=5",
            false), // Where clause not allowed when rewriting manifests
        Arguments.of(
            "OPTIMIZE TABLE a.b.c REWRITE MANIFESTS (target_file_size_mb=2)",
            false), // Options not allowed when rewriting manifests
        Arguments.of(
            "OPTIMIZE TABLE a.b.c REWRITE DATA target_file_size_mb=2",
            false), // Options must be enclosed in parentheses
        Arguments.of(
            "OPTIMIZE TABLE a.b.c REWRITE MANIFESTS WHERE id=5 (target_file_size_mb=2)",
            false), // Where clause and options not allowed when rewriting manifests
        Arguments.of(
            "OPTIMIZE TABLE a.b.c REWRITE DATA USING SORT (target_file_size_mb=2, min_input_files=5 WHERE id=5)",
            false) // Where clause must be before options
        );
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testOptimizeTableParseVariants(String query, boolean shouldSucceed)
      throws SqlParseException {
    if (!shouldSucceed) {
      assertThrows(SqlParseException.class, () -> parse(query));
    } else {
      parse(query);
    }
  }
}
