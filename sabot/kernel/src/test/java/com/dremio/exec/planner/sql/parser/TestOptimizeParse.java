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

import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collection;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;

/**
 * Tests for optimize command
 */
@RunWith(Parameterized.class)
public class TestOptimizeParse {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
      {"OPTIMIZE TABLE a.b.c", true},
      {"OPTIMIZE TABLE a.b.c USING BIN_PACK", true},
      {"OPTIMIZE TABLE a.b.c (target_file_size_mb=2)", true},
      {"OPTIMIZE TABLE a.b.c USING BIN_PACK (target_file_size_mb=2)", true},
      {"OPTIMIZE TABLE a.b.c REWRITE DATA USING BIN_PACK", true},
      {"OPTIMIZE a.b.c", false}, // No table keyword
      {"OPTIMIZE TABLE WHERE id=5", false}, // No table name
      {"OPTIMIZE TABLE a.b.c USING SORT", false}, // SORT not supported
      {"OPTIMIZE TABLE a.b.c WHERE id=5", false}, // WHERE conditions not supported
      {"OPTIMIZE TABLE a.b.c (unknown_file_size=2)", false}, // Invalid option
      {"OPTIMIZE TABLE a.b.c (target_file_size_bytes=2)", false}, // Old options should not work
      {"OPTIMIZE TABLE a.b.c (target_file_size_mb=0.2)", false}, // Options must be numeric
      {"OPTIMIZE TABLE a.b.c (target_file_size_mb=COUNT(col_name))", false}, // Options must be literal
      {"OPTIMIZE TABLE a.b.c (target_file_size_mb=2) WHERE id=5", false}, // Where clause must be before options
      {"OPTIMIZE TABLE a.b.c REWRITE MANIFESTS", false}, // REWRITE MANIFESTS not supported
      {"OPTIMIZE TABLE a.b.c REWRITE MANIFESTS WHERE id=5", false}, // Where clause not allowed when rewriting manifests
      {"OPTIMIZE TABLE a.b.c REWRITE MANIFESTS (target_file_size_mb=2)", false}, // Options not allowed when rewriting manifests
      {"OPTIMIZE TABLE a.b.c REWRITE DATA target_file_size_mb=2", false}, // Options must be enclosed in parentheses
      {"OPTIMIZE TABLE a.b.c REWRITE MANIFESTS WHERE id=5 (target_file_size_mb=2)", false}, // Where clause and options not allowed when rewriting manifests
      {"OPTIMIZE TABLE a.b.c REWRITE DATA USING SORT (target_file_size_mb=2, min_input_files=5 WHERE id=5)", false} // Where clause must be before options
    });
  }

  private String query;
  private boolean shouldSucceed;

  public TestOptimizeParse(String query, boolean shouldSucceed) {
    this.query = query;
    this.shouldSucceed = shouldSucceed;
  }

  @Test
  public void testOptimizeTableParseVariants() throws SqlParseException {
    if (!shouldSucceed) {
      assertThrows(SqlParseException.class, () -> parse(query));
    } else {
      parse(query);
    }
  }

  private SqlNode parse(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }
}
