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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;

public class TestSqlOptimize {
  private static SqlDialect DREMIO_DIALECT =
    new SqlDialect(SqlDialect.DatabaseProduct.UNKNOWN, "Dremio", Character.toString(SqlUtils.QUOTE), NullCollation.FIRST);
  private SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);

  @Test
  public void testBasic() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse("RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals("Compaction type does not match,", CompactionType.BIN_PACK, optimizeParsed.getCompactionType());
  }

  @Test
  public void testBasicWithBinPack() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c USING BIN_PACK");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse("RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals("Compaction type does not match,", CompactionType.BIN_PACK, optimizeParsed.getCompactionType());
  }

  @Test
  public void testBasicWithOptions() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c (target_file_size_mb=2)");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK (\"target_file_size_mb\" = 2)";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse("RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals("Compaction type does not match,", CompactionType.BIN_PACK, optimizeParsed.getCompactionType());
    assertEquals("Options do not match.", "target_file_size_mb", optimizeParsed.getOptionNames().get(0).toString());
    assertEquals("Options do not match.", "2", optimizeParsed.getOptionValues().get(0).toString());
  }

  @Test
  public void testRewriteDataWithBinPack() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c REWRITE DATA USING BIN_PACK");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse("RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals("CompactionType does not match.", CompactionType.BIN_PACK, optimizeParsed.getCompactionType());
  }

  @Test
  public void testRewriteDataWithBinPackOptions() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c REWRITE DATA USING BIN_PACK (target_file_size_mb=2)");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK (\"target_file_size_mb\" = 2)";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse("RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals("CompactionType does not match.", CompactionType.BIN_PACK, optimizeParsed.getCompactionType());
    assertEquals("Options do not match.", "target_file_size_mb", optimizeParsed.getOptionNames().get(0).toString());
    assertEquals("Options do not match.", "2", optimizeParsed.getOptionValues().get(0).toString());
    assertEquals("Options not retrievable", 2L, optimizeParsed.getTargetFileSize().get().longValue());
  }

  @Test
  public void testRewriteDataWithBinPackMultipleOptions() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c REWRITE DATA USING BIN_PACK (target_file_size_mb=2, min_input_files=5)");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK (\"target_file_size_mb\" = 2, \"min_input_files\" = 5)";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse("RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals("CompactionType does not match.", CompactionType.BIN_PACK, optimizeParsed.getCompactionType());
    assertEquals("Options do not match.", "target_file_size_mb", optimizeParsed.getOptionNames().get(0).toString());
    assertEquals("Options do not match.", "2", optimizeParsed.getOptionValues().get(0).toString());
    assertEquals("Options do not match.", "min_input_files", optimizeParsed.getOptionNames().get(1).toString());
    assertEquals("Options do not match.", "5", optimizeParsed.getOptionValues().get(1).toString());
    assertEquals("Options not retrievable", 2L, optimizeParsed.getTargetFileSize().get().longValue());
    assertEquals("Options not retrievable", 5L, optimizeParsed.getMinInputFiles().get().longValue());
    assertEquals("Unset options should be empty", Optional.empty(), optimizeParsed.getMaxFileSize());
    assertEquals("Unset options should be empty", Optional.empty(), optimizeParsed.getMinFileSize());
  }

  private SqlNode parse(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }
}
