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

import static com.dremio.exec.calcite.SqlNodes.DREMIO_DIALECT;
import static com.dremio.exec.planner.sql.parser.TestParserUtil.parse;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

public class TestSqlOptimize {
  private SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);

  @Test
  public void testOperandSetter() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    optimizeParsed.setOperand(0, optimizeParsed.getTable().setName(0, "d"));
    optimizeParsed.setOperand(3, SqlLiteral.createSymbol(CompactionType.SORT, SqlParserPos.ZERO));
    optimizeParsed.setOperand(4, new SqlIdentifier("e", SqlParserPos.ZERO));
    optimizeParsed.setOperand(5, SqlNodeList.of(new SqlIdentifier("f", SqlParserPos.ZERO)));
    optimizeParsed.setOperand(6, SqlNodeList.of(new SqlIdentifier("g", SqlParserPos.ZERO)));

    optimizeParsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedString =
        "OPTIMIZE TABLE \"d\".\"b\".\"c\" USING SORT FOR PARTITIONS \"e\" (\"f\" = \"g\")";
    assertEquals(expectedString, actualString);
  }

  @Test
  public void testOperandSetterRewriteManifest() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    optimizeParsed.setOperand(0, optimizeParsed.getTable().setName(0, "d"));
    optimizeParsed.setOperand(1, SqlLiteral.createBoolean(true, SqlParserPos.ZERO));
    optimizeParsed.setOperand(2, SqlLiteral.createBoolean(false, SqlParserPos.ZERO));

    optimizeParsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedString = "OPTIMIZE TABLE \"d\".\"b\".\"c\" REWRITE MANIFESTS";
    assertEquals(expectedString, actualString);
  }

  @Test
  public void testOperandSetterRewriteDataFiles() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    optimizeParsed.setOperand(0, optimizeParsed.getTable().setName(0, "d"));
    optimizeParsed.setOperand(1, SqlLiteral.createBoolean(false, SqlParserPos.ZERO));
    optimizeParsed.setOperand(2, SqlLiteral.createBoolean(true, SqlParserPos.ZERO));

    optimizeParsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedString = "OPTIMIZE TABLE \"d\".\"b\".\"c\" REWRITE DATA USING BIN_PACK";
    assertEquals(expectedString, actualString);
  }

  @Test
  public void testBasic() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" USING BIN_PACK";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertTrue(
        "RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals(
        "Compaction type does not match,",
        CompactionType.BIN_PACK,
        optimizeParsed.getCompactionType());
  }

  @Test
  public void testBasicWithBinPack() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c USING BIN_PACK");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "OPTIMIZE TABLE \"a\".\"b\".\"c\" USING BIN_PACK";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertTrue(
        "RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals(
        "Compaction type does not match,",
        CompactionType.BIN_PACK,
        optimizeParsed.getCompactionType());
  }

  @Test
  public void testBasicWithOptions() throws SqlParseException {
    SqlNode parsed = parse("OPTIMIZE TABLE a.b.c (target_file_size_mb=2)");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "OPTIMIZE TABLE \"a\".\"b\".\"c\" USING BIN_PACK (\"target_file_size_mb\" = 2)";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertTrue(
        "RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals(
        "Compaction type does not match,",
        CompactionType.BIN_PACK,
        optimizeParsed.getCompactionType());
    assertEquals(
        "Options do not match.",
        "target_file_size_mb",
        optimizeParsed.getOptionNames().get(0).toString());
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
    assertFalse(
        "RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertTrue(
        "RewriteDataFiles is incorrect.", optimizeParsed.getRewriteDataFiles().booleanValue());
    assertEquals(
        "CompactionType does not match.",
        CompactionType.BIN_PACK,
        optimizeParsed.getCompactionType());
  }

  @Test
  public void testRewriteDataWithBinPackOptions() throws SqlParseException {
    SqlNode parsed =
        parse("OPTIMIZE TABLE a.b.c REWRITE DATA USING BIN_PACK (target_file_size_mb=2)");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK (\"target_file_size_mb\" = 2)";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse(
        "RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals(
        "CompactionType does not match.",
        CompactionType.BIN_PACK,
        optimizeParsed.getCompactionType());
    assertEquals(
        "Options do not match.",
        "target_file_size_mb",
        optimizeParsed.getOptionNames().get(0).toString());
    assertEquals("Options do not match.", "2", optimizeParsed.getOptionValues().get(0).toString());
    assertEquals(
        "Options not retrievable", 2L, optimizeParsed.getTargetFileSize().get().longValue());
  }

  @Test
  public void testRewriteDataWithBinPackMultipleOptions() throws SqlParseException {
    SqlNode parsed =
        parse(
            "OPTIMIZE TABLE a.b.c REWRITE DATA USING BIN_PACK (target_file_size_mb=2, min_input_files=5)");
    assertTrue(parsed instanceof SqlOptimize);
    SqlOptimize optimizeParsed = (SqlOptimize) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "OPTIMIZE TABLE \"a\".\"b\".\"c\" REWRITE DATA USING BIN_PACK (\"target_file_size_mb\" = 2, \"min_input_files\" = 5)";
    assertEquals(actualString, expectedUnparsedString);

    assertEquals("Table name does not match.", "a.b.c", optimizeParsed.getTable().toString());
    assertFalse(
        "RewriteManifests is incorrect.", optimizeParsed.getRewriteManifests().booleanValue());
    assertEquals(
        "CompactionType does not match.",
        CompactionType.BIN_PACK,
        optimizeParsed.getCompactionType());
    assertEquals(
        "Options do not match.",
        "target_file_size_mb",
        optimizeParsed.getOptionNames().get(0).toString());
    assertEquals("Options do not match.", "2", optimizeParsed.getOptionValues().get(0).toString());
    assertEquals(
        "Options do not match.",
        "min_input_files",
        optimizeParsed.getOptionNames().get(1).toString());
    assertEquals("Options do not match.", "5", optimizeParsed.getOptionValues().get(1).toString());
    assertEquals(
        "Options not retrievable", 2L, optimizeParsed.getTargetFileSize().get().longValue());
    assertEquals(
        "Options not retrievable", 5L, optimizeParsed.getMinInputFiles().get().longValue());
    assertEquals(
        "Unset options should be empty", Optional.empty(), optimizeParsed.getMaxFileSize());
    assertEquals(
        "Unset options should be empty", Optional.empty(), optimizeParsed.getMinFileSize());
  }

  @Test
  public void testDataOptionsWithRewriteManifests() {
    assertThatThrownBy(() -> parse("OPTIMIZE TABLE a.b.c REWRITE MANIFESTS USING BIN_PACK"))
        .isInstanceOf(SqlParseException.class);

    assertThatThrownBy(
            () ->
                parse(
                    "OPTIMIZE TABLE a.b.c REWRITE MANIFESTS (\"target_file_size_mb\" = 2, \"min_input_files\" = 5)"))
        .isInstanceOf(SqlParseException.class);
  }
}
