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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCompoundIdentifier {
  @Test
  public void testDoubleQuoted() {
    final ParserConfig parserConfig = new ParserConfig(Quoting.DOUBLE_QUOTE, 100);
    final String sql = "SELECT * FROM \"a\".b.\"c.d.e\"";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlIdentifier compoundId = (SqlIdentifier) ((SqlSelect) sqlNode).getFrom();
    Assertions.assertEquals(ImmutableList.of("a", "b", "c.d.e"), compoundId.names);
    Assertions.assertTrue(compoundId.isComponentQuoted(0));
    Assertions.assertFalse(compoundId.isComponentQuoted(1));
    Assertions.assertTrue(compoundId.isComponentQuoted(2));
  }

  @Test
  public void testBracketed() {
    final ParserConfig parserConfig = new ParserConfig(Quoting.BRACKET, 100);
    final String sql = "SELECT * FROM [\"a\"].b.[c.d.e]";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlIdentifier compoundId = (SqlIdentifier) ((SqlSelect) sqlNode).getFrom();
    Assertions.assertEquals(ImmutableList.of("\"a\"", "b", "c.d.e"), compoundId.names);
    Assertions.assertTrue(compoundId.isComponentQuoted(0));
    Assertions.assertFalse(compoundId.isComponentQuoted(1));
    Assertions.assertTrue(compoundId.isComponentQuoted(2));
  }

  @Test
  public void testBackticked() {
    final ParserConfig parserConfig = new ParserConfig(Quoting.BACK_TICK, 100);
    final String sql = "SELECT * FROM `\"a\"`.b.`c.d.e`";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlIdentifier compoundId = (SqlIdentifier) ((SqlSelect) sqlNode).getFrom();
    Assertions.assertEquals(ImmutableList.of("\"a\"", "b", "c.d.e"), compoundId.names);
    Assertions.assertTrue(compoundId.isComponentQuoted(0));
    Assertions.assertFalse(compoundId.isComponentQuoted(1));
    Assertions.assertTrue(compoundId.isComponentQuoted(2));
  }
}
