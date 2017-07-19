/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.common.utils;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link SqlUtils}
 */
public class TestSqlUtils {

  @Test
  public void testIsKeyword() {
    assertTrue(SqlUtils.isKeyword("USER"));
    assertTrue(SqlUtils.isKeyword("FiLeS"));
    assertFalse(SqlUtils.isKeyword("myUSER"));
  }

  @Test
  public void testQuoteIdentifier() {
    assertEquals("\"window\"", SqlUtils.quoteIdentifier("window"));
    assertEquals("\"metadata\"", SqlUtils.quoteIdentifier("metadata"));

    assertEquals("abc", SqlUtils.quoteIdentifier("abc"));
    assertEquals("abc123", SqlUtils.quoteIdentifier("abc123"));
    assertEquals("a_bc", SqlUtils.quoteIdentifier("a_bc"));
    assertEquals("\"a_\"\"bc\"", SqlUtils.quoteIdentifier("a_\"bc"));
    assertEquals("\"a.\"\"bc\"", SqlUtils.quoteIdentifier("a.\"bc"));

    assertEquals("\"ab-c\"", SqlUtils.quoteIdentifier("ab-c"));
    assertEquals("\"ab/c\"", SqlUtils.quoteIdentifier("ab/c"));
    assertEquals("\"ab.c\"", SqlUtils.quoteIdentifier("ab.c"));
    assertEquals("\"123\"", SqlUtils.quoteIdentifier("123"));
    assertEquals("U&\"foo\\000abar\"", SqlUtils.quoteIdentifier("foo\nbar"));
  }

  @Test
  public void testParseSchemaPath() {
    assertEquals(asList("a", "b", "c"), SqlUtils.parseSchemaPath("a.b.c"));
    assertEquals(asList("a"), SqlUtils.parseSchemaPath("a"));
    assertEquals(asList("a", "b.c", "d"), SqlUtils.parseSchemaPath("a.\"b.c\".d"));
    assertEquals(asList("a", "c"), SqlUtils.parseSchemaPath("a..c"));
  }
}
