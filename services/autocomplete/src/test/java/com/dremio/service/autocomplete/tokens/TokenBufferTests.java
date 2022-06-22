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
package com.dremio.service.autocomplete.tokens;

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.BEL;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.FROM;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.IDENTIFIER;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.LPAREN;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.PLUS;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.RPAREN;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.SELECT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.STAR;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.service.autocomplete.statements.grammar.ParseException;
import com.google.common.collect.ImmutableList;

public final class TokenBufferTests {
  private static final ImmutableList<DremioToken> SAMPLE_TOKENS = ImmutableList.of(
    DremioToken.createFromParserKind(SELECT),
    DremioToken.createFromParserKind(STAR),
    DremioToken.createFromParserKind(FROM),
    new DremioToken(IDENTIFIER, "EMP"));

  @Test
  public void testPeek() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0), tokenBuffer.peek());
  }

  @Test
  public void testPeekKind() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0).getKind(), tokenBuffer.peekKind());
  }

  @Test
  public void testKindIs() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertTrue(tokenBuffer.kindIs(SAMPLE_TOKENS.get(0).getKind()));
  }

  @Test
  public void testRead() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0), tokenBuffer.read());
    Assert.assertEquals(SAMPLE_TOKENS.get(1).getKind(), tokenBuffer.peekKind());
    Assert.assertEquals(SAMPLE_TOKENS.get(1), tokenBuffer.read());
    Assert.assertEquals(SAMPLE_TOKENS.get(2), tokenBuffer.read());
    Assert.assertEquals(SAMPLE_TOKENS.get(3), tokenBuffer.read());
    Assert.assertEquals(null, tokenBuffer.read());
  }

  @Test
  public void testReadKind() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0).getKind(), tokenBuffer.readKind());
  }

  @Test
  public void testReadImage() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0).getImage(), tokenBuffer.readImage());
  }

  @Test
  public void testReadIf() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0), tokenBuffer.readIf((token) -> true));
    Assert.assertEquals(SAMPLE_TOKENS.get(1).getKind(), tokenBuffer.peekKind());
    Assert.assertEquals(null, tokenBuffer.readIf((token) -> false));
    Assert.assertEquals(SAMPLE_TOKENS.get(1).getKind(), tokenBuffer.peekKind());
  }

  @Test
  public void testReadAndCheckKind() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0), tokenBuffer.readAndCheckKind(SAMPLE_TOKENS.get(0).getKind()));

    try {
      tokenBuffer.readAndCheckKind(BEL);
      Assert.fail("Expected an exception");
    } catch (ParseException pe) {
      // Do Nothing
    }
  }

  @Test
  public void testIsEmpty() {
    Assert.assertTrue(new TokenBuffer(ImmutableList.of()).isEmpty());

    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertFalse(tokenBuffer.isEmpty());
    Assert.assertEquals(SAMPLE_TOKENS.get(0), tokenBuffer.read());
    Assert.assertEquals(SAMPLE_TOKENS.get(1), tokenBuffer.read());
    Assert.assertEquals(SAMPLE_TOKENS.get(2), tokenBuffer.read());
    Assert.assertEquals(SAMPLE_TOKENS.get(3), tokenBuffer.read());
    Assert.assertTrue(tokenBuffer.isEmpty());
  }

  @Test
  public void testToList() {
    TokenBuffer tokenBuffer = new TokenBuffer(SAMPLE_TOKENS);
    Assert.assertEquals(SAMPLE_TOKENS.get(0), tokenBuffer.read());
    Assert.assertEquals(SAMPLE_TOKENS.size() - 1, tokenBuffer.toList().size());
  }

  @Test
  public void testReadUntil() {
    Assert.assertEquals(2, new TokenBuffer(SAMPLE_TOKENS).readUntilKind(FROM).size());
    Assert.assertEquals(SAMPLE_TOKENS.size(), new TokenBuffer(SAMPLE_TOKENS).readUntilKind(-1).size());
  }

  @Test
  public void testReadUntilMatchAtSameLevel() {
    ImmutableList<DremioToken> nestedParens = ImmutableList.of(
      DremioToken.createFromParserKind(LPAREN),
        DremioToken.createFromParserKind(LPAREN),
        DremioToken.createFromParserKind(SELECT),
        DremioToken.createFromParserKind(PLUS),
        DremioToken.createFromParserKind(SELECT),
        DremioToken.createFromParserKind(RPAREN),
          DremioToken.createFromParserKind(PLUS),
        DremioToken.createFromParserKind(LPAREN),
        DremioToken.createFromParserKind(SELECT),
        DremioToken.createFromParserKind(PLUS),
        DremioToken.createFromParserKind(SELECT),
        DremioToken.createFromParserKind(RPAREN),
          DremioToken.createFromParserKind(PLUS),
        DremioToken.createFromParserKind(LPAREN),
        DremioToken.createFromParserKind(SELECT),
        DremioToken.createFromParserKind(PLUS),
        DremioToken.createFromParserKind(SELECT),
        DremioToken.createFromParserKind(RPAREN),
      DremioToken.createFromParserKind(RPAREN));
    TokenBuffer tokenBuffer = new TokenBuffer(nestedParens);
    tokenBuffer.read();
    // We should read past the first PLUS, since it's in a nested level
    Assert.assertEquals(5, tokenBuffer.readUntilMatchKindAtSameLevel(PLUS).size());
  }
}
