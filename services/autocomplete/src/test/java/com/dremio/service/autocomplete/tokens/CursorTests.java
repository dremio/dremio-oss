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

import org.junit.Assert;
import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Test for the SQL autocomplete resolver.
 */
public final class CursorTests {
  private static final DremioToken TOKEN_ENDING_WITH_CURSOR = new DremioToken(0, "ASDF" + Cursor.CURSOR_CHARACTER);
  private static final DremioToken TOKEN_NOT_ENDING_WITH_CURSOR = new DremioToken(0, "ASDF");
  private static final ImmutableList<DremioToken> LIST_WITH_CURSOR = ImmutableList.of(
    DremioToken.createFromParserKind(0),
    TOKEN_ENDING_WITH_CURSOR,
    DremioToken.createFromParserKind(1));

  private static final ImmutableList<DremioToken> LIST_WITHOUT_CURSOR = ImmutableList.of(
    DremioToken.createFromParserKind(0),
    DremioToken.createFromParserKind(1));

  @Test
  public void testTokenEndsWithCursor() {
    Assert.assertTrue(Cursor.tokenEndsWithCursor(TOKEN_ENDING_WITH_CURSOR));
    Assert.assertFalse(Cursor.tokenEndsWithCursor(TOKEN_NOT_ENDING_WITH_CURSOR));
  }

  @Test
  public void testTokensHasCursor() {
    Assert.assertTrue(Cursor.tokensHasCursor(LIST_WITH_CURSOR));
    Assert.assertFalse(Cursor.tokensHasCursor(LIST_WITHOUT_CURSOR));
  }

  @Test
  public void testIndexOfTokenWithCursor() {
    Assert.assertEquals(1,Cursor.indexOfTokenWithCursor(LIST_WITH_CURSOR));
    Assert.assertEquals(-1, Cursor.indexOfTokenWithCursor(LIST_WITHOUT_CURSOR));
  }

  @Test
  public void testTokenizeWithCursor() {
    GoldenFileTestBuilder.create(CursorTests::testTokenizeWithCursorImplementation)
      .add("EMPTY STRING", new Input("", 0))
      .add("END OF TOKEN", new Input("HELLO", "HELLO".length()))
      .add("START OF TOKEN", new Input("HELLO", 0))
      .add("Unattached Bell Character", new Input("HELLO ", "HELLO".length() + 1))
      .runTests();
  }

  public static ImmutableList<DremioToken> testTokenizeWithCursorImplementation(Input input) {
    return Cursor.tokenizeWithCursor(input.getCorpus(), input.getPosition());
  }

  public static final class Input {
    private final String corpus;
    private final int position;

    public Input(String corpus, int position) {
      this.corpus = corpus;
      this.position = position;
    }

    public String getCorpus() {
      return corpus;
    }

    public int getPosition() {
      return position;
    }
  }
}
