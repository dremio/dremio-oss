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
package com.dremio.service.autocomplete;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for SqlStateMachine.
 */
public final class SqlStateMachineTests {
  @Test
  public void regularQuery() {
    executeTest("SELECT * FROM c", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void singleLineComment() {
    executeTest("--Blah Blah2", SqlStateMachine.State.IN_SINGLE_LINE_COMMENT);
  }

  @Test
  public void singleLineCommentWithNewLine() {
    executeTest("--Blah\nBlah2", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void multiLineComment() {
    executeTest("/*Blah\nBlah2", SqlStateMachine.State.IN_MULTILINE_COMMENT);
  }

  @Test
  public void multiLineCommentWithTerminator() {
    executeTest("/*Blah\nBlah2*/", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void multiLineCommenTerminatorWithoutStart() {
    executeTest("\"Hello World", SqlStateMachine.State.IN_DOUBLE_QUOTE_STRING_LITERAL);
  }

  @Test
  public void stringLiteral() {
    executeTest("\"Hello World", SqlStateMachine.State.IN_DOUBLE_QUOTE_STRING_LITERAL);
  }

  @Test
  public void stringLiteralWithTerminator() {
    executeTest("\"Hello World\"", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void singleQuote() {
    executeTest("'String literal that starts with single quote", SqlStateMachine.State.IN_SINGLE_QUOTE_STRING_LITERAL);
  }

  @Test
  public void singleQuoteInDoubleQuote() {
    executeTest("\"'String literal that starts with double quote", SqlStateMachine.State.IN_DOUBLE_QUOTE_STRING_LITERAL);
  }

  @Test
  public void doubleQuoteInSingleQuote() {
    executeTest("'\"String literal that starts with single quote", SqlStateMachine.State.IN_SINGLE_QUOTE_STRING_LITERAL);
  }

  @Test
  public void commentInStringLiteral() {
    executeTest("\"/*This is a comment -- inside of a string literal", SqlStateMachine.State.IN_DOUBLE_QUOTE_STRING_LITERAL);
  }

  @Test
  public void commentInComment() {
    executeTest("-- /*This is a comment --\"inside of a comment", SqlStateMachine.State.IN_SINGLE_LINE_COMMENT);
  }

  @Test
  public void escapeCharacterNoAction() {
    executeTest("\\", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void escapeCharacterSingleQuote() {
    executeTest("\\'", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void escapeCharacterDoubleQuote() {
    executeTest("\\\"", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void escapeCharacterNonEscapeCode() {
    executeTest("\\hello", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void escapeCharacterComment() {
    executeTest("\\--hello", SqlStateMachine.State.IN_SINGLE_LINE_COMMENT);
  }

  @Test
  public void numericLiteralAtStart() {
    executeTest("1234", SqlStateMachine.State.IN_NUMERIC_LITERAL);
  }

  @Test
  public void numericLiteralAtNonStart() {
    executeTest("hello 1234", SqlStateMachine.State.IN_NUMERIC_LITERAL);
  }

  @Test
  public void numericLiteralAtNonStart2() {
    executeTest("hello\n1234", SqlStateMachine.State.IN_NUMERIC_LITERAL);
  }

  @Test
  public void codeWithNumericSuffix() {
    executeTest("hello1234", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void numericLiteralWithTerminater() {
    executeTest("1234 ", SqlStateMachine.State.IN_CODE);
  }

  @Test
  public void numbersInsideNonCode() {
    executeTest("--1234", SqlStateMachine.State.IN_SINGLE_LINE_COMMENT);
  }

  private static void executeTest(String sql, SqlStateMachine.State expectedState) {
    SqlStateMachine.State actualState = SqlStateMachine.getState(sql);
    Assert.assertEquals(expectedState, actualState);
  }
}
