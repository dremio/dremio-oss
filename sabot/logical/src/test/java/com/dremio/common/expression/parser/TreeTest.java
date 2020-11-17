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
package com.dremio.common.expression.parser;

import java.io.IOException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.parser.ExprLexer;
import com.dremio.common.expression.parser.ExprParser;
import com.dremio.common.expression.parser.ExprParser.parse_return;
import com.dremio.test.DremioTest;

public class TreeTest extends DremioTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TreeTest.class);

  @Test
  public void escapeStringLiteral() throws Exception {
    String expr = "func(`identifier`, '\\\\d+', 0, 'fjds')";
    testExpressionParsing(expr);
  }

  @Test
  public void escapeQuotedIdentifier() throws Exception {
    String expr = "`a\\\\b` + `c'd`";
    testExpressionParsing(expr);
  }

  @Test
  public void testIfWithCase() throws Exception{
    testExpressionParsing("if ($F1) then case when (_MAP.R_NAME = 'AFRICA') then 2 else 4 end else if(4==3) then 1 else if(x==3) then 7 else (if(2==1) then 6 else 4 end) end");
  }

  @Test
  public void testSideReference() throws Exception {
    testExpressionParsing("INPUT_REFERENCE(3, field1)");
  }

  @Test
  public void testAdd() throws Exception{
    testExpressionParsing("2+2");
  }

  @Test
  public void testIf() throws Exception{
    testExpressionParsing("if ('blue.red') then 'orange' else if (false) then 1 else 0 end");
  }

  @Test
  public void testQuotedIdentifier() throws Exception{
    testExpressionParsing("`hello friend`.`goodbye`");
  }

  @Test
  public void testSpecialQuoted() throws Exception{
    testExpressionParsing("`*0` + `*` ");
  }

  @Test
  public void testQuotedIdentifier2() throws Exception{
    testExpressionParsing("`hello friend`.goodbye");
  }

  @Test
  public void testComplexIdentifier() throws Exception{
    testExpressionParsing("goodbye[4].`hello`");
  }

  @Test // DRILL-2606
  public void testCastToBooleanExpr() throws Exception{
    testExpressionParsing("cast( (cast( (`bool_col` ) as VARCHAR(100) ) ) as BIT )");
  }

  @Test
  public void testInExpr() throws Exception {
    testExpressionParsing("$x in ( 1 , 2, 3, 4, 5)");
  }

  private LogicalExpression parseExpression(String expr) throws RecognitionException, IOException{

    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);

//    tokens.fill();
//    for(Token t : (List<Token>) tokens.getTokens()){
//      System.out.println(t + "" + t.getType());
//    }
//    tokens.rewind();

    ExprParser parser = new ExprParser(tokens);
    parse_return ret = parser.parse();

    return ret.e;

  }

  private String serializeExpression(LogicalExpression expr){

    ExpressionStringBuilder b = new ExpressionStringBuilder();
    StringBuilder sb = new StringBuilder();
    expr.accept(b, sb);
    return sb.toString();
  }

  /**
   * Attempt to parse an expression.  Once parsed, convert it to a string and then parse it again to make sure serialization works.
   * @param expr
   * @throws RecognitionException
   * @throws IOException
   */
  private void testExpressionParsing(String expr) throws RecognitionException, IOException{
    logger.debug("-----" + expr + "-----");
    LogicalExpression e = parseExpression(expr);

    String newStringExpr = serializeExpression(e);
    logger.debug(newStringExpr);
    LogicalExpression e2 = parseExpression(newStringExpr);
    //Assert.assertEquals(e, e2);

  }



}
