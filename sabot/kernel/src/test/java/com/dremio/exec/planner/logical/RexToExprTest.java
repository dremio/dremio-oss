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
package com.dremio.exec.planner.logical;

import static com.dremio.exec.planner.physical.PlannerSettings.CASE_EXPRESSIONS_THRESHOLD;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.parser.ExprLexer;
import com.dremio.common.expression.parser.ExprParser;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.DremioTest;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableList;

public class RexToExprTest {

  @Test
  public void testUnsupportedRexNode() {
    /*
     * Method checks if we raise the appropriate error while dealing with RexNode that cannot be converted to
     * equivalent Dremio expressions
     */
    try {
      // Create the data type factory.
      RelDataTypeFactory relFactory = SqlTypeFactoryImpl.INSTANCE;
      // Create the rex builder
      RexBuilder rex = new DremioRexBuilder(relFactory);
      RelDataType anyType = relFactory.createSqlType(SqlTypeName.ANY);
      List<RexNode> emptyList = new LinkedList<>();
      ImmutableList<RexFieldCollation> e = ImmutableList.copyOf(new RexFieldCollation[0]);

      // create a dummy RexOver object.
      RexNode window = rex.makeOver(anyType, SqlStdOperatorTable.AVG, emptyList, emptyList, e, null, null, true,
          false, false, false);
      RexToExpr.toExpr(buildContext(), null, null, window);
    } catch (UserException e) {
      if (e.getMessage().contains(RexToExpr.UNSUPPORTED_REX_NODE_ERROR)) {
        // got expected error return
        return;
      }
      Assert.fail("Hit exception with unexpected error message");
    }

    Assert.fail("Failed to raise the expected exception");
  }

  @Test
  public void testLargeCaseStatement() throws Exception {
    /*
     * Test round trip of a large case statement:
     * RexNode -> LogicalExpression -> Serialize -> Deserialize
     */

    // Create the data type factory.
    RelDataTypeFactory relFactory = SqlTypeFactoryImpl.INSTANCE;

    // Create the rex builder
    RexBuilder rex = new DremioRexBuilder(relFactory);

    RelDataType intType = relFactory.createSqlType(SqlTypeName.BIGINT);
    List<RexNode> rexNodes = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      RexNode whenCond = rex.makeCall(SqlStdOperatorTable.EQUALS, rex.makeBigintLiteral(BigDecimal.valueOf(i)), rex.makeBigintLiteral(BigDecimal.valueOf(i)));
      RexNode whenValue = rex.makeBigintLiteral(BigDecimal.valueOf(i));
      rexNodes.add(whenCond);
      rexNodes.add(whenValue);
    }

    rexNodes.add(rex.makeNullLiteral(intType));
    RexNode caseStatement = rex.makeCall(SqlStdOperatorTable.CASE, rexNodes);
    LogicalExpression expr = RexToExpr.toExpr(buildContext(), null, null, caseStatement);
    String exprString = serializeExpression(expr);
    LogicalExpression parsedExpr = parseExpression(exprString);
    assertEquals(expr.toString(), parsedExpr.toString());
  }

  private LogicalExpression parseExpression(String expr) throws Exception {
    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    ExprParser.parse_return ret = parser.parse();
    return ret.e;
  }

  private String serializeExpression(LogicalExpression expr) {
    ExpressionStringBuilder b = new ExpressionStringBuilder();
    StringBuilder sb = new StringBuilder();
    expr.accept(b, sb);
    return sb.toString();
  }

  private ParseContext buildContext() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(
        new OptionResolverSpec()
            .addOption(CASE_EXPRESSIONS_THRESHOLD, 2));
    return new ParseContext(new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionResolver, null));
  }
}
