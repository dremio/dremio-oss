package com.dremio.common.expression;
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
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class FunctionCallArgsTest {

  @Test
  public void testToStringOfFunctionCall() {
    FunctionCall functionCall = new FunctionCall("test", null);
    String s = functionCall.toString();
    Assert.assertEquals("FunctionCall toString() returned unexpected results","FunctionCall [func=test, args=]", s);
    List<LogicalExpression> exps = new ArrayList<>();
    LogicalExpression exp = new ValueExpressions.IntExpression(2);
    exps.add(exp);
    LogicalExpression exp1 = new ValueExpressions.IntExpression(3);
    exps.add(exp1);
    LogicalExpression exp2 = new ValueExpressions.IntExpression(4);
    exps.add(exp2);
    LogicalExpression exp3 = new ValueExpressions.IntExpression(5);
    exps.add(exp3);
    LogicalExpression exp4 = new ValueExpressions.IntExpression(6);
    exps.add(exp4);
    LogicalExpression exp5 = new ValueExpressions.IntExpression(7);
    exps.add(exp5);
    LogicalExpression exp6 = new ValueExpressions.IntExpression(8);
    exps.add(exp6);
    LogicalExpression exp7 = new ValueExpressions.IntExpression(9);
    exps.add(exp7);
    functionCall = new FunctionCall("test", exps);
    s = functionCall.toString();
    Assert.assertEquals("FunctionCall toString() returned unexpected result","FunctionCall [func=test, args=Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true)]", s);
    LogicalExpression exp8 = new ValueExpressions.IntExpression(10);
    exps.add(exp8);
    LogicalExpression exp9 = new ValueExpressions.IntExpression(11);
    exps.add(exp9);
    LogicalExpression exp10 = new ValueExpressions.IntExpression(12);
    exps.add(exp10);
    functionCall = new FunctionCall("test", exps);
    s = functionCall.toString();
    Assert.assertEquals("FunctionCall toString() returned unexpected result","FunctionCall [func=test, args=Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true), Int(32, true)]", s);
  }
}
