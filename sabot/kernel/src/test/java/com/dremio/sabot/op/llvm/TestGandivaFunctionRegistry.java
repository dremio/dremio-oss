/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.llvm;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.expr.fn.GandivaFunctionHolder;
import com.dremio.exec.expr.fn.GandivaFunctionRegistry;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

@Ignore
public class TestGandivaFunctionRegistry extends ExecTest {

  /*
   * Test that function lookups on the gandiva repository works.
   */
  @Test
  public void testGandivaPluggableRegistry() throws Exception {
    GandivaFunctionRegistry fnRegistry = new GandivaFunctionRegistry(DEFAULT_SABOT_CONFIG);

    FunctionCall fnCall = getAddFn();
    GandivaFunctionHolder holder = (GandivaFunctionHolder)fnRegistry.getFunction(fnCall);
    Assert.assertNotNull(holder);
    ArrowType.Int int32 = new ArrowType.Int(32, true);
    CompleteType expectedReturnType = new CompleteType(int32);
    Assert.assertEquals(expectedReturnType, holder.getReturnType());
    Assert.assertNotNull(holder.getExpr(fnCall.getName(), fnCall.args));
  }

  /*
   * Test that dremio repository is integrated with Gandiva as a pluggable repository/
   */
  @Test
  public void testFunctionImplementationRegistry() {
    FunctionImplementationRegistry fnRegistry = FUNCTIONS();
    GandivaFunctionHolder holder = (GandivaFunctionHolder)fnRegistry.findNonFunction(getAddFn());
    Assert.assertNotNull(holder);
  }

  private FunctionCall getAddFn() {
    List<LogicalExpression> args = Lists.newArrayList(ValueExpressions.getInt(1), ValueExpressions
      .getInt(2));
    return new FunctionCall("add", args);
  }
}
