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
package com.dremio.exec.expr.fn;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.expression.CodeModelArrowHelper;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.BlockType;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JOp;
import com.sun.codemodel.JVar;

public class SimpleFunctionHolder extends BaseFunctionHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleFunctionHolder.class);

  private final String functionClass;

  public SimpleFunctionHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
    functionClass = checkNotNull(initializer.getClassName());
  }

  private String setupBody() {
    return meth("setup", false);
  }
  private String evalBody() {
    return meth("eval");
  }
  private String resetBody() {
    return meth("reset", false);
  }
  private String cleanupBody() {
    return meth("cleanup", false);
  }

  @Override
  public boolean isNested() {
    return false;
  }

  public SimpleFunction createInterpreter() throws Exception {
    return (SimpleFunction)Class.forName(functionClass).newInstance();
  }

  @Override
  public HoldingContainer renderEnd(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, JVar[]  workspaceJVars){
    //If the function's annotation specifies a parameter has to be constant expression, but the HoldingContainer
    //for the argument is not, then raise exception.
    for (int i =0; i < inputVariables.length; i++) {
      if (parameters[i].isConstant() && !inputVariables[i].isConstant()) {
        throw new IllegalArgumentException(String.format("The argument '%s' of Function '%s' has to be constant!", parameters[i].getName(), this.getRegisteredNames()[0]));
      }
    }
    generateBody(g, BlockType.SETUP, setupBody(), inputVariables, workspaceJVars, true);
    HoldingContainer c = generateEvalBody(g, resolvedOutput, inputVariables, evalBody(), workspaceJVars);
    generateBody(g, BlockType.RESET, resetBody(), null, workspaceJVars, false);
    generateBody(g, BlockType.CLEANUP, cleanupBody(), null, workspaceJVars, false);
    return c;
  }

  protected HoldingContainer generateEvalBody(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars) {

    g.getEvalBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);
    JBlock topSub = sub;
    HoldingContainer out = null;


    // add outside null handling if it is defined.
    if (nullHandling == NullHandling.NULL_IF_NULL) {
      JExpression e = null;
      for (HoldingContainer v : inputVariables) {
        final JExpression isNullExpr;
        if (v.isReader()) {
          isNullExpr = JOp.cond(v.getHolder().invoke("isSet"), JExpr.lit(1), JExpr.lit(0));
        } else {
          isNullExpr = v.getIsSet();
        }
        if (e == null) {
          e = isNullExpr;
        } else {
          e = e.mul(isNullExpr);
        }
      }

      if (e != null) {
        // if at least one expression must be checked, set up the conditional.
        out = g.declare(resolvedOutput);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }

    if (out == null) {
      out = g.declare(resolvedOutput);
    }

    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);


    JVar internalOutput = sub.decl(JMod.FINAL, CodeModelArrowHelper.getHolderType(resolvedOutput, g.getModel()), getReturnName(), JExpr._new(CodeModelArrowHelper.getHolderType(resolvedOutput, g.getModel())));
    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, false);

    if (sub != topSub || inputVariables.length == 0) {
      sub.assign(internalOutput.ref("isSet"), JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    }
    sub.assign(out.getHolder(), internalOutput);

    g.getEvalBlock().directStatement(String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return out;
  }
}
