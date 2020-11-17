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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CodeModelArrowHelper;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.Types;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.BlockType;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;

class AggrFunctionHolder extends BaseFunctionHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggrFunctionHolder.class);

  private String setup() {
    return meth("setup");
  }
  private String reset() {
    return meth("reset", false);
  }
  private String add() {
    return meth("add");
  }
  private String output() {
    return meth("output");
  }
  private String cleanup() {
    return meth("cleanup", false);
  }

  public AggrFunctionHolder(
      FunctionAttributes attributes,
      FunctionInitializer initializer) {
    super(attributes, initializer);
    checkArgument(attributes.getNullHandling() == NullHandling.INTERNAL, "An aggregation function is required to do its own null handling.");
  }

  @Override
  public boolean isNested(){
    return true;
  }

  @Override
  public boolean isAggregating() {
    return true;
  }

  @Override
  public JVar[] renderStart(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, FunctionErrorContext errorContext) {
    if (!g.getMappingSet().isHashAggMapping()) {  //Declare workspace vars for non-hash-aggregation.
        JVar[] workspaceJVars = declareWorkspaceVariables(g, errorContext);
        generateBody(g, BlockType.SETUP, setup(), null, workspaceJVars, true);
        return workspaceJVars;
      } else {  //Declare workspace vars and workspace vectors for hash aggregation.

        JVar[] workspaceJVars = declareWorkspaceVectors(g);

        JBlock setupBlock = g.getSetupBlock();

        //Loop through all workspace vectors, to get the minimum of size of all workspace vectors.
        JVar sizeVar = setupBlock.decl(g.getModel().INT, "vectorSize", JExpr.lit(Integer.MAX_VALUE));
        JClass mathClass = g.getModel().ref(Math.class);
        for (int id = 0; id<workspaceVars.length; id ++) {
          if (!workspaceVars[id].isInject()) {
            setupBlock.assign(sizeVar,mathClass.staticInvoke("min").arg(sizeVar).arg(g.getWorkspaceVectors().get(workspaceVars[id]).invoke("getValueCapacity")));
          }
        }

        for(int i =0 ; i < workspaceVars.length; i++) {
          if (!workspaceVars[i].isInject()) {
            setupBlock.assign(workspaceJVars[i], JExpr._new(CodeModelArrowHelper.getHolderType(workspaceVars[i].completeType, g.getModel())));
          }
        }

        //Use for loop to initialize entries in the workspace vectors.
        JForLoop forLoop = setupBlock._for();
        JVar ivar = forLoop.init(g.getModel().INT, "dremio_internal_i", JExpr.lit(0));
        forLoop.test(ivar.lt(sizeVar));
        forLoop.update(ivar.assignPlus(JExpr.lit(1)));

        JBlock subBlock = generateInitWorkspaceBlockHA(g, BlockType.SETUP, setup(), workspaceJVars, ivar);
        forLoop.body().add(subBlock);
        return workspaceJVars;
      }


  }


  @Override
  public void renderMiddle(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, JVar[]  workspaceJVars) {
    addProtectedBlock(g, g.getBlock(BlockType.EVAL), add(), inputVariables, workspaceJVars, false);
  }


  @Override
  public HoldingContainer renderEnd(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, JVar[]  workspaceJVars) {
    HoldingContainer out = g.declare(resolvedOutput, false);
    JBlock sub = new JBlock();
    g.getEvalBlock().add(sub);
    JVar internalOutput = sub.decl(JMod.FINAL, CodeModelArrowHelper.getHolderType(resolvedOutput, g.getModel()), getReturnName(), JExpr._new(CodeModelArrowHelper.getHolderType(resolvedOutput, g.getModel())));
    addProtectedBlock(g, sub, output(), null, workspaceJVars, false);
    sub.assign(out.getHolder(), internalOutput);
        //hash aggregate uses workspace vectors. Initialization is done in "setup" and does not require "reset" block.
        if (!g.getMappingSet().isHashAggMapping()) {
          generateBody(g, BlockType.RESET, reset(), null, workspaceJVars, false);
        }
       generateBody(g, BlockType.CLEANUP, cleanup(), null, workspaceJVars, false);

    return out;
  }


  private JVar[] declareWorkspaceVectors(ClassGenerator<?> g) {
    JVar[] workspaceJVars = new JVar[workspaceVars.length];


    for(int i =0 ; i < workspaceVars.length; i++){
      final WorkspaceReference workspaceVar = workspaceVars[i];
      if (workspaceVar.isInject()) {
        String function = FunctionContext.INJECTABLE_GETTER_METHODS.get(workspaceVar.type);
        if(function == null) {
          throw UserException.validationError().message("Unknown inject type %s.", workspaceVar.type.getName()).build(logger);
        }
        workspaceJVars[i] = g.declareClassField("work", g.getModel()._ref(workspaceVars[i].type));
        g.getBlock(BlockType.SETUP).assign(workspaceJVars[i], JExpr.direct("context").invoke(function));
      } else {
        Preconditions.checkState(workspaceVars[i].completeType.isFixedWidthScalar(), "Workspace variable '%s' in aggregation function '%s' is not allowed to have variable length type.", workspaceVars[i].name, registeredNames[0]);

        //workspaceJVars[i] = g.declareClassField("work", g.getHolderType(workspaceVars[i].majorType), JExpr._new(g.getHolderType(workspaceVars[i].majorType)));
        workspaceJVars[i] = g.declareClassField("work", CodeModelArrowHelper.getHolderType(workspaceVars[i].completeType, g.getModel()));

        //Declare a workspace vector for the workspace var.
        TypedFieldId typedFieldId = new TypedFieldId(workspaceVars[i].completeType, g.getWorkspaceTypes().size());
        JVar vv  = g.declareVectorValueSetupAndMember(g.getMappingSet().getWorkspace(), typedFieldId);

        g.getWorkspaceTypes().add(typedFieldId);
        g.getWorkspaceVectors().put(workspaceVars[i], vv);
      }
    }
    return workspaceJVars;
  }

  private JBlock generateInitWorkspaceBlockHA(ClassGenerator<?> g, BlockType bt, String body, JVar[] workspaceJVars, JExpression wsIndexVariable){
    JBlock initBlock = new JBlock(true, true);
    if(!Strings.isNullOrEmpty(body) && !body.trim().isEmpty()){
      JBlock sub = new JBlock(true, true);
      addProtectedBlockHA(g, sub, body, null, workspaceJVars, wsIndexVariable);
      initBlock.directStatement(String.format("/** start %s for function %s **/ ", bt.name(), registeredNames[0]));
      initBlock.add(sub);
      initBlock.directStatement(String.format("/** end %s for function %s **/ ", bt.name(), registeredNames[0]));
    }
    return initBlock;
  }

  @Override
  protected void addProtectedBlock(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables, JVar[] workspaceJVars, boolean decConstantInputOnly){
    if (!g.getMappingSet().isHashAggMapping()) {
      super.addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, decConstantInputOnly);
    } else {
      JExpression indexVariable = g.getMappingSet().getWorkspaceIndex();
      addProtectedBlockHA(g, sub, body, inputVariables, workspaceJVars, indexVariable);
    }
  }

  /*
   * This is customized version of "addProtectedBlock" for hash aggregation. It take one additional parameter "wsIndexVariable".
   */
  private void addProtectedBlockHA(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables, JVar[] workspaceJVars, JExpression wsIndexVariable){
    if (inputVariables != null){
      for(int i =0; i < inputVariables.length; i++){
        ValueReference parameter = parameters[i];
        HoldingContainer inputVariable = inputVariables[i];
        sub.decl(inputVariable.getHolder().type(), parameter.getName(), inputVariable.getHolder());
      }
    }

    JVar[] internalVars = new JVar[workspaceJVars.length];
    for(int i =0; i < workspaceJVars.length; i++){

      if (workspaceVars[i].isInject()) {
        internalVars[i] = sub.decl(g.getModel()._ref(workspaceVars[i].type), workspaceVars[i].name, workspaceJVars[i]);
        continue;
      }
      //sub.assign(workspaceJVars[i], JExpr._new(g.getHolderType(workspaceVars[i].majorType)));
      //Access workspaceVar through workspace vector.
      JConditional cond = sub._if(g.getWorkspaceVectors().get(workspaceVars[i]).invoke("isNull").arg(wsIndexVariable).not());
      JInvocation getValueAccessor = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("get");
      if (workspaceVars[i].completeType == CompleteType.OBJECT) {
        cond._then().add(getValueAccessor.arg(wsIndexVariable).arg(workspaceJVars[i]));
      } else if (workspaceVars[i].completeType.getType().getTypeID() == ArrowType.ArrowTypeID.Decimal) {
        cond._then().assign(workspaceJVars[i].ref("buffer"), getValueAccessor.arg(wsIndexVariable));
      } else {
        cond._then().assign(workspaceJVars[i].ref("value"), getValueAccessor.arg(wsIndexVariable));
      }
      internalVars[i] = sub.decl(CodeModelArrowHelper.getHolderType(workspaceVars[i].completeType, g.getModel()),  workspaceVars[i].name, workspaceJVars[i]);
    }

    Preconditions.checkNotNull(body);
    sub.directStatement(body);

    // reassign workspace variables back.
    for(int i =0; i < workspaceJVars.length; i++){
      sub.assign(workspaceJVars[i], internalVars[i]);

      // Injected buffers are not stored as vectors skip storing them in vectors
      if (workspaceVars[i].isInject()) {
        continue;
      }
      //Change workspaceVar through workspace vector.
      JInvocation setMeth;
      CompleteType type = workspaceVars[i].completeType;
      if (Types.usesHolderForGet(type.toMinorType())) {
          setMeth = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("setSafe").arg(wsIndexVariable).arg(workspaceJVars[i]);
      }else{
        if (!type.isFixedWidthScalar()) {
          setMeth = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("setSafe").arg(wsIndexVariable).arg(workspaceJVars[i].ref("value"));
        } else {
          setMeth = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("set").arg(wsIndexVariable).arg(workspaceJVars[i].ref("value"));
        }
      }

      sub.add(setMeth);

      JClass runTimeException = g.getModel().ref(RuntimeException.class);
    }

  }

}
