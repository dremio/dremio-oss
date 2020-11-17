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

import java.util.List;

import com.dremio.exec.expr.annotations.FunctionTemplate;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.CodeModelArrowHelper;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.HiveFuncHolderExpr;
import com.dremio.exec.expr.fn.impl.hive.DeferredObject;
import com.dremio.exec.expr.fn.impl.hive.ObjectInspectorHelper;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JVar;

public class HiveFuncHolder extends AbstractFunctionHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);
  static final Logger DEPRECATED_FUNCTION_WARNING_LOGGER =
    LoggerFactory.getLogger("hive.deprecated.function.warning.logger");

  private CompleteType[] argTypes;
  private ObjectInspector returnOI;
  private CompleteType returnType;
  private Class<? extends GenericUDF> genericUdfClazz;
  private boolean isGenericUDF = true;
  private Class<? extends UDF> udfClazz = null;
  private String udfName = "";
  private boolean isRandom;


  /**
   * Create holder for GenericUDF
   * @param genericUdfClazz implementation class
   * @param argTypes
   * @param returnOI
   * @param returnType
   */
  public HiveFuncHolder(
      Class<? extends GenericUDF> genericUdfClazz,
      CompleteType[] argTypes,
      ObjectInspector returnOI,
      CompleteType returnType,
      boolean isRandom) {
    this.genericUdfClazz = genericUdfClazz;
    this.argTypes = argTypes;
    this.returnOI = returnOI;
    this.returnType = returnType;
    this.isRandom = isRandom;
  }

  /**
   * Create holder for UDF
   * @param udfName name of the UDF class
   * @param udfClazz UDF implementation class
   * @param argTypes
   * @param returnOI
   * @param returnType
   */
  public HiveFuncHolder(String udfName, Class< ? extends UDF> udfClazz, CompleteType[] argTypes,
                        ObjectInspector returnOI, CompleteType returnType, boolean isRandom) {
    this(GenericUDFBridge.class, argTypes, returnOI, returnType, isRandom);
    this.isGenericUDF = false;
    this.udfClazz = udfClazz;
    this.udfName = udfName;
  }

  /**
   * UDF return type
   */
  public CompleteType getReturnType() {
    return returnType;
  }

  /**
   * Aggregate function
   */
  public boolean isAggregating() {
    // currently only simple UDFS are supported
    return false;
  }

  /**
   * is the function non-deterministic?
   */
  public boolean isRandom() {
    return isRandom;
  }

  @Override
  public CompleteType getParamType(int i) {
    return argTypes[i];
  }

  @Override
  public int getParamCount() {
    return argTypes.length;
  }

  @Override
  public boolean checkPrecisionRange() {
    throw new UnsupportedOperationException("Hive Functions do not support these.");
  }

  @Override
  public boolean isReturnTypeIndependent() {
    throw new UnsupportedOperationException("Hive Functions do not support these.");
  }

  @Override
  public CompleteType getReturnType(List<LogicalExpression> args) {
    return returnType;
  }

  @Override
  public FunctionTemplate.NullHandling getNullHandling() {
    throw new UnsupportedOperationException("Hive Functions do not support these.");
  }

  /**
   * Start generating code
   * @return workspace variables
   */
  @Override
  public JVar[] renderStart(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, FunctionErrorContext errorContext){
    JVar[] workspaceJVars = new JVar[5];

    workspaceJVars[0] = g.declareClassField("returnOI", g.getModel()._ref(ObjectInspector.class));
    workspaceJVars[1] = g.declareClassField("udfInstance", g.getModel()._ref(GenericUDF.class));
    workspaceJVars[2] = g.declareClassField("deferredObjects", g.getModel()._ref(DeferredObject[].class));
    workspaceJVars[3] = g.declareClassField("arguments", g.getModel()._ref(DeferredObject[].class));
    workspaceJVars[4] = g.declareClassField("returnValueHolder", CodeModelArrowHelper.getHolderType(g.getModel(), returnType.toMinorType(), DataMode.OPTIONAL));

    return workspaceJVars;
  }

  /**
   * Complete code generation
   * @param g
   * @param inputVariables
   * @param workspaceJVars
   * @return HoldingContainer for return value
   */
  @Override
  public HoldingContainer renderEnd(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, JVar[]  workspaceJVars) {
    generateSetup(g, workspaceJVars);
    return generateEval(g, inputVariables, workspaceJVars);
  }

  private JInvocation getUDFInstance(JCodeModel m) {
    if (isGenericUDF) {
      return JExpr._new(m.directClass(genericUdfClazz.getCanonicalName()));
    } else {
      return JExpr._new(m.directClass(GenericUDFBridge.class.getCanonicalName()))
        .arg(JExpr.lit(udfName))
        .arg(JExpr.lit(false))
        .arg(JExpr.lit(udfClazz.getCanonicalName().toString()));
    }
  }

  @Override
  public FunctionHolderExpression getExpr(String name, List<LogicalExpression> args) {
    DEPRECATED_FUNCTION_WARNING_LOGGER.warn("Deprecated Hive function used: {}", name);
    return new HiveFuncHolderExpr(name, this, args);
  }

  private void generateSetup(ClassGenerator<?> g, JVar[] workspaceJVars) {
    JCodeModel m = g.getModel();
    JBlock sub = new JBlock(true, true);

    // declare and instantiate argument ObjectInspector's
    JVar oiArray = sub.decl(
      m._ref(ObjectInspector[].class),
      "argOIs",
      JExpr.newArray(m._ref(ObjectInspector.class), argTypes.length));

    JClass oih = m.directClass(ObjectInspectorHelper.class.getCanonicalName());
    JClass mt = m.directClass(MinorType.class.getCanonicalName());
    JClass mode = m.directClass(DataMode.class.getCanonicalName());
    for(int i=0; i<argTypes.length; i++) {
      sub.assign(
        oiArray.component(JExpr.lit(i)),
        oih.staticInvoke("getObjectInspector")
          .arg(mode.staticInvoke("valueOf").arg(JExpr.lit("OPTIONAL")))
          .arg(mt.staticInvoke("valueOf").arg(JExpr.lit(argTypes[i].toMinorType().name())))
          .arg((((PrimitiveObjectInspector) returnOI).getPrimitiveCategory() ==
              PrimitiveObjectInspector.PrimitiveCategory.STRING) ? JExpr.lit(true) : JExpr.lit(false)));
    }

    // declare and instantiate DeferredObject array
    sub.assign(workspaceJVars[2], JExpr.newArray(m._ref(DeferredObject.class), argTypes.length));

    for(int i=0; i<argTypes.length; i++) {
      sub.assign(
        workspaceJVars[2].component(JExpr.lit(i)),
        JExpr._new(m.directClass(DeferredObject.class.getCanonicalName())));
    }

    // declare empty array for argument deferred objects
    sub.assign(workspaceJVars[3], JExpr.newArray(m._ref(DeferredObject.class), argTypes.length));

    // create new instance of the UDF class
    sub.assign(workspaceJVars[1], getUDFInstance(m));

    // create try..catch block to initialize the UDF instance with argument OIs
    JTryBlock udfInitTry = sub._try();
    udfInitTry.body().assign(
      workspaceJVars[0],
      workspaceJVars[1].invoke("initialize")
      .arg(oiArray));

    JCatchBlock udfInitCatch = udfInitTry._catch(m.directClass(Exception.class.getCanonicalName()));
    JVar exVar = udfInitCatch.param("ex");
    udfInitCatch.body()
      ._throw(JExpr._new(m.directClass(RuntimeException.class.getCanonicalName()))
        .arg(JExpr.lit(String.format("Failed to initialize GenericUDF"))).arg(exVar));

    sub.add(ObjectInspectorHelper.initReturnValueHolder(g, m, workspaceJVars[4], returnOI, returnType.toMinorType()));

    // now add it to the doSetup block in Generated class
    JBlock setup = g.getBlock(ClassGenerator.BlockType.SETUP);
    setup.directStatement(String.format("/** start %s for function %s **/ ",
      ClassGenerator.BlockType.SETUP.name(), genericUdfClazz.getName() + (!isGenericUDF ? "("+udfName+")" : "")));

    setup.add(sub);

    setup.directStatement(String.format("/** end %s for function %s **/ ",
      ClassGenerator.BlockType.SETUP.name(), genericUdfClazz.getName() + (!isGenericUDF ? "("+udfName+")" : "")));
  }

  private HoldingContainer generateEval(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[] workspaceJVars) {

    HoldingContainer out = g.declare(returnType);

    JCodeModel m = g.getModel();
    JBlock sub = new JBlock(true, true);

    // initialize DeferredObject's. For an optional type, assign the value holder only if it is not null
    for(int i=0; i<argTypes.length; i++) {
      sub.assign(workspaceJVars[3].component(JExpr.lit(i)), workspaceJVars[2].component(JExpr.lit(i)));
      JBlock conditionalBlock = new JBlock(false, false);
      JConditional jc = conditionalBlock._if(inputVariables[i].getIsSet().ne(JExpr.lit(0)));
      jc._then().assign(JExpr.ref(workspaceJVars[3].component(JExpr.lit(i)), "valueHolder"), inputVariables[i].getHolder());
      jc._else().assign(JExpr.ref(workspaceJVars[3].component(JExpr.lit(i)), "valueHolder"), JExpr._null());
      sub.add(conditionalBlock);
    }

    // declare generic object for storing return value from GenericUDF.evaluate
    JVar retVal = sub.decl(m._ref(Object.class), "ret");

    // create try..catch block to call the GenericUDF instance with given input
    JTryBlock udfEvalTry = sub._try();
    udfEvalTry.body().assign(retVal,
      workspaceJVars[1].invoke("evaluate").arg(workspaceJVars[3]));

    JCatchBlock udfEvalCatch = udfEvalTry._catch(m.directClass(Exception.class.getCanonicalName()));
    JVar exVar = udfEvalCatch.param("ex");
    udfEvalCatch.body()
      ._throw(JExpr._new(m.directClass(RuntimeException.class.getCanonicalName()))
        .arg(JExpr.lit(String.format("GenericUDF.evaluate method failed"))).arg(exVar));

    // get the ValueHolder from retVal and return ObjectInspector
    sub.add(ObjectInspectorHelper.getObject(m, returnOI, workspaceJVars[0], workspaceJVars[4], retVal));
    sub.assign(out.getHolder(), workspaceJVars[4]);

    // now add it to the doEval block in Generated class
    JBlock setup = g.getBlock(ClassGenerator.BlockType.EVAL);
    setup.directStatement(String.format("/** start %s for function %s **/ ",
      ClassGenerator.BlockType.EVAL.name(), genericUdfClazz.getName() + (!isGenericUDF ? "("+udfName+")" : "")));
    setup.add(sub);
    setup.directStatement(String.format("/** end %s for function %s **/ ",
      ClassGenerator.BlockType.EVAL.name(), genericUdfClazz.getName() + (!isGenericUDF ? "("+udfName+")" : "")));

    return out;
  }
}
