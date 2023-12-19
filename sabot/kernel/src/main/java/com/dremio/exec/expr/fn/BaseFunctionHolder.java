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

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.complex.reader.FieldReader;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.Types;
import com.dremio.exec.compile.sig.SignatureHolder;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.BlockType;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionSyntax;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public abstract class BaseFunctionHolder extends AbstractFunctionHolder {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseFunctionHolder.class);

  private final FunctionAttributes attributes;
  private final OutputDerivation derivation;
  protected final FunctionTemplate.FunctionScope scope;
  protected final FunctionTemplate.NullHandling nullHandling;
  protected final FunctionTemplate.FunctionCostCategory costCategory;
  protected final boolean isBinaryCommutative;
  protected final boolean isDeterministic;
  protected final FunctionSyntax syntax;
  protected final String[] registeredNames;
  protected final WorkspaceReference[] workspaceVars;
  protected final ValueReference[] parameters;
  private final ValueReference returnValue;
  private final FunctionInitializer initializer;
  private final boolean usesErrContext;
  private final ImmutableList<CompleteType> paramTypes;

  public BaseFunctionHolder(
      FunctionAttributes attributes,
      FunctionInitializer initializer) {
    super();
    this.attributes = attributes;
    this.scope = attributes.getScope();
    this.nullHandling = attributes.getNullHandling();
    this.costCategory = attributes.getCostCategory();
    this.isBinaryCommutative = attributes.isBinaryCommutative();
    this.isDeterministic = attributes.isDeterministic();
    this.syntax = attributes.getSyntax();
    this.registeredNames = attributes.getRegisteredNames();
    this.workspaceVars = attributes.getWorkspaceVars();
    this.parameters = attributes.getParameters();
    this.returnValue = attributes.getReturnValue();
    this.derivation = attributes.getDerivation();
    this.initializer = initializer;

    if (parameters == null) {
      this.paramTypes = ImmutableList.of();
    } else {
      this.paramTypes = ImmutableList.copyOf(
        Arrays.stream(parameters)
        .map(ValueReference::getType)
        .collect(ImmutableList.toImmutableList())
      );
    }

    boolean usesErrContext = false;
    for (int i = 0; i < workspaceVars.length; i++) {
      WorkspaceReference ref = workspaceVars[i];
      if (ref.getType() == FunctionErrorContext.class) {
        usesErrContext = true;
      }
    }
    this.usesErrContext = usesErrContext;

    // Make sure functions with INTERNAL null handling use a Nullable output
    assert nullHandling != NullHandling.INTERNAL || returnValue.getOldType().getMode() == TypeProtos.DataMode.OPTIONAL :
      "Function [" + initializer.getClassName() + "] with INTERNAL null handling should use a Nullable output";

    // Make sure functions with Complex output define a proper output derivation
    assert !returnValue.isComplexWriter() || !isReturnTypeIndependent() :
      "Function [" + initializer.getClassName() + "] has a ComplexWriter output but it's using the Default derivation";
  }

  protected String meth(String methodName) {
    return meth(methodName, true);
  }

  protected String meth(String methodName, boolean required) {
    String method = initializer.getMethod(methodName);
    if (method == null) {
      if (!required) {
        return "";
      }
      long count = initializer.getCount();
      Collection<String> methods = initializer.getMethodNames();
      throw UserException
          .functionError()
          .message("Failure while trying use function. No body found for required method %s. Count %d, Methods that were available %s", methodName, count, methods)
          .addContext("FunctionClass", initializer.getClassName())
          .build(logger);
    }
    return method;
  }

  @Override
  public JVar[] renderStart(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, FunctionErrorContext errorContext) {
    return declareWorkspaceVariables(g, errorContext);
  }

  @Override
  public void renderMiddle(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables, JVar[] workspaceJVars) {
  }

  @Override
  public abstract HoldingContainer renderEnd(ClassGenerator<?> g, CompleteType resolvedOutput, HoldingContainer[] inputVariables,
      JVar[] workspaceJVars);

  @Override
  public abstract boolean isNested();

  @Override
  public FunctionHolderExpression getExpr(String name, List<LogicalExpression> args) {
    return new FunctionHolderExpr(name, this, args);
  }

  public boolean isAggregating() {
    return false;
  }

  public boolean isDeterministic() {
    return attributes.isDeterministic();
  }

  public boolean isDynamic() {
    return attributes.isDynamic();
  }

  public FunctionSyntax getSyntax() {
    return syntax;
  }

  protected JVar[] declareWorkspaceVariables(ClassGenerator<?> g, FunctionErrorContext errorContext) {
    assert usesErrContext == (errorContext != null);  // Caller should have created a FunctionErrorContext if usesErrContext()

    JVar[] workspaceJVars = new JVar[workspaceVars.length];
    for (int i = 0; i < workspaceVars.length; i++) {
      WorkspaceReference ref = workspaceVars[i];
      JType jtype = g.getModel()._ref(ref.type);

      if (ScalarReplacementTypes.CLASSES.contains(ref.type)) {
        workspaceJVars[i] = g.declareClassField("work", jtype);
        JBlock b = g.getBlock(SignatureHolder.INIT_METHOD);
        b.assign(workspaceJVars[i], JExpr._new(jtype));
      } else {
        workspaceJVars[i] = g.declareClassField("work", jtype);
      }

      if (ref.isInject()) {
        String injectableFactoryFunction = FunctionContext.INJECTABLE_GETTER_METHODS.get(ref.getType());
        if (injectableFactoryFunction != null) {
          JInvocation injectableFactoryInvocation = JExpr.direct("context").invoke(injectableFactoryFunction);
          if (ref.getType() == FunctionErrorContext.class) {
            injectableFactoryInvocation.arg(JExpr.lit(errorContext.getId()));
            g.getBlock(BlockType.SETUP)
              .assign(workspaceJVars[i], injectableFactoryInvocation);
          } else {
            g.getBlock(BlockType.SETUP)
              .assign(workspaceJVars[i], injectableFactoryInvocation);
          }
        } else {
          // Invalid injectable type provided, this should have been caught in FunctionConverter
          throw new IllegalArgumentException("Invalid injectable type requested in UDF: " + ref.getType().getSimpleName());
        }
      } else {
        //g.getBlock(BlockType.SETUP).assign(workspaceJVars[i], JExpr._new(jtype));
      }
    }
    return workspaceJVars;
  }

  protected void generateBody(ClassGenerator<?> g, BlockType bt, String body, HoldingContainer[] inputVariables,
      JVar[] workspaceJVars, boolean decConstantInputOnly) {
    final String trimmedBody = Strings.nullToEmpty(body).trim();
    if (!trimmedBody.isEmpty() && !"{}".equals(trimmedBody)) {
      JBlock sub = new JBlock(true, true);
      if (decConstantInputOnly) {
        addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, true);
      } else {
        addProtectedBlock(g, sub, body, null, workspaceJVars, false);
      }
      g.getBlock(bt).directStatement(String.format("/** start %s for function %s **/ ", bt.name(), registeredNames[0]));
      g.getBlock(bt).add(sub);
      g.getBlock(bt).directStatement(String.format("/** end %s for function %s **/ ", bt.name(), registeredNames[0]));
    }
  }

  protected void addProtectedBlock(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables,
      JVar[] workspaceJVars, boolean decConstInputOnly) {
    Map<JVar, JVar> inputStartPositionMap = new HashMap<>();
    if (inputVariables != null) {
      for (int i = 0; i < inputVariables.length; i++) {
        if (decConstInputOnly && !inputVariables[i].isConstant()) {
          continue;
        }

        ValueReference parameter = parameters[i];
        HoldingContainer inputVariable = inputVariables[i];
        if (parameter.isFieldReader && !inputVariable.isReader() && inputVariable.getCompleteType().isScalar()) {
          JType singularReaderClass = g.getModel()._ref(TypeHelper.getHolderReaderImpl(getArrowMinorType(inputVariable.getCompleteType().toMinorType())));
          JType fieldReadClass = g.getModel()._ref(FieldReader.class);
          sub.decl(fieldReadClass, parameter.name, JExpr._new(singularReaderClass).arg(inputVariable.getHolder()));
        }else if(parameter.isFieldReader && inputVariable.isReader() && (inputVariable.getCompleteType().isList() ||
          inputVariable.getCompleteType().isMap())) {
          // Need this block to store the start position of the list reader for case when the list reader used for different
          // functions in the same expression and function iterate by each element.
          JVar input = sub.decl(inputVariable.getJType(), parameter.name, inputVariable.getHolder());
          JVar listStartPosition = sub.decl(g.getModel().INT, "listStartPosition" + i,
            input.invoke("getPosition"));
          inputStartPositionMap.put(input, listStartPosition);
        }else {
          sub.decl(inputVariable.getJType(), parameter.name, inputVariable.getHolder());
        }
      }
    }

    JVar[] internalVars = new JVar[workspaceJVars.length];
    for (int i = 0; i < workspaceJVars.length; i++) {
      if (decConstInputOnly) {
        internalVars[i] = sub.decl(g.getModel()._ref(workspaceVars[i].type), workspaceVars[i].name, workspaceJVars[i]);
      } else {
        internalVars[i] = sub.decl(g.getModel()._ref(workspaceVars[i].type), workspaceVars[i].name, workspaceJVars[i]);
      }

    }

    Preconditions.checkNotNull(body);
    sub.directStatement(body);

    if(!inputStartPositionMap.isEmpty()) {
      for (Map.Entry<JVar, JVar> entry : inputStartPositionMap.entrySet()) {
        sub.add(entry.getKey().invoke("reset"));
        sub.add(entry.getKey().invoke("setPosition").arg(entry.getValue()));
      }
    }
    // reassign workspace variables back to global space.
    for (int i = 0; i < workspaceJVars.length; i++) {
      sub.assign(workspaceJVars[i], internalVars[i]);
    }
  }

  public boolean matches(CompleteType returnType, List<CompleteType> argTypes) {

    if (!returnType.equals(returnValue.type)) {
      return false;
    }

    if (argTypes.size() != parameters.length) {
      return false;
    }

    for (int i = 0; i < parameters.length; i++) {
      if(!parameters[i].type.equals(argTypes.get(i))){
        return false;
      }
    }

    return true;
  }

  @Override
  public CompleteType getParamType(int i) {
    return this.parameters[i].type;
  }

  @Override
  public int getParamCount() {
    return this.parameters.length;
  }

  @Override
  public boolean usesErrContext() {
    return usesErrContext;
  }

  public boolean isConstant(int i) {
    return this.parameters[i].isConstant;
  }

  @Override
  public boolean isFieldReader(int i) {
    return this.parameters[i].isFieldReader;
  }

  @Override
  public CompleteType getReturnType(final List<LogicalExpression> args) {
    return derivation.getOutputType(returnValue.type, args);
  }

  public String getReturnName(){
    return returnValue.name;
  }

  @Override
  public NullHandling getNullHandling() {
    return attributes.getNullHandling();
  }

  public String[] getRegisteredNames() {
    return attributes.getRegisteredNames();
  }

  public int getCostCategory() {
    return attributes.getCostCategory().getValue();
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return this.getClass().getSimpleName()
        + " [functionNames=" + Arrays.toString(registeredNames)
        + ", returnType=" + returnValue.type
        + ", nullHandling=" + nullHandling
        + ", parameters=" + (parameters != null ? Arrays.asList(parameters).subList(0, Math.min(parameters.length, maxLen)) : null) + "]";
  }

  public WorkspaceReference[] getWorkspaceVars() {
    return this.workspaceVars;
  }

  public ValueReference[] getParameters() {
    return this.parameters;
  }

  public static class ValueReference {
    private MajorType oldType; // used for interpreter
    private CompleteType type;
    private String name;
    private boolean isConstant = false;
    private boolean isFieldReader = false;
    private boolean isComplexWriter = false;


    public ValueReference(CompleteType type, MajorType oldType, String name) {
      super();
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(name);
      this.type = type;
      this.oldType = oldType;
      this.name = name;
    }

    public CompleteType getType() {
      return type;
    }

    @Deprecated
    public MajorType getOldType() {
      return oldType;
    }

    public String getName() {
      return name;
    }

    public void setConstant(boolean isConstant) {
      this.isConstant = isConstant;
    }

    public boolean isConstant(){
      return isConstant;
    }

    @Override
    public String toString() {
      return "ValueReference [type=" + type + ", name=" + name + "]";
    }

    public static ValueReference createFieldReaderRef(String name) {
      ValueReference ref = new ValueReference(CompleteType.LATE, Types.LATE_BIND_TYPE, name);
      ref.isFieldReader = true;

      return ref;
    }

    public static ValueReference createComplexWriterRef(String name) {
      ValueReference ref = new ValueReference(CompleteType.LATE, Types.LATE_BIND_TYPE, name);
      ref.isComplexWriter = true;
      return ref;
    }

    public boolean isComplexWriter() {
      return isComplexWriter;
    }

  }

  public static class WorkspaceReference {
    Class<?> type;
    String name;
    CompleteType completeType;
    boolean inject;

    public WorkspaceReference(Class<?> type, String name, boolean inject) {
      super();
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(name);
      this.type = type;
      this.name = name;
      this.inject = inject;
    }

    void setCompleteType(CompleteType completeType) {
      this.completeType = completeType;

      // overwrite the holder to use the nullable version.
      this.type = completeType.getHolderClass();
    }

    public boolean isInject() {
      return inject;
    }

    public Class<?> getType() {
      return type;
    }

    public String getName() {
      return name;
    }
  }

  @Override
  public boolean checkPrecisionRange() {
    return false;
  }

  /**
   * Does this function always return the same type, no matter the inputs?
   */
  @Override
  public boolean isReturnTypeIndependent(){
    return derivation.getClass() == OutputDerivation.Default.class;
  }

  public ValueReference getReturnValue() {
    return returnValue;
  }

  public List<CompleteType> getParamTypes(){
    return paramTypes;
  }
}
