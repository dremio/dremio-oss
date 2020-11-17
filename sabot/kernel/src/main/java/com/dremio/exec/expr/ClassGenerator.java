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
package com.dremio.exec.expr;

import static com.dremio.exec.compile.sig.GeneratorMapping.GM;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dremio.common.expression.CodeModelArrowHelper;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.compile.sig.CodeGeneratorArgument;
import com.dremio.exec.compile.sig.CodeGeneratorMethod;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.compile.sig.SignatureHolder;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.fn.BaseFunctionHolder.WorkspaceReference;
import com.dremio.exec.record.TypedFieldId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JLabel;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public class ClassGenerator<T>{

  public static final GeneratorMapping DEFAULT_SCALAR_MAP = GM("doSetup", "doEval", null, null);
  public static final GeneratorMapping DEFAULT_CONSTANT_MAP = GM("doSetup", "doSetup", null, null);

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassGenerator.class);
  public static enum BlockType {SETUP, EVAL, RESET, CLEANUP}

  private static final int MAX_EXPRESSIONS_IN_FUNCTION = 50;

  private final SignatureHolder sig;
  private final EvaluationVisitor evaluationVisitor;
  private final Map<ValueVectorSetup, JVar> vvDeclaration = Maps.newHashMap();
  private final Map<String, ClassGenerator<T>> innerClasses = Maps.newHashMap();
  private final List<TypedFieldId> workspaceTypes = Lists.newArrayList();
  private final Map<WorkspaceReference, JVar> workspaceVectors = Maps.newHashMap();
  private final CodeGenerator<T> codeGenerator;

  public final JDefinedClass clazz;
  private final LinkedList<SizedJBlock>[] blocks;
  private final JCodeModel model;

  private int index = 0;
  private int labelIndex = 0;
  private MappingSet mappings;

  public static MappingSet getDefaultMapping() {
    return new MappingSet("inIndex", "outIndex", DEFAULT_CONSTANT_MAP, DEFAULT_SCALAR_MAP);
  }

  @SuppressWarnings("unchecked")
  ClassGenerator(CodeGenerator<T> codeGenerator, MappingSet mappingSet, SignatureHolder signature, EvaluationVisitor eval, JDefinedClass clazz, JCodeModel model) throws JClassAlreadyExistsException {
    this.codeGenerator = codeGenerator;
    this.clazz = clazz;
    this.mappings = mappingSet;
    this.sig = signature;
    this.evaluationVisitor = eval;
    this.model = Preconditions.checkNotNull(model, "Code model object cannot be null.");
    blocks = new LinkedList[sig.size()];

    for (int i =0; i < sig.size(); i++) {
      blocks[i] = Lists.newLinkedList();
    }
    rotateBlock();

    for (SignatureHolder child : signature.getChildHolders()) {
      final String innerClassName = child.getSignatureClass().getSimpleName();
      final JDefinedClass innerClazz;
      // we need to extend the template class and avoid using static inner classes.
      innerClazz = clazz._class(JMod.FINAL, innerClassName)._extends(child.getSignatureClass());

      // we also need to delegate any inner class constructors.
      for(Constructor<?> c : child.getSignatureClass().getDeclaredConstructors()){
        final Class<?>[] params = c.getParameterTypes();
        JMethod constructor = innerClazz.constructor(JMod.PUBLIC);
        JBlock block = constructor.body();
        JInvocation invoke = block.invoke("super");
        block.invoke(SignatureHolder.INIT_METHOD);

        // start at 1 since first parameter is the parent class
        for (int i = 1; i < params.length; i++) {
          constructor.param(params[i], "arg" + i);
          invoke.arg(JExpr.direct("arg" + i));
        }
      }

      innerClasses.put(innerClassName, new ClassGenerator<>(codeGenerator, mappingSet, child, eval, innerClazz, model));
    }
  }

  public ClassGenerator<T> getInnerGenerator(String name) {
    ClassGenerator<T> inner = innerClasses.get(name);
    Preconditions.checkNotNull(inner);
    return inner;
  }

  public MappingSet getMappingSet() {
    return mappings;
  }

  public void setMappingSet(MappingSet mappings) {
    this.mappings = mappings;
  }

  public CodeGenerator<T> getCodeGenerator() {
    return codeGenerator;
  }

  private GeneratorMapping getCurrentMapping() {
    return mappings.getCurrentMapping();
  }

  public JBlock getBlock(String methodName) {
    JBlock blk = this.blocks[sig.get(methodName)].getLast().getBlock();
    Preconditions.checkNotNull(blk, "Requested method name of %s was not available for signature %s.",  methodName, this.sig);
    return blk;
  }

  public JBlock getBlock(BlockType type) {
    return getBlock(getCurrentMapping().getMethodName(type));
  }

  public JBlock getSetupBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.SETUP));
  }
  public JBlock getEvalBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.EVAL));
  }
  public JBlock getResetBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.RESET));
  }
  public JBlock getCleanupBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.CLEANUP));
  }

  private int innerMethodCount = 0;

  public JMethod innerMethod(CompleteType type) {
    JMethod method = clazz.method(JMod.PRIVATE, type.getHolderClass(), "inner_method_" + innerMethodCount++);
    String methodName = getCurrentMapping().getMethodName(BlockType.EVAL);
    CodeGeneratorMethod cgm = sig.get(sig.get(methodName));
    for (CodeGeneratorArgument arg : cgm) {
      method.param(arg.getType(), arg.getName());
    }
    nestEvalBlock(method.body());
    evaluationVisitor.previousExpressions.clear();
    return method;
  }

  public JMethod nestSetupMethod() {
    JMethod method = clazz.method(JMod.PRIVATE, void.class, "inner_setup_method_" + innerMethodCount++);
    String methodName = getCurrentMapping().getMethodName(BlockType.SETUP);
    CodeGeneratorMethod cgm = sig.get(sig.get(methodName));
    for (CodeGeneratorArgument arg : cgm) {
      method.param(arg.getType(), arg.getName());
    }
    nestSetupBlock(method.body());
    return method;
  }

  public JInvocation invokeInnerMethod(JMethod method, BlockType blockType) {
    JInvocation invocation = JExpr.invoke(method);
    String methodName = getCurrentMapping().getMethodName(blockType);
    CodeGeneratorMethod cgm = sig.get(sig.get(methodName));
    for (CodeGeneratorArgument arg : cgm) {
      invocation.arg(JExpr.ref(arg.getName()));
    }
    return invocation;
  }

  public void nestEvalBlock(JBlock block) {
    String methodName = getCurrentMapping().getMethodName(BlockType.EVAL);
    evaluationVisitor.newScope();
    this.blocks[sig.get(methodName)].addLast(new SizedJBlock(block));
  }

  public void unNestEvalBlock() {
    String methodName = getCurrentMapping().getMethodName(BlockType.EVAL);
    evaluationVisitor.leaveScope();
    this.blocks[sig.get(methodName)].removeLast();
  }

  public void nestSetupBlock(JBlock block) {
    String methodName = getCurrentMapping().getMethodName(BlockType.SETUP);
    this.blocks[sig.get(methodName)].addLast(new SizedJBlock(block));
  }

  public void unNestSetupBlock() {
    String methodName = getCurrentMapping().getMethodName(BlockType.SETUP);
    this.blocks[sig.get(methodName)].removeLast();
  }

  public JLabel getEvalBlockLabel (String prefix) {
    return getEvalBlock().label(prefix + labelIndex ++);
  }

  /**
   * Creates an inner braced and indented block
   * @param type type of the created block
   * @return a newly created inner block
   */
  private JBlock createInnerBlock(BlockType type) {
    final JBlock currBlock = getBlock(type);
    final JBlock innerBlock = new JBlock();
    currBlock.add(innerBlock);
    return innerBlock;
  }

  /**
   * Creates an inner braced and indented block for evaluation of the expression.
   * @return a newly created inner eval block
   */
  protected JBlock createInnerEvalBlock() {
    return createInnerBlock(BlockType.EVAL);
  }

  public JVar declareVectorValueSetupAndMember(String batchName, TypedFieldId fieldId) {
    return declareVectorValueSetupAndMember( DirectExpression.direct(batchName), fieldId);
  }

  public JVar declareVectorValueSetupAndMember(DirectExpression batchName, TypedFieldId fieldId) {
    final ValueVectorSetup setup = new ValueVectorSetup(batchName, fieldId);

    final Class<?> valueVectorClass = fieldId.getIntermediateClass();
    final JClass vvClass = model.ref(valueVectorClass);
    final JClass retClass = fieldId.isHyperReader() ? vvClass.array() : vvClass;

    final JVar vv = declareClassField("vv", retClass);
    final JBlock b = getSetupBlock();
    int[] fieldIndices = fieldId.getFieldIds();
    JInvocation invoke = model.ref(VectorResolver.class).staticInvoke(fieldId.isHyperReader() ? "hyper" : "simple")
        .arg(batchName)
        .arg(vvClass.dotclass());

    for(int i = 0; i < fieldIndices.length; i++){
      invoke.arg(JExpr.lit(fieldIndices[i]));
    }

    // we have to cast here since Janino doesn't handle generic inference well.
    JExpression casted = JExpr.cast(retClass, invoke);
    b.assign(vv, casted);
    vvDeclaration.put(setup, vv);

    return vv;
  }

  public enum BlockCreateMode {
    NEW_BLOCK,  // Create new block
    MERGE, // Do not create block; put into existing block.
    NEW_IF_TOO_LARGE // Create new block only if # of expressions added hit upper-bound MAX_EXPRESSIONS_IN_FUNCTION
  }

  public HoldingContainer addExpr(LogicalExpression ex) {
    return addExpr(ex, false);
  }

  public HoldingContainer addExpr(LogicalExpression ex, boolean allowInnerMethods) {
    // default behavior is always to put expression into new block.
    return addExpr(ex, BlockCreateMode.NEW_BLOCK, allowInnerMethods);
  }

  public HoldingContainer addExpr(LogicalExpression ex, BlockCreateMode mode) {
    return addExpr(ex, mode, false);
  }

  public HoldingContainer addExpr(LogicalExpression ex, BlockCreateMode mode, boolean allowInnerMethods) {
    if (mode == BlockCreateMode.NEW_BLOCK || mode == BlockCreateMode.NEW_IF_TOO_LARGE) {
      rotateBlock(mode);
    }

    for (LinkedList<SizedJBlock> b : blocks) {
      b.getLast().incCounter();
    }

    return evaluationVisitor.addExpr(ex, this, allowInnerMethods);
  }

  public void rotateBlock() {
    // default behavior is always to create new block.
    rotateBlock(BlockCreateMode.NEW_BLOCK);
  }

  private void rotateBlock(BlockCreateMode mode) {
    boolean blockRotated = false;
    for (LinkedList<SizedJBlock> b : blocks) {
      if (mode == BlockCreateMode.NEW_BLOCK ||
          (mode == BlockCreateMode.NEW_IF_TOO_LARGE &&
            b.getLast().getCount() > MAX_EXPRESSIONS_IN_FUNCTION)) {
        b.add(new SizedJBlock(new JBlock(true, true)));
        blockRotated = true;
      }
    }
    if (blockRotated) {
      evaluationVisitor.previousExpressions.clear();
    }
  }

  void flushCode() {
    int i = 0;
    for(CodeGeneratorMethod method : sig) {
      JMethod outer = clazz.method(JMod.PUBLIC, model._ref(method.getReturnType()), method.getMethodName());
      for(CodeGeneratorArgument arg : method) {
        outer.param(arg.getType(), arg.getName());
      }
      for(Class<?> c : method.getThrowsIterable()) {
        outer._throws(model.ref(c));
      }
      outer._throws(SchemaChangeException.class);

      int methodIndex = 0;
      int exprsInMethod = 0;
      boolean isVoidMethod = method.getReturnType() == void.class;
      for(SizedJBlock sb : blocks[i++]) {
        JBlock b = sb.getBlock();
        if(!b.isEmpty()) {
          if (exprsInMethod > MAX_EXPRESSIONS_IN_FUNCTION) {
            JMethod inner = clazz.method(JMod.PUBLIC, model._ref(method.getReturnType()), method.getMethodName() + methodIndex);
            JInvocation methodCall = JExpr.invoke(inner);
            for (CodeGeneratorArgument arg : method) {
              inner.param(arg.getType(), arg.getName());
              methodCall.arg(JExpr.direct(arg.getName()));
            }
            for (Class<?> c : method.getThrowsIterable()) {
              inner._throws(model.ref(c));
            }
            inner._throws(SchemaChangeException.class);

            if (isVoidMethod) {
              outer.body().add(methodCall);
            } else {
              outer.body()._return(methodCall);
            }
            outer = inner;
            exprsInMethod = 0;
            ++methodIndex;
          }
          outer.body().add(b);
          exprsInMethod += sb.getCount();
        }
      }
    }

    for(ClassGenerator<T> child : innerClasses.values()) {
      child.flushCode();
    }
  }

  public JCodeModel getModel() {
    return model;
  }

  public String getNextVar() {
    return "v" + index++;
  }

  public String getNextVar(String prefix) {
    return prefix + index++;
  }

  public JVar declareClassField(String prefix, JType t) {
    return clazz.field(JMod.NONE, t, prefix + index++);
  }

  public JVar declareClassField(String prefix, JType t, JExpression init) {
    return clazz.field(JMod.NONE, t, prefix + index++, init);
  }

  public HoldingContainer declare(CompleteType t) {
    return declare(t, true);
  }

  public HoldingContainer declare(CompleteType t, boolean includeNewInstance) {
    JType holderType = CodeModelArrowHelper.getHolderType(t, model);
    JVar var;
    if (includeNewInstance) {
      var = getEvalBlock().decl(holderType, "out" + index, JExpr._new(holderType));
    } else {
      var = getEvalBlock().decl(holderType, "out" + index);
    }
    index++;
    return new HoldingContainer(t, var, var.ref("value"), var.ref("isSet"));
  }

  public List<TypedFieldId> getWorkspaceTypes() {
    return this.workspaceTypes;
  }

  public Map<WorkspaceReference, JVar> getWorkspaceVectors() {
    return this.workspaceVectors;
  }

  private static class ValueVectorSetup{
    final DirectExpression batch;
    final TypedFieldId fieldId;

    public ValueVectorSetup(DirectExpression batch, TypedFieldId fieldId) {
      super();
      this.batch = batch;
      this.fieldId = fieldId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((batch == null) ? 0 : batch.hashCode());
      result = prime * result + ((fieldId == null) ? 0 : fieldId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ValueVectorSetup other = (ValueVectorSetup) obj;
      if (batch == null) {
        if (other.batch != null) {
          return false;
        }
      } else if (!batch.equals(other.batch)) {
        return false;
      }
      if (fieldId == null) {
        if (other.fieldId != null) {
          return false;
        }
      } else if (!fieldId.equals(other.fieldId)) {
        return false;
      }
      return true;
    }

  }

  public static class HoldingContainer{
    private final JVar holder;
    private final JFieldRef value;
    private final JFieldRef isSet;
    private final CompleteType type;
    private boolean isConstant;
    private final boolean singularRepeated;
    private final boolean isReader;

    public HoldingContainer(CompleteType t, JVar holder, JFieldRef value, JFieldRef isSet) {
      this(t, holder, value, isSet, false, false);
    }

    public HoldingContainer(CompleteType t, JVar holder, JFieldRef value, JFieldRef isSet, boolean singularRepeated, boolean isReader) {
      this.holder = holder;
      this.value = value;
      this.isSet = isSet;
      this.type = t;
      this.isConstant = false;
      this.singularRepeated = singularRepeated;
      this.isReader = isReader;
    }

    public boolean isReader() {
      return this.isReader;
    }

    public boolean isSingularRepeated() {
      return singularRepeated;
    }

    public HoldingContainer setConstant(boolean isConstant) {
      this.isConstant = isConstant;
      return this;
    }

    public JFieldRef f(String name) {
      return holder.ref(name);
    }

    public boolean isConstant() {
      return this.isConstant;
    }

    public JVar getHolder() {
      return holder;
    }

    public JFieldRef getValue() {
      return value;
    }

    public CompleteType getCompleteType() {
      return type;
    }

    public JFieldRef getIsSet() {
      Preconditions.checkNotNull(isSet, "You cannot access the isSet variable when operating on a non-nullable output value.");
      return isSet;
    }

  }

}
