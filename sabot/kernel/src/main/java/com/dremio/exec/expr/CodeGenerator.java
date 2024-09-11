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

import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.compile.sig.SignatureHolder;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Preconditions;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JMod;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A code generator is responsible for generating the Java source code required to complete the
 * implementation of an abstract template. It is used with a class transformer to merge precompiled
 * template code with runtime generated and compiled query specific code to create a runtime
 * instance.
 *
 * <p>A code generator can contain one or more ClassGenerators that implement outer and inner
 * classes associated with a particular runtime generated instance.
 *
 * @param <T> The interface that results from compiling and merging the runtime code that is
 *     generated.
 */
public class CodeGenerator<T> {
  /**
   * Interface that uniquely identified as a code generation. Can be used as key.
   *
   * @param <T> Type of class being generated
   */
  public interface CodeDefinition<T> {
    TemplateClassDefinition<T> getDefinition();

    String getGeneratedCode();

    String getMaterializedClassName();
  }

  private static final String PACKAGE_NAME = "com.dremio.s";

  private static final String SIMPLE_TYPES =
      "(BigInt|Int|Float4|Float8|VarBinary|VarChar|TimeStampMilli|DateMilli|TimeMilli|Decimal)";
  private static final String HOLDER_STRING = Pattern.quote("Holder()");
  private static final String REPLACE_SINGLE_TYPE_REGEX =
      "new " + SIMPLE_TYPES + "(" + HOLDER_STRING + ")";
  private static final Pattern REPLACE_SINGLE_TYPE_TO_NULLABLE =
      Pattern.compile(REPLACE_SINGLE_TYPE_REGEX);
  private static final String REPLACE_PATTERN = "new Nullable$1$2";

  private final DefaultCodeDefinition<T> codeDefinition;
  private final CodeCompiler compiler;

  private final FunctionContext functionContext;
  private JCodeModel model;
  private ClassGenerator<T> rootGenerator;

  CodeGenerator(
      CodeCompiler compiler,
      TemplateClassDefinition<T> definition,
      FunctionContext functionContext) {
    this(compiler, ClassGenerator.getDefaultMapping(), definition, functionContext);
  }

  CodeGenerator(
      CodeCompiler compiler,
      MappingSet mappingSet,
      TemplateClassDefinition<T> definition,
      FunctionContext functionContext) {
    Preconditions.checkNotNull(
        definition.getSignature(),
        "The signature for defintion %s was incorrectly initialized.",
        definition);
    this.codeDefinition = new DefaultCodeDefinition<>(definition);
    this.compiler = compiler;
    try {
      this.model = new JCodeModel();
      JDefinedClass clazz = model._package(PACKAGE_NAME)._class("GenericGenerated");
      clazz = clazz._extends(model.directClass(definition.getTemplateClassName()));
      clazz.constructor(JMod.PUBLIC).body().invoke(SignatureHolder.INIT_METHOD);
      rootGenerator =
          new ClassGenerator<>(
              this,
              mappingSet,
              definition.getSignature(),
              new EvaluationVisitor(functionContext),
              clazz,
              model,
              functionContext);
      this.functionContext = functionContext;
    } catch (JClassAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }

  public ClassGenerator<T> getRoot() {
    return rootGenerator;
  }

  public void generate() throws IOException {
    Preconditions.checkNotNull(model, "model can not be null");
    Preconditions.checkNotNull(rootGenerator, "rootGenerator can not be null");

    rootGenerator.flushCode();

    SingleClassStringWriter w = new SingleClassStringWriter();
    model.build(w);
    // Free up unused Space early : model, rootGenerator.
    model = null;
    rootGenerator = null;

    this.codeDefinition.setGenerifiedCode(
        REPLACE_SINGLE_TYPE_TO_NULLABLE
            .matcher(w.getCode().toString()) // hack for single type variables.
            .replaceAll(REPLACE_PATTERN));
  }

  public CodeDefinition<T> getCodeDefinition() {
    return codeDefinition;
  }

  public static <T> CodeGenerator<T> get(
      TemplateClassDefinition<T> definition,
      CodeCompiler compiler,
      FunctionContext functionContext) {
    return new CodeGenerator<>(compiler, definition, functionContext);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CodeGenerator<?> that = (CodeGenerator<?>) o;
    return codeDefinition.equals(that.codeDefinition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(codeDefinition);
  }

  public T getImplementationClass() {
    return compiler.getImplementationClass(this);
  }

  public List<T> getImplementationClass(final int instanceCount) {
    return compiler.getImplementationClass(this, instanceCount);
  }

  public FunctionContext getFunctionContext() {
    return functionContext;
  }

  private static final class DefaultCodeDefinition<T> implements CodeDefinition<T> {
    private final TemplateClassDefinition<T> definition;
    private final String className;
    private final String fqcn;
    private String generifiedCode;

    private DefaultCodeDefinition(TemplateClassDefinition<T> definition) {
      this.definition = definition;
      this.className =
          definition.getExternalInterface().getSimpleName()
              + "Gen"
              + definition.getNextClassNumber();
      this.fqcn = PACKAGE_NAME + "." + className;
      this.generifiedCode = "";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DefaultCodeDefinition<?> that = (DefaultCodeDefinition<?>) o;
      return Objects.equals(definition, that.definition)
          && Objects.equals(generifiedCode, that.generifiedCode);
    }

    @Override
    public int hashCode() {
      return Objects.hash(definition, generifiedCode);
    }

    private void setGenerifiedCode(String generifiedCode) {
      this.generifiedCode = generifiedCode;
    }

    @Override
    public TemplateClassDefinition<T> getDefinition() {
      return definition;
    }

    @Override
    public String getGeneratedCode() {
      return generifiedCode.replaceAll("GenericGenerated", this.className);
    }

    @Override
    public String getMaterializedClassName() {
      return fqcn;
    }
  }
}
