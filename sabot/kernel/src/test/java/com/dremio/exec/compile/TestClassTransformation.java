/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.compile;

import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.compile.ClassTransformer.ClassSet;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.OptionValue.OptionType;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.rpc.user.UserSession;

public class TestClassTransformation extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestClassTransformation.class);

  private static final int ITERATION_COUNT = Integer.valueOf(System.getProperty("TestClassTransformation.iteration", "1"));

  private static SessionOptionManager sessionOptions;

  @BeforeClass
  public static void beforeTestClassTransformation() throws Exception {
    final UserSession userSession = UserSession.Builder.newBuilder()
      .withOptionManager(getSabotContext().getOptionManager())
      .build();
    sessionOptions = (SessionOptionManager) userSession.getOptions();
  }

  @Test
  public void testJaninoClassCompiler() throws Exception {
    logger.debug("Testing JaninoClassCompiler");
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, ClassCompilerSelector.JAVA_COMPILER_OPTION, ClassCompilerSelector.CompilerPolicy.JANINO.name()));
    ClassCompilerSelector classCompilerSelector = new ClassCompilerSelector(DEFAULT_SABOT_CONFIG, sessionOptions);
    QueryClassLoader loader = new QueryClassLoader(classCompilerSelector);
    for (int i = 0; i < ITERATION_COUNT; i++) {
      compilationInnerClass(loader);
    }
    loader.close();
  }

  @Test
  public void testJDKClassCompiler() throws Exception {
    logger.debug("Testing JDKClassCompiler");
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, ClassCompilerSelector.JAVA_COMPILER_OPTION, ClassCompilerSelector.CompilerPolicy.JDK.name()));
    ClassCompilerSelector classCompilerSelector = new ClassCompilerSelector(DEFAULT_SABOT_CONFIG, sessionOptions);
    QueryClassLoader loader = new QueryClassLoader(classCompilerSelector);
    for (int i = 0; i < ITERATION_COUNT; i++) {
      compilationInnerClass(loader);
    }
    loader.close();
  }

  @Test
  public void testCompilationNoDebug() throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
    CodeGenerator<ExampleInner> cg = newCodeGenerator(ExampleInner.class, ExampleTemplateWithInner.class);
    ClassSet classSet = new ClassSet(null, cg.getDefinition().getTemplateClassName(), cg.getMaterializedClassName());
    String sourceCode = cg.generateAndGet();
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, ClassCompilerSelector.JAVA_COMPILER_OPTION, ClassCompilerSelector.CompilerPolicy.JDK.name()));

    sessionOptions.setOption(OptionValue.createBoolean(OptionType.SESSION, ClassCompilerSelector.JAVA_COMPILER_DEBUG_OPTION, false));
    ClassCompilerSelector classCompilerSelector = new ClassCompilerSelector(DEFAULT_SABOT_CONFIG, sessionOptions);
    QueryClassLoader loader = new QueryClassLoader(classCompilerSelector);
    final ClassBytes[] codeWithoutDebug = loader.getClassByteCode(classSet.generated, sourceCode);
    loader.close();
    int sizeWithoutDebug = 0;
    for (ClassBytes bs : codeWithoutDebug) {
      sizeWithoutDebug += bs.getBytes().length;
    }

    sessionOptions.setOption(OptionValue.createBoolean(OptionType.SESSION, ClassCompilerSelector.JAVA_COMPILER_DEBUG_OPTION, true));
    classCompilerSelector = new ClassCompilerSelector(DEFAULT_SABOT_CONFIG, sessionOptions);
    loader = new QueryClassLoader(classCompilerSelector);
    final ClassBytes[] codeWithDebug = loader.getClassByteCode(classSet.generated, sourceCode);
    loader.close();
    int sizeWithDebug = 0;
    for(ClassBytes bs : codeWithDebug) {
      sizeWithDebug += bs.getBytes().length;
    }

    Assert.assertTrue("Debug code is smaller than optimized code!!!", sizeWithDebug > sizeWithoutDebug);
    logger.debug("Optimized code is {}% smaller than debug code.", (int)((sizeWithDebug - sizeWithoutDebug)/(double)sizeWithDebug*100));
  }

  /**
   * Do a test of a three level class to ensure that nested code generators works correctly.
   * @throws Exception
   */
  private void compilationInnerClass(QueryClassLoader loader) throws Exception{
    CodeGenerator<ExampleInner> cg = newCodeGenerator(ExampleInner.class, ExampleTemplateWithInner.class);

    ClassTransformer ct = new ClassTransformer(sessionOptions);
    Class<? extends ExampleInner> c = (Class<? extends ExampleInner>) ct.getImplementationClass(loader, cg.getDefinition(), cg.generateAndGet(), cg.getMaterializedClassName());
    ExampleInner t = c.newInstance();
    t.doOutside();
    t.doInsideOutside();
  }

  private <T, X extends T> CodeGenerator<T> newCodeGenerator(Class<T> iface, Class<X> impl) {
    FunctionContext mockFunctionContext = mock(FunctionContext.class);

    final TemplateClassDefinition<T> template = new TemplateClassDefinition<>(iface, impl);
    CodeGenerator<T> cg = CodeGenerator.get(template, getSabotContext().getCompiler(), mockFunctionContext);

    ClassGenerator<T> root = cg.getRoot();
    root.setMappingSet(new MappingSet(new GeneratorMapping("doOutside", null, null, null)));
    root.getSetupBlock().directStatement("System.out.println(\"outside\");");


    ClassGenerator<T> inner = root.getInnerGenerator("TheInnerClass");
    inner.setMappingSet(new MappingSet(new GeneratorMapping("doInside", null, null, null)));
    inner.getSetupBlock().directStatement("System.out.println(\"inside\");");

    ClassGenerator<T> doubleInner = inner.getInnerGenerator("DoubleInner");
    doubleInner.setMappingSet(new MappingSet(new GeneratorMapping("doDouble", null, null, null)));
    doubleInner.getSetupBlock().directStatement("System.out.println(\"double\");");
    return cg;
  }
}
