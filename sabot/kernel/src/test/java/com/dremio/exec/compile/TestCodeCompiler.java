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
package com.dremio.exec.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.CompilationOptions;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.rpc.user.UserSession;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCodeCompiler extends BaseTestQuery {

  private static CodeCompiler codeCompiler;
  private static OptionManager sessionOptions;

  @BeforeClass
  public static void beforeTestClassTransformation() {
    final UserSession userSession =
        UserSession.Builder.newBuilder()
            .withSessionOptionManager(
                new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
                getSabotContext().getOptionManager())
            .build();
    sessionOptions = userSession.getOptions();
    codeCompiler = new CodeCompiler(DEFAULT_SABOT_CONFIG, sessionOptions);
  }

  // Check that class was loaded from LocalCache
  @Test
  public void checkClassWasLoadedFromCache() {
    final TemplateClassDefinition<ExampleInner> template =
        new TemplateClassDefinition<>(ExampleInner.class, ExampleTemplateWithInner.class);

    CodeGenerator<ExampleInner> cg = newCodeGenerator(template, false);
    ExampleTemplateWithInner clazz1 = codeCompiler.getImplementationClass(cg);
    cg = newCodeGenerator(template, false);
    ExampleTemplateWithInner clazz2 = codeCompiler.getImplementationClass(cg);
    assertEquals(clazz1.getClass(), clazz2.getClass());
  }

  // Check that new class file was created
  @Test
  public void checkClassWasCreatedNew() {
    final TemplateClassDefinition<ExampleInner> template =
        new TemplateClassDefinition<>(ExampleInner.class, ExampleTemplateWithInner.class);

    CodeGenerator<ExampleInner> cg = newCodeGenerator(template, true);
    ExampleTemplateWithInner clazz1 = codeCompiler.getImplementationClass(cg);
    cg = newCodeGenerator(template, false);
    ExampleTemplateWithInner clazz2 = codeCompiler.getImplementationClass(cg);
    assertNotEquals(clazz1.getClass(), clazz2.getClass());
  }

  private <T, X extends T> CodeGenerator<T> newCodeGenerator(
      TemplateClassDefinition<T> template, boolean withInner) {
    CompilationOptions compilationOptions = mock(CompilationOptions.class);
    when(compilationOptions.getNewMethodThreshold()).thenReturn(100);
    FunctionContext mockFunctionContext = mock(FunctionContext.class);
    when(mockFunctionContext.getCompilationOptions()).thenReturn(compilationOptions);

    CodeGenerator<T> cg =
        CodeGenerator.get(template, getSabotContext().getCompiler(), mockFunctionContext);

    ClassGenerator<T> root = cg.getRoot();
    root.setMappingSet(new MappingSet(new GeneratorMapping("doOutside", null, null, null)));
    root.getSetupBlock().directStatement("System.out.println(\"outside\");");
    if (withInner) {
      ClassGenerator<T> inner = root.getInnerGenerator("TheInnerClass");
      inner.setMappingSet(new MappingSet(new GeneratorMapping("doInside", null, null, null)));
      inner.getSetupBlock().directStatement("System.out.println(\"inside\");");

      ClassGenerator<T> doubleInner = inner.getInnerGenerator("DoubleInner");
      doubleInner.setMappingSet(new MappingSet(new GeneratorMapping("doDouble", null, null, null)));
      doubleInner.getSetupBlock().directStatement("System.out.println(\"double\");");
    }
    return cg;
  }
}
