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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * These tests are temporarily kept to compare performance of different Java code constructs.
 * TODO(ramesh) : Remove these tests in the future, once we finalize the code construct for case
 */
public class TestCodeGen {
  // When running perf tests increase these MAX numbers
  private static final int MAX_NESTED_IF_ELSE = 2;
  private static final int MAX_FLAT_IF_ELSE = 2;
  private static final int MAX_CASE_STATEMENTS = 2;
  private static final int MAX_BREAK = 2;

  private static final String METHODS_PLACE_HOLDER = "--methods--";
  private static final String METHOD_BODY_PLACE_HOLDER = "--body--";
  private static final String METHOD_NAME_PREFIX = "--name--";
  private static final String LITERAL_PLACE_HOLDER = "--literal--";
  private static final String BLOCK_PLACE_HOLDER = "==blocks--";
  private static final String CONSTRUCTOR_PLACE_HOLDER = "--constructor--";
  private static final String DUMMY_STATEMENT = "System.out.println(\"ARG IS \" + arg);\n";
  private static final String CLASS_TEMPLATE =
    "package com.dremio.exec.compile.sample;\n" +
      "import java.util.Collections;\n" +
      "import java.util.Map;\n" +
      "import java.util.HashMap;\n" +
      "public class FooTest {\n" +
      CONSTRUCTOR_PLACE_HOLDER + System.lineSeparator() +
      METHODS_PLACE_HOLDER +
      "}\n";
  private static final String METHOD_TEMPLATE =
    "public void " + METHOD_NAME_PREFIX + "(int arg) {\n" +
      METHOD_BODY_PLACE_HOLDER +
      "}\n";
  private static final String NESTED_IF_CODE_TEMPLATE =
    "        if (arg == " + LITERAL_PLACE_HOLDER + ") {\n" +
      "           " + DUMMY_STATEMENT +
      "        } else { code\n }\n";

  private static final String FLAT_IF_CODE_TEMPLATE =
    "if (arg == " + LITERAL_PLACE_HOLDER + ") {\n" +
      DUMMY_STATEMENT +
      "}\n";

  private static final String BREAK_CODE_TEMPLATE =
    "block: {\n" + BLOCK_PLACE_HOLDER + "}\n";

  private static final String BREAK_BLOCK_CODE_TEMPLATE =
    "if (arg == " + LITERAL_PLACE_HOLDER + ") {\n" +
      DUMMY_STATEMENT +
      "break block;" +
      "}\n";

  private static final String MAP_CODE_CONSTRUCTOR =
    "private final Map<Integer, Runnable> runnables;\n" +
      "public FooTest(Map<Integer, Runnable> inputMap) {\n" +
      "this.runnables = Collections.unmodifiableMap(inputMap);\n}\n";

  private static final String MAP_CODE_TEMPLATE =
    "final Runnable x = (Runnable) this.runnables.get(arg);\n" +
      "if (x == null) {\n" + DUMMY_STATEMENT + "}\n";

  @Rule
  public TemporaryFolder testRootDir = new TemporaryFolder();

  @Test
  public void testNestedIfElseCode() {
    runTest(nestedIfElseCode(), 1);
  }

  @Test
  public void testFlatIfElseCode() {
    runTest(flatIfElseCode(), 1);
  }

  @Test
  public void testSwitchCode() {
    runTest(switchCaseCode(), 1);
  }

  @Test
  public void testBreakCode() {
    runTest(breakCode(), 1);
  }

  @Test
  public void testMapCode() {
    runTest(mapCode(), 1, () -> CLASS_TEMPLATE.replaceAll(CONSTRUCTOR_PLACE_HOLDER, MAP_CODE_CONSTRUCTOR));
  }

  @Test
  public void testMultiMethodCase() {
    runTest(switchCaseCode(), 100);
  }

  @Test
  public void testMultiMethodNestedIfElse() {
    runTest(nestedIfElseCode(), 100);
  }

  @Test
  public void testMultiMethodFlatIfElse() {
    runTest(flatIfElseCode(), 100);
  }

  @Test
  public void testMultiMethodBreak() {
    runTest(breakCode(), 100);
  }

  public void runTest(String embedded, int numMethods, Supplier<String> classTemplateSupplier) {

    File classes = testRootDir.getRoot();
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numMethods; i++) {
      sb.append(METHOD_TEMPLATE.replaceAll(METHOD_BODY_PLACE_HOLDER, embedded)
        .replaceAll(METHOD_NAME_PREFIX, "foo" + i)).append(System.lineSeparator());
    }
    final String largeSource = classTemplateSupplier.get().replaceAll(METHODS_PLACE_HOLDER, sb.toString())
      .replaceAll(CONSTRUCTOR_PLACE_HOLDER, "");
    try (URLClassLoader classLoader = new URLClassLoader(new URL[]{classes.toURI().toURL()}, null)) {
      JaninoClassCompiler janinoClassCompiler = new JaninoClassCompiler(classLoader);
      final long start = System.nanoTime();
      assertNotNull(janinoClassCompiler.getClassByteCode(
        new ClassTransformer.ClassNames("com.dremio.exec.compile.LargeRxpressionTest"), largeSource, true));
      System.out.println("ELAPSED = " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    } catch (IOException | CompileException | ClassNotFoundException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private void runTest(String embedded, int numMethods) {
    runTest(embedded, numMethods, () -> CLASS_TEMPLATE);
  }

  public static String nestedIfElseCode() {
    String generatedCode = NESTED_IF_CODE_TEMPLATE.replaceAll(LITERAL_PLACE_HOLDER, "0");
    for (int i = 1; i < MAX_NESTED_IF_ELSE; i++) {
      String toReplace = NESTED_IF_CODE_TEMPLATE.replaceAll(LITERAL_PLACE_HOLDER, String.valueOf(i));
      generatedCode = generatedCode.replaceAll("code", toReplace);
    }
    generatedCode = generatedCode.replaceAll("code", "{ \n" + DUMMY_STATEMENT + "     }\n");
    return generatedCode;
  }

  public static String flatIfElseCode() {
    StringBuilder generatedCode = new StringBuilder(FLAT_IF_CODE_TEMPLATE.replaceAll(LITERAL_PLACE_HOLDER, "0"));
    for (int i = 1; i < MAX_FLAT_IF_ELSE; i++) {
      generatedCode.append("else ").append(FLAT_IF_CODE_TEMPLATE.replaceAll(LITERAL_PLACE_HOLDER, String.valueOf(i)));
    }
    generatedCode.append("else {\n").append(DUMMY_STATEMENT).append("\n}\n");
    return generatedCode.toString();
  }

  public static String breakCode() {
    StringBuilder generatedCode = new StringBuilder(BREAK_BLOCK_CODE_TEMPLATE.replaceAll(LITERAL_PLACE_HOLDER, "0"));
    for (int i = 1; i < MAX_BREAK; i++) {
      generatedCode.append(BREAK_BLOCK_CODE_TEMPLATE.replaceAll(LITERAL_PLACE_HOLDER, String.valueOf(i)));
    }
    generatedCode.append("else {\n").append(DUMMY_STATEMENT).append("\n}\n");
    return BREAK_CODE_TEMPLATE.replaceAll(BLOCK_PLACE_HOLDER, generatedCode.toString());
  }

  public static String mapCode() {
    return MAP_CODE_TEMPLATE;
  }

  public static String switchCaseCode() {
    StringBuilder sb = new StringBuilder();
    sb.append("switch (arg) {").append(System.lineSeparator());
    for (int i = 0; i < MAX_CASE_STATEMENTS; i++) {
      sb.append("case ").append(i).append(": ").append(System.lineSeparator());
      sb.append("{").append(System.lineSeparator());
      sb.append(DUMMY_STATEMENT);
      sb.append(System.lineSeparator()).append("}").append(System.lineSeparator());
      sb.append("break;\n");
    }
    sb.append("default : break;\n");
    sb.append("}\n");
    return sb.toString();
  }
}
