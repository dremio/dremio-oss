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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Locale;

import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.exec.compile.ClassTransformer.ClassNames;
import com.dremio.exec.exception.ClassTransformationException;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * Test classloading issues with {@code JDKClassCompiler} and {@code JaninoClassCompiler}
 */
public class TestClassCompilers {

  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /*
   * Classes are compiled independently into classes/ directory
   * so they don't end up into the test classpath
   */
  private static File classes;

  @BeforeClass
  public static void compileDependencyClass() throws IOException, ClassNotFoundException {
    JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    Assume.assumeNotNull(javaCompiler);

    classes = temporaryFolder.newFolder("classes");;

    StandardJavaFileManager fileManager = javaCompiler.getStandardFileManager(null, Locale.ROOT, UTF_8);
    fileManager.setLocation(StandardLocation.CLASS_OUTPUT, ImmutableList.of(classes));

    SimpleJavaFileObject compilationUnit = new SimpleJavaFileObject(URI.create("FooTest.java"), Kind.SOURCE) {
      String fooTestSource = Resources.toString(Resources.getResource("com/dremio/exec/compile/FooTest.java"), UTF_8);
      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        return fooTestSource;
      }
    };

    CompilationTask task = javaCompiler.getTask(null, fileManager, null, Collections.<String>emptyList(), null, ImmutableList.of(compilationUnit));
    assertTrue(task.call());
  }

  @Test
  public void testJDKCompilation() throws IOException, ClassTransformationException, CompileException, ClassNotFoundException {
    try(URLClassLoader classLoader = new URLClassLoader(new URL[] { classes.toURI().toURL()}, null)) {
      JDKClassCompiler jdkClassCompiler = JDKClassCompiler.newInstance(classLoader);
      testCompilation(jdkClassCompiler);
    };
  }

  @Test
  public void testJaninoCompilation() throws IOException, ClassTransformationException, CompileException, ClassNotFoundException {
    try(URLClassLoader classLoader = new URLClassLoader(new URL[] { classes.toURI().toURL()}, null)) {
      JaninoClassCompiler janinoClassCompiler = new JaninoClassCompiler(classLoader);
      testCompilation(janinoClassCompiler);
    };
  }

  private void testCompilation(ClassCompiler compiler) throws IOException, ClassTransformationException, CompileException, ClassNotFoundException {
    String barTestSource = Resources.toString(Resources.getResource("com/dremio/exec/compile/BarTest.java"), UTF_8);

    assertNotNull(compiler.getClassByteCode(new ClassNames("com.dremio.exec.compile.BarTest"), barTestSource, true));
  }
}
