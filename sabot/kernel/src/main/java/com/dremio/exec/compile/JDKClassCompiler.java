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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

import javax.lang.model.SourceVersion;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.codehaus.commons.compiler.CompileException;

import com.dremio.exec.compile.ClassTransformer.ClassNames;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

class JDKClassCompiler extends AbstractClassCompiler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JDKClassCompiler.class);

  private final ImmutableList<String> defaultCompilerOptions;
  private final DiagnosticListener<JavaFileObject> listener;
  private final JavaCompiler compiler;
  private final DremioJavaFileManager fileManager;

  public static JDKClassCompiler newInstance() {
    // By default, the context classloader is the system classloader
    // but when running in container, it might represent the application class loader
    return newInstance(Thread.currentThread().getContextClassLoader());
  }


  @VisibleForTesting
  static JDKClassCompiler newInstance(ClassLoader classLoader) {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      logger.warn("JDK Java compiler not available - probably you're running Dremio with a JRE and not a JDK");
      return null;
    }
    return new JDKClassCompiler(compiler, classLoader);
  }

  private JDKClassCompiler(JavaCompiler compiler, ClassLoader classLoader) {
    this.compiler = compiler;
    this.listener = new DremioDiagnosticListener();
    this.fileManager = new DremioJavaFileManager(compiler.getStandardFileManager(listener, null, UTF_8));
    boolean aboveJava8 = false;
    try {
      aboveJava8 = compiler.getSourceVersions().contains(SourceVersion.valueOf("RELEASE_9"));
    } catch (IllegalArgumentException ignored) {
    }

    ImmutableList.Builder<String> compilerOptionsBuilder = ImmutableList.builder();
    if (aboveJava8) {
      compilerOptionsBuilder.add("-source", "8");
      compilerOptionsBuilder.add("-target", "8");
    }
    // Provides the application classpath to the compiler
    //
    // Javac cannot use the classloader directly so we need to convert it back
    // to a list of paths, if possible.
    //
    // Note that Maven surefire plugin encodes application classpath in META-INF/MANIFEST.MF
    // so using JavaFileManager#setLocation(Location, Iterable<File>) would not work as it
    // doesn't expand the classpath recursively, unlike the Java compiler '-classpath' option.
    // Unfortunately, Java compiler (jdk7) only handles relative paths, whereas
    // URLClassLoader supports both relative and absolute paths. Of course, surefire plugin uses
    // absolute paths, which causes the compiler not to found any of the classes :(
    // As surefire also sets "java.class.path" with the expanded version of the classpath (which seems
    // a reasonable thing to do since this is a well-known system property), we can let the java compiler
    // use its default behaviour if we detect the use of the plugin.
    String surefireRealClassPath = System.getProperty("surefire.real.class.path");
    if (surefireRealClassPath != null && classLoader == ClassLoader.getSystemClassLoader()) {
      logger.debug("Surefire detected. Compiler will automatically use the following classpath: {}", System.getProperty("java.class.path"));
    } else if (classLoader instanceof URLClassLoader) {
      List<String> files = getClassPath((URLClassLoader) classLoader);
      if (!files.isEmpty()) {
        compilerOptionsBuilder.add("-classpath", Joiner.on(File.pathSeparator).join(files));
      }
    } else if (classLoader != null) {
      // the class loader cannot be converted back to a list of urls
      // let's fall back to the default behavior of the compiler to rely on
      // standard system properties with the classpath being set.
      logger.warn(
          "Provided classLoader for compilation is not a URLClassLoader (was: {}). You might have compilation issues.",
          classLoader.getClass().getName());
    }

    this.defaultCompilerOptions = compilerOptionsBuilder.build();
  }

  private static List<String> getClassPath(URLClassLoader classLoader) {
    ImmutableList.Builder<String> files = ImmutableList.builder();

    URL[] urls = classLoader.getURLs();
    for(URL url: urls) {
      URI uri;
      try {
        uri = url.toURI();
      } catch (URISyntaxException e) {
        logger.warn("Invalid URL in classpath: {}", url);
        continue;
      }

      if (!"file".equals(uri.getScheme())) {
        logger.debug("Ignoring non-file URI: {}", uri);
        continue;
      }

     files.add(new File(uri).getAbsolutePath());
    }

    return files.build();
  }

  @Override
  protected ClassBytes[] getByteCode(final ClassNames className, final String sourceCode, boolean debug)
      throws CompileException, IOException, ClassNotFoundException {
    try {
      // Create one Java source file in memory, which will be compiled later.
      DremioJavaFileObject compilationUnit = new DremioJavaFileObject(className.dot, sourceCode);

      Iterable<String> compilerOptions = Iterables.concat(
          ImmutableList.of(debug ? "-g:source,lines,vars" : "-g:none"),
          defaultCompilerOptions
      );
      CompilationTask task = compiler.getTask(null, fileManager, listener, compilerOptions, null, Collections.singleton(compilationUnit));

      // Run the compiler.
      if(!task.call()) {
        throw new CompileException("Compilation failed", null);
      } else if (!compilationUnit.isCompiled()) {
        throw new ClassNotFoundException(className + ": Class file not created by compilation.");
      }
      // all good
      return compilationUnit.getByteCode();
    } catch (RuntimeException rte) {
      // Unwrap the compilation exception and throw it.
      Throwable cause = rte.getCause();
      if (cause != null) {
        cause = cause.getCause();
        if (cause instanceof CompileException) {
          throw (CompileException) cause;
        }
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
      }
      throw rte;
    }
  }

  @Override
  protected org.slf4j.Logger getLogger() { return logger; }

}
