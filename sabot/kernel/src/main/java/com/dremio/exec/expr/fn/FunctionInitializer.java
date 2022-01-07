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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.FileUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;

/**
 * To avoid the cost of initializing all functions up front,
 * this class contains all information required to initialize a function when it is used.
 */
public class FunctionInitializer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionInitializer.class);

  private final String className;
  private ImmutableMap<String, String> methods;
  private volatile boolean ready;
  private final AtomicLong count = new AtomicLong();

  /**
   * @param className the fully qualified name of the class implementing the function
   */
  public FunctionInitializer(String className) {
    super();
    this.className = className;
  }

  /**
   * @return the fully qualified name of the class implementing the function
   */
  public String getClassName() {
    return className;
  }

  public Collection<String> getMethodNames() {
    return methods.keySet();
  }

  public long getCount() {
    return count.get();
  }

  /**
   * Gets the source (as a labelled statement) for the given method name.
   *
   * @param methodName name of the method
   * @return the content of the method (for java code gen inlining)
   */
  public String getMethod(String methodName) {
    checkInit();
    return methods.get(methodName);
  }

  private void checkInit() {
    count.incrementAndGet();
    if (ready) {
      return;
    }

    synchronized (this) {
      if (ready) {
        return;
      }

      // get function body.

      try {
        final Class<?> clazz = Class.forName(className);
        final Java.AbstractCompilationUnit cu = get(clazz);

        if (cu == null) {
          throw new IOException(String.format("Failure while loading class %s.", clazz.getName()));
        }

        methods = ImmutableMap.copyOf(MethodGrabber.getMethods(cu, clazz));
        if (methods.isEmpty()) {
          throw UserException.functionError()
            .message("Failure reading Function class. No methods were found.")
            .addContext("Function Class", className)
            .build(logger);
        } else {
          if (logger.isTraceEnabled()) {
            logger.trace("{} Methods for class {} loaded. Methods available: {}", methods.size(), className,
              methods.entrySet());
          }
        }
        ready = true;
      } catch (IOException | ClassNotFoundException e) {
        throw UserException.functionError(e)
          .message("Failure reading Function class.")
          .addContext("Function Class", className)
          .build(logger);
      }
    }
  }

  /**
   * Helper method to get the resource URL of a source file based on the class
   * name.
   *
   * @param c the class name
   * @return the URL to the source file
   */
  public static URL getSourceURL(Class<?> c) {
    String path = c.getName();
    path = path.replaceFirst("\\$.*", "");
    path = path.replace(".", FileUtils.separator);
    path = "/" + path + ".java";

    return Resources.getResource(c, path);
  }

  private Java.AbstractCompilationUnit get(Class<?> c) throws IOException {
    URL u = getSourceURL(c);
    try (Reader reader = Resources.asCharSource(u, UTF_8).openStream()) {
      String body = CharStreams.toString(reader);

      for (Replacement r : REPLACERS) {
        body = r.apply(body);
      }

      try {
        return new Parser(new Scanner(null, new StringReader(body))).parseAbstractCompilationUnit();
      } catch (CompileException e) {
        logger.warn("Failure while parsing function class:\n{}", body, e);
        return null;
      }
    }
  }

  private static class Replacement {
    private final Pattern from;
    private final String to;

    public Replacement(String from, String to) {
      this.from = Pattern.compile("\b" + from + "\b");
      this.to = to;
    }

    public String apply(String body) {
      return from.matcher(body).replaceAll(to);
    }
  }

  private static Replacement r(String type) {
    return new Replacement(type + "Holder", "Nullable" + type + "Holder");
  }

  private static final Replacement[] REPLACERS = {r("BigInt"), r("Int"), r("Float4"), r("Float8"), r("VarChar"),
    r("VarBinary"), r("Time"), r("TimeStampMilli"), r("Date"), r("Decimal")};
}
