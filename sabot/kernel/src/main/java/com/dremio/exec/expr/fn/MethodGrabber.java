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

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.Unparser;
import org.codehaus.janino.util.AbstractTraverser;
import org.codehaus.janino.util.DeepCopier;

public class MethodGrabber extends AbstractTraverser<RuntimeException> {
  private final Class<?> clazz;
  private final Map<String, String> methods = new HashMap<>();
  private boolean captureMethods = false;

  private MethodGrabber(Class<?> c) {
    super();
    this.clazz = c;
  }

  @Override
  public void traverseClassDeclaration(Java.AbstractClassDeclaration cd) {
    boolean prevCapture = captureMethods;
    captureMethods = clazz.getName().equals(cd.getClassName());
    super.traverseClassDeclaration(cd);
    captureMethods = prevCapture;
  }

  @Override
  public void traverseMethodDeclarator(MethodDeclarator md) {
    if (captureMethods) {
      Java.LabeledStatement ls;
      try {
        ls = xFormReturnToLabel(md);
      } catch (CompileException e) {
        throw new RuntimeException(e);
      }
      if (ls != null) {
        StringWriter writer = new StringWriter();
        Unparser unparser = new Unparser(writer);
        unparser.unparseBlockStatement(ls);
        unparser.close();
        methods.put(md.name, writer.getBuffer().toString());
      }
    }
  }

  private Java.LabeledStatement xFormReturnToLabel(MethodDeclarator md) throws CompileException {
    final List<? extends Java.BlockStatement> statements = md.statements;
    if (md.statements == null) {
      return null;
    }
    String[] fQCN = md.getDeclaringType().getClassName().split("\\.");
    String returnLabel = fQCN[fQCN.length - 1] + "_" + md.name;
    Java.Block b = new Java.Block(md.getLocation());
    b.addStatements(
        new DeepCopier() {

          @Override
          public Java.BlockStatement copyReturnStatement(Java.ReturnStatement subject) {
            return new Java.BreakStatement(subject.getLocation(), returnLabel);
          }
        }.copyBlockStatements(statements));
    return new Java.LabeledStatement(md.getLocation(), returnLabel, b);
  }

  public static Map<String, String> getMethods(Java.AbstractCompilationUnit cu, Class<?> c) {
    MethodGrabber grabber = new MethodGrabber(c);
    grabber.visitAbstractCompilationUnit(cu);
    return grabber.methods;
  }
}
