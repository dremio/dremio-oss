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

import com.dremio.common.util.DremioStringUtils;
import com.dremio.exec.compile.ClassTransformer.ClassNames;
import com.dremio.exec.exception.ClassTransformationException;
import java.io.IOException;
import org.codehaus.commons.compiler.CompileException;

public abstract class AbstractClassCompiler implements ClassCompiler {
  protected AbstractClassCompiler() {}

  @Override
  public ClassBytes[] getClassByteCode(ClassNames className, String sourceCode, boolean debug)
      throws CompileException, IOException, ClassNotFoundException, ClassTransformationException {
    if (getLogger().isDebugEnabled()) {
      getLogger()
          .debug(
              "Compiling (source size={}):\n{}",
              DremioStringUtils.readable(sourceCode.length()),
              debug ? prefixLineNumbers(sourceCode) : false);

      /* uncomment this to get a dump of the generated source in /tmp
            // This can be used to write out the generated operator classes for debugging purposes
            // TODO: should these be put into a directory named with the query id and/or fragment id
            final int lastSlash = className.slash.lastIndexOf('/');
            final File dir = new File("/tmp", className.slash.substring(0, lastSlash));
            dir.mkdirs();
            final File file = new File(dir, className.slash.substring(lastSlash + 1) + ".java");
            final FileWriter writer = new FileWriter(file);
            writer.write(sourceCode);
            writer.close();
      */
    }
    return getByteCode(className, sourceCode, debug);
  }

  private String prefixLineNumbers(String code) {
    StringBuilder out = new StringBuilder();
    int i = 1;
    for (String line : code.split("\n")) {
      int start = out.length();
      out.append(i++);
      int numLength = out.length() - start;
      out.append(":");
      for (int spaces = 0; spaces < 7 - numLength; ++spaces) {
        out.append(" ");
      }
      out.append(line);
      out.append('\n');
    }
    return out.toString();
  }

  protected abstract ClassBytes[] getByteCode(
      ClassNames className, String sourcecode, boolean debug)
      throws CompileException, IOException, ClassNotFoundException, ClassTransformationException;

  protected abstract org.slf4j.Logger getLogger();
}
