/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import javax.tools.SimpleJavaFileObject;

import com.google.common.collect.Maps;

final class DremioJavaFileObject extends SimpleJavaFileObject {
  private final String sourceCode;

  private final String className;
  private final ByteArrayOutputStream outputStream;

  private Map<String, DremioJavaFileObject> outputFiles;

  public DremioJavaFileObject(final String className, final String sourceCode) {
    super(makeURI(className), Kind.SOURCE);
    this.sourceCode = sourceCode;
    this.className = className;
    this.outputStream = null;
  }

  private DremioJavaFileObject(final String className, final Kind kind) {
    super(makeURI(className), kind);
    this.outputStream = new ByteArrayOutputStream();
    this.className = className;
    this.sourceCode = null;
  }

  public boolean isCompiled() {
    return (outputFiles != null);
  }

  public ClassBytes[] getByteCode() {
    if (!isCompiled()) {
      return null;
    } else {
      int index = 0;
      ClassBytes[] byteCode = new ClassBytes[outputFiles.size()];
      for(DremioJavaFileObject outputFile : outputFiles.values()) {
        byteCode[index++] = new ClassBytes(outputFile.className, outputFile.outputStream.toByteArray());
      }
      return byteCode;
    }
  }

  public DremioJavaFileObject addOutputJavaFile(String className) {
    if (outputFiles == null) {
      outputFiles = Maps.newLinkedHashMap();
    }
    DremioJavaFileObject outputFile = new DremioJavaFileObject(className, Kind.CLASS);
    outputFiles.put(className, outputFile);
    return outputFile;
  }

  @Override
  public Reader openReader(final boolean ignoreEncodingErrors) throws IOException {
    return new StringReader(sourceCode);
  }

  @Override
  public CharSequence getCharContent(final boolean ignoreEncodingErrors) throws IOException {
    if (sourceCode == null) {
      throw new UnsupportedOperationException("This instance of DremioJavaFileObject is not an input object.");
    }
    return sourceCode;
  }

  @Override
  public OutputStream openOutputStream() {
    if (outputStream == null) {
      throw new UnsupportedOperationException("This instance of DremioJavaFileObject is not an output object.");
    }
    return outputStream;
  }

  private static URI makeURI(final String canonicalClassName) {
    final int dotPos = canonicalClassName.lastIndexOf('.');
    final String simpleClassName = dotPos == -1 ? canonicalClassName : canonicalClassName.substring(dotPos + 1);
    try {
      return new URI(simpleClassName + Kind.SOURCE.extension);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

}
