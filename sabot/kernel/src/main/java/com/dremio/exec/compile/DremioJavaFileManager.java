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

import java.io.IOException;
import java.util.Set;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.StandardJavaFileManager;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

/* package */
class DremioJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioJavaFileManager.class);

  public static final Predicate<Kind> NO_SOURCES_KIND = new Predicate<Kind>() {
    @Override
    public boolean apply(Kind input) {
      return input != Kind.SOURCE;
    }
  };

  public DremioJavaFileManager(StandardJavaFileManager fileManager) {
    super(fileManager);
  }

  @Override
  public Iterable<JavaFileObject> list(Location location, String packageName, Set<Kind> kinds, boolean recurse) throws IOException {
    return super.list(location, packageName, Sets.filter(kinds, NO_SOURCES_KIND), recurse);
  }

  @Override
  public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling) throws IOException {
    logger.trace("Creating JavaFileForOutput@(location:{}, className:{}, kinds:{})", location, className, kind);
    if (sibling != null && sibling instanceof DremioJavaFileObject) {
      return ((DremioJavaFileObject)sibling).addOutputJavaFile(className);
    }
    throw new IOException("The source file passed to getJavaFileForOutput() is not a DremioJavaFileObject: " + sibling);
  }

}
