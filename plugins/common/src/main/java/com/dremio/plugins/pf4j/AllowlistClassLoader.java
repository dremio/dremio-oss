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
package com.dremio.plugins.pf4j;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * A classloader delegating to its parent if and only if a class or a resource
 * belongs to the allow list
 */
final class AllowlistClassLoader extends ClassLoader {

  private final ImmutableList<String> allowlist;

  private AllowlistClassLoader(ClassLoader parent, List<String> allowlist) {
    super(parent);
    this.allowlist = ImmutableList.copyOf(allowlist);
  }

  /**
   * Wrap an existing classloader to only allow resources and classes present in the allowlist
   * to be found
   *
   * @param parent the parent classloader
   * @param allowlist the allowed list of class/resource names prefixes. Prefixes should use '/' character as the delimiter
   * @return
   */
  public static ClassLoader of(ClassLoader parent, List<String> allowlist) {
    return new AllowlistClassLoader(parent, allowlist);
  }

  private static boolean matchPackage(List<String> packages, String resourceName) {
    return packages.stream().anyMatch(resourceName::startsWith);
  }

  @Override
  public URL getResource(String name) {
    if (!matchPackage(allowlist, name)) {
      return null;
    }
    return super.getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    if (!matchPackage(allowlist, name)) {
      return Collections.emptyEnumeration();
    }
    return super.getResources(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    if (!matchPackage(allowlist, name)) {
      return null;
    }
    return super.getResourceAsStream(name);
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    final String resourceName = name.replace('.', '/');
    if (!matchPackage(allowlist, resourceName)) {
      throw new ClassNotFoundException(name);
    }
    return super.loadClass(name);
  }
}
