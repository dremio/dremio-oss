/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.namespace;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;

import com.dremio.common.utils.PathUtils;
import com.google.common.collect.ImmutableList;

/**
 * NamespaceKey describes list representation of dotted schema path.
 */
public class NamespaceKey {
  private final List<String> pathComponents;
  private final String schemaPath;

  public NamespaceKey(String root) {
    this(Collections.singletonList(root));
  }

  public NamespaceKey(List<String> pathComponents) {
    checkNotNull(pathComponents);
    this.pathComponents = ImmutableList.copyOf(pathComponents);
    this.schemaPath = PathUtils.constructFullPath(pathComponents);
  }

  public int size(){
    return pathComponents.size();
  }

  public List<String> getPathComponents() {
    return pathComponents;
  }

  public String getSchemaPath() {
    return schemaPath;
  }

  @Override
  public int hashCode() {
    return schemaPath.hashCode();
  }

  @Override
  public String toString() {
    return schemaPath;
  }

  public String toUrlEncodedString() {
    return PathUtils.encodeURIComponent(schemaPath);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      NamespaceKey o = (NamespaceKey) obj;
      return pathComponents.equals(o.pathComponents);
    }
    return false;
  }

  public String getName() {
    return pathComponents.get(pathComponents.size() - 1);
  }

  public String getRoot() {
    return pathComponents.get(0);
  }

  public NamespaceKey getParent() {
    return new NamespaceKey(pathComponents.subList(0, pathComponents.size() - 1));
  }

  public boolean hasParent() {
    return pathComponents.size() > 1;
  }
}
