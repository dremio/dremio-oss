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
package com.dremio.service.namespace;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.dremio.common.utils.PathUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * NamespaceKey describes list representation of dotted schema path.
 */
public class NamespaceKey {
  private static final Joiner DOT_JOINER = Joiner.on('.');

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

  @JsonCreator
  public NamespaceKey(@JsonProperty("pathComponents") List<String> pathComponents, @JsonProperty("schemaPath") String schemaPath) {
    checkNotNull(pathComponents);
    this.pathComponents = ImmutableList.copyOf(pathComponents);
    this.schemaPath = StringUtils.isEmpty(schemaPath) ? PathUtils.constructFullPath(pathComponents) : schemaPath;
  }

  public int size(){
    return pathComponents.size();
  }

  public List<String> getPathComponents() {
    return pathComponents;
  }

  @JsonIgnore
  public NamespaceKey getChild(String name) {
    return new NamespaceKey(ImmutableList.copyOf(Iterables.concat(pathComponents, ImmutableList.of(name))));
  }

  public String getSchemaPath() {
    return schemaPath;
  }

  @JsonIgnore
  public String getLeaf() {
    return pathComponents.get(pathComponents.size() - 1);
  }

  @Override
  public int hashCode() {
    return schemaPath.hashCode();
  }

  @JsonIgnore
  public NamespaceKey asLowerCase() {
    return new NamespaceKey(pathComponents.stream().map(String::toLowerCase).collect(Collectors.toList()));
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
    if (obj != null && obj instanceof NamespaceKey) {
      NamespaceKey o = (NamespaceKey) obj;
      return pathComponents.equals(o.pathComponents);
    }
    return false;
  }

  @JsonIgnore
  public String getName() {
    return pathComponents.get(pathComponents.size() - 1);
  }

  @JsonIgnore
  public String getRoot() {
    return pathComponents.get(0);
  }

  @JsonIgnore
  public List<String> getPathWithoutRoot() {
    return pathComponents.subList(1, pathComponents.size());
  }

  @JsonIgnore
  public NamespaceKey getParent() {
    return new NamespaceKey(pathComponents.subList(0, pathComponents.size() - 1));
  }

  @JsonIgnore
  public String toUnescapedString() {
    return DOT_JOINER.join(pathComponents);
  }

  @JsonIgnore
  public boolean hasParent() {
    return pathComponents.size() > 1;
  }
}
