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
package com.dremio.dac.model.common;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Lists;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.UriBuilder;

/** Return list of path components to form namespace key */
public abstract class NamespacePath {
  private final RootEntity root;
  private final List<FolderName> folderPath;
  private final LeafEntity leaf;
  private final NamespaceKey namespaceKey;

  public NamespacePath(RootEntity root, List<FolderName> folderPath, LeafEntity leaf) {
    this.root = root;
    this.folderPath = folderPath;
    this.leaf = leaf;
    this.namespaceKey = new NamespaceKey(createPathList());
  }

  public NamespacePath(String path) {
    this(PathUtils.parseFullPath(path));
  }

  public NamespacePath(final List<String> path) {
    final List<String> pathComponents = new ArrayList<>(path);
    int length = pathComponents.size();
    if (length < getMinimumComponents()) {
      throw new IllegalArgumentException(
          "path too short: " + PathUtils.getPathJoiner().join(pathComponents));
    }
    if (getMaximumComponents() != -1 && length > getMaximumComponents()) {
      throw new IllegalArgumentException(
          "path too long: " + PathUtils.getPathJoiner().join(pathComponents));
    }

    this.root = getRoot(pathComponents.remove(0));
    --length;
    if (length > 0) {
      --length;
      this.leaf = getLeaf(pathComponents.remove(length));
    } else {
      this.leaf = null;
    }
    final List<FolderName> folders = new ArrayList<>();
    for (final String folder : pathComponents) {
      folders.add(new FolderName(folder));
    }
    this.folderPath = Collections.unmodifiableList(folders);
    this.namespaceKey = new NamespaceKey(path);
  }

  public NamespaceKey toNamespaceKey() {
    return namespaceKey;
  }

  public List<String> toPathList() {
    return namespaceKey.getPathComponents();
  }

  // Get list of path components.
  private List<String> createPathList() {
    final List<String> path = new ArrayList<>();
    path.add(root.getName());
    for (FolderName folder : folderPath) {
      path.add(folder.getName());
    }
    if (leaf != null) {
      path.add(leaf.getName());
    }
    return path;
  }

  // Get list of Parent path components.
  public List<String> toParentPathList() {
    final List<String> path = new ArrayList<>();
    path.add(root.getName());
    for (FolderName folder : folderPath) {
      path.add(folder.getName());
    }
    return path;
  }

  public List<FolderName> getFolderPath() {
    return folderPath;
  }

  @JsonValue
  public String toPathString() {
    return namespaceKey.getSchemaPath();
  }

  public String toParentPath() {
    return PathUtils.constructFullPath(toParentPathList());
  }

  @Override
  public String toString() {
    return toPathString();
  }

  public RootEntity getRoot() {
    return root;
  }

  public RootEntity.RootType getRootType() {
    return root.getRootType();
  }

  public LeafEntity getLeaf() {
    return leaf;
  }

  public LeafEntity.LeafType getLeafType() {
    return leaf.getLeafType();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return toString().equals(obj.toString());
  }

  public int getMaximumComponents() {
    return -1;
  }

  public abstract RootEntity getRoot(String name);

  public abstract LeafEntity getLeaf(String name);

  public abstract int getMinimumComponents();

  public String toUrlPath() {
    return toUrlPathWithAction(getDefaultUrlPathType());
  }

  public String toUrlPathWithAction(String action) {
    return this.toUrlPathWithRootAndAction(action);
  }

  protected String getDefaultUrlPathType() {
    return null;
  }

  private static final class URITemplateBuilder {

    private final List<String> parts = Lists.newArrayList();

    public void add(String part) {
      parts.add(part);
    }

    public final String build() {
      final StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < parts.size(); ++i) {
        stringBuilder.append("/" + PathUtils.encodeURIComponent(parts.get(i)));
      }
      return stringBuilder.toString();
    }
  }

  protected String toUrlPathWithRootAndAction(String action) {
    final URITemplateBuilder builder = new URITemplateBuilder();
    builder.add(root.getRootUrl());
    builder.add(root.getName());

    if (action != null) {
      builder.add(action);
      if (folderPath != null) {
        for (FolderName folderName : folderPath) {
          builder.add(folderName.toString());
        }
      }
      if (leaf != null) {
        builder.add(leaf.getName());
      }
    }

    return builder.build();
  }

  /**
   * Generates special url for querying datasets. This should be temporary (famous last words) until
   * dataset urls are made consistent.
   */
  public String getQueryUrlPath() {
    List<String> pathList = toPathList();
    final URITemplateBuilder builder = new URITemplateBuilder();
    builder.add(root.getRootUrl());
    builder.add(root.getName());
    builder.add(PathUtils.constructFullPath(pathList.subList(1, pathList.size())));
    return builder.build();
  }

  public String getPreviewDataUrlPath() {
    List<String> pathList = toPathList();
    URI uri =
        UriBuilder.fromUri("/dataset/{fullPath}/preview")
            .build(PathUtils.constructFullPath(pathList));
    return uri.toString();
  }

  public String getUrlFromPath(List<String> pathList) {
    final URITemplateBuilder builder = new URITemplateBuilder();
    for (String path : pathList) {
      builder.add(path);
    }
    return builder.build();
  }

  // default impl assume its a folder
  @JsonCreator
  public static NamespacePath defaultImpl(final String pathString) {
    return new FolderPath(pathString);
  }

  @JsonCreator
  public static NamespacePath defaultImpl(final List<String> path) {
    return new FolderPath(path);
  }
}
