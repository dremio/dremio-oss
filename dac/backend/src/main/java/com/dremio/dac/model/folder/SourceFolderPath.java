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
package com.dremio.dac.model.folder;

import java.util.List;

import com.dremio.dac.model.common.LeafEntity;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.dac.model.sources.SourceName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * Folder inside source.
 */
public class SourceFolderPath extends NamespacePath {
  /**
   * Creates a SourceFolderPath from a sourceName and a path component from a URL
   *
   * @param sourceName the name of the source
   * @param path a relative path represented as a string
   * @return a new SourceFolderPath instance
   */
  public static SourceFolderPath fromURLPath(SourceName sourceName, String path) {
    Iterable<String> components = Splitter.on('/').omitEmptyStrings().split(path);
    return new SourceFolderPath(ImmutableList.<String> builder().add(sourceName.getName()).addAll(components).build());
  }

  public SourceFolderPath(RootEntity root, List<FolderName> folderPath, FolderName name) {
    super(root, folderPath, name);
  }

  @JsonCreator
  public SourceFolderPath(final String path) {
    super(path);
  }

  public SourceFolderPath(List<String> fullPathList) {
    super(fullPathList);
  }

  @Override
  public RootEntity getRoot(String name) throws IllegalArgumentException {
    return new SourceName(name);
  }

  @Override
  public LeafEntity getLeaf(String name) throws IllegalArgumentException {
    return new FolderName(name);
  }

  public FolderPath getPathWithoutRoot() {
    return new FolderPath(toPathList().subList(1, toPathList().size()));
  }

  public SourceName getSourceName() {
    return new SourceName(getRoot().getName());
  }

  @Override
  public int getMinimumComponents() {
    return 2;
  }

  public FolderName getFolderName() {
    return (FolderName)getLeaf();
  }

  @Override
  protected String getDefaultUrlPathType() {
    return "folder";
  }

}
