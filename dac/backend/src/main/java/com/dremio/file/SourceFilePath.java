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
package com.dremio.file;

import com.dremio.dac.model.common.LeafEntity;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.sources.SourceName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.util.List;

/** The full path to a file in source. File must start with a source.folder1...folderN.filename */
public final class SourceFilePath extends NamespacePath {
  /**
   * Creates a SourceFilePath from a sourceName and a path component from a URL
   *
   * @param sourceName the name of the source
   * @param path a relative path represented as a string
   * @return a new SourceFilePath instance
   */
  public static SourceFilePath fromURLPath(SourceName sourceName, String path) {
    Iterable<String> components = Splitter.on('/').omitEmptyStrings().split(path);
    return new SourceFilePath(
        ImmutableList.<String>builder().add(sourceName.getName()).addAll(components).build());
  }

  public SourceFilePath(RootEntity root, List<FolderName> folderPath, FileName fileName) {
    super(root, folderPath, fileName);
  }

  @JsonCreator
  public SourceFilePath(String path) {
    super(path);
  }

  public SourceFilePath(List<String> fullPathList) {
    super(fullPathList);
  }

  public final SourceFilePath rename(String newName) {
    return new SourceFilePath(getRoot(), getFolderPath(), new FileName(newName));
  }

  @Override
  public RootEntity getRoot(String name) throws IllegalArgumentException {
    return new SourceName(name);
  }

  @Override
  public LeafEntity getLeaf(String name) throws IllegalArgumentException {
    return new FileName(name);
  }

  public SourceName getSourceName() {
    return new SourceName(getRoot().getName());
  }

  public FileName getFileName() {
    return (FileName) getLeaf();
  }

  @Override
  public int getMinimumComponents() {
    return 2;
  }

  @Override
  protected String getDefaultUrlPathType() {
    return "file";
  }
}
