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
package com.dremio.file;

import java.util.List;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.model.common.LeafEntity;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.TempSpace;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;

/**
 * The full path to a file.
 * File must start with a home space @home.folder1...folderN.filename
 */
public final class FilePath extends NamespacePath {
  /**
   * Creates a FilePath from a root entity and a path component from a URL
   *
   * @param root the root entity
   * @param path a relative path represented as a string
   * @return a new FilePath instance
   */
  public static FilePath fromURLPath(RootEntity root, String path) {
    List<String> components = PathUtils.toPathComponents(path);

    return new FilePath(ImmutableList.<String> builder().add(root.getName()).addAll(components).build());
  }

  public FilePath(RootEntity root, List<FolderName> folderPath, FileName fileName) {
    super(root, folderPath, fileName);
  }

  @JsonCreator
  public FilePath(String path) {
    super(path);
  }

  public FilePath(List<String> fullPathList) {
    super(fullPathList);
  }

  public final FilePath rename(String newName) {
    return new FilePath(getRoot(), getFolderPath(), new FileName(newName));
  }

  @Override
  public RootEntity getRoot(String name) throws IllegalArgumentException {
    if (TempSpace.isTempSpace(name)) {
      return TempSpace.impl();
    }
    if (name.startsWith(HomeName.HOME_PREFIX)) {
      return new HomeName(name);
    }
    return new SpaceName(name);
  }

  @Override
  public LeafEntity getLeaf(String name) throws IllegalArgumentException {
    return new FileName(name);
  }

  @Override
  public int getMinimumComponents() {
    return 2;
  }

  public FileName getFileName() {
    return (FileName)getLeaf();
  }

  @Override
  protected String getDefaultUrlPathType() {
    return "file";
  }

}

