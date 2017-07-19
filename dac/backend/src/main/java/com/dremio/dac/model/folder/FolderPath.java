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
package com.dremio.dac.model.folder;

import java.util.Arrays;
import java.util.List;

import com.dremio.dac.model.common.LeafEntity;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.TempSpace;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;

/**
 * The full path to a folder in space/home.
 *
 */
public class FolderPath extends NamespacePath {
  /**
   * Creates a FolderPath from a root entity and a path component from a URL
   *
   * @param root the root entity
   * @param path a relative path represented as a string
   * @return a new FolderPath instance
   */
  public static FolderPath fromURLPath(RootEntity root, String path) {
    List<String> components = Splitter.on('/').omitEmptyStrings().splitToList(path);
    return new FolderPath(ImmutableList.<String> builder().add(root.getName()).addAll(components).build());
  }

  public FolderPath(RootEntity root, List<FolderName> folderPath, FolderName name) {
    super(root, folderPath, name);
  }

  public FolderPath(final String path) {
    super(path);
  }

  public FolderPath(final String space, final String urlPath) {
    super(Arrays.asList(ObjectArrays.concat(new String[] { space }, urlPath.split("/"), String.class)));
  }

  public FolderPath(final List<String> path) {
    super(path);
  }

  public final FolderPath rename(String newName) {
    return new FolderPath(getRoot(), getFolderPath(), new FolderName(newName));
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
    return new FolderName(name);
  }

  // one can list source and space as folder
  @Override
  public int getMinimumComponents() {
    return 1;
  }

  public FolderName getFolderName() {
    return (FolderName) getLeaf();
  }

  @Override
  protected String getDefaultUrlPathType() {
    return "folder";
  }


}
