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
package com.dremio.dac.model.sources;

import java.util.List;

import com.dremio.dac.model.common.LeafEntity;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.file.FileName;

public class VirtualDatasetPath extends NamespacePath {
  public VirtualDatasetPath(SourceName source, List<FolderName> folderPath, FileName fileName) {
    super(source, folderPath, fileName);
  }

  public VirtualDatasetPath(List<String> path) {
    super(path);
  }

  @Override
  public RootEntity getRoot(String name) throws IllegalArgumentException {
    return new SourceName(name);
  }

  @Override
  public LeafEntity getLeaf(String name) throws IllegalArgumentException {
    return new FileName(name);
  }

  @Override
  public int getMinimumComponents() {
    return 2;
  }

  @Override
  public SourceName getRoot() {
    return (SourceName) super.getRoot();
  }

  public FileName getFileName() {
    return new FileName(getLeaf().getName());
  }

  public SourceName getSourceName() {
    return new SourceName(getRoot().getName());
  }

  @Override
  protected String getDefaultUrlPathType() {
    return "dataset";
  }
}
