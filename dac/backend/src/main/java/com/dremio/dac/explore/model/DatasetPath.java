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
package com.dremio.dac.explore.model;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

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
 * The full path to a dataset
 *
 */
public class DatasetPath extends NamespacePath {

  public static final DatasetPath NONE = new DatasetPath("__none");

  private static final String URL_PATH_TYPE = "dataset";

  public static DatasetPath fromURLPath(RootEntity root, String path) {
    List<String> components = PathUtils.toPathComponents(path);

    return new DatasetPath(ImmutableList.<String> builder().add(root.getName()).addAll(components).build());
  }

  public DatasetPath(RootEntity root, DatasetName dataset) {
    this(root, Collections.<FolderName>emptyList(), dataset);
  }

  public DatasetPath(RootEntity root, List<FolderName> folderPath, DatasetName dataset) {
    super(root, folderPath, dataset);
  }

  @JsonCreator
  public DatasetPath(String path) {
    super(path);
  }

  @JsonCreator
  public DatasetPath(List<String> path) {
    super(path);
  }

  @Override
  public RootEntity getRoot(final String name) {
    if (TempSpace.isTempSpace(name)) {
      return TempSpace.impl();
    }
    if (name.startsWith(HomeName.HOME_PREFIX)) {
      return new HomeName(name);
    }
    return new SpaceName(name);
  }

  public Table getTable(SchemaPlus rootSchema){
    List<FolderName> components = this.getFolderPath();
    SchemaPlus schema = rootSchema.getSubSchema(this.getRoot().getName());
    if(schema == null){
      throw new IllegalStateException(String.format("Failure finding schema path %s in position 0 of path %s", getRoot().getName(), toPathString()));
    }

    int i = 1;
    for(FolderName folder : components){
      schema = schema.getSubSchema(folder.getName());
      if(schema == null){
        throw new IllegalStateException(String.format("Failure finding schema path %s in position %d of path %s", folder.getName(), i, toPathString()));
      }
      i++;
    }
    Table table = schema.getTable(getLeaf().getName());
    if(table == null){
      throw new IllegalStateException(String.format("Failure finding table in path %s. The schema exists but no table in that schema matches %s", toPathString(), getLeaf().getName()));
    }

    return table;
  }

  public DatasetName getDataset() {
    return (DatasetName)getLeaf();
  }

  @Override
  public LeafEntity getLeaf(String name) throws IllegalArgumentException {
    return new DatasetName(name);
  }

  public final DatasetPath rename(DatasetName newDatasetName) {
    return new DatasetPath(getRoot(), getFolderPath(), newDatasetName);
  }

  @Override
  public int getMinimumComponents() {
    return 1;
  }

  @Override
  public String getDefaultUrlPathType() {
    return URL_PATH_TYPE;
  }

  @Override
  public int hashCode() {
    return toPathList().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof DatasetPath)) {
      return false;
    }
    return toPathList().equals(((DatasetPath)obj).toPathList());
  }

  public String toUnescapedString() {
    return PathUtils.getKeyJoiner().join(toPathList());
  }
}
