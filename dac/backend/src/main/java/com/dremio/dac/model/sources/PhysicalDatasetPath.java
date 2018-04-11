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
package com.dremio.dac.model.sources;

import java.util.List;

import com.dremio.dac.model.common.LeafEntity;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.file.FileName;
import com.dremio.file.SourceFilePath;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * Raw dataset path can point to file or folder in source.
 */
public class PhysicalDatasetPath extends NamespacePath {

  /**
   * Creates a PhysicalDatasetPath from a sourceName and a path component from a URL
   *
   * @param sourceName the name of the source
   * @param path a relative path represented as a string
   * @return a new PhysicalDatasetPath instance
   */
  public static PhysicalDatasetPath fromURLPath(SourceName sourceName, String path) {
    Iterable<String> components = Splitter.on('/').omitEmptyStrings().split(path);
    return new PhysicalDatasetPath(ImmutableList.<String> builder().add(sourceName.getName()).addAll(components).build());
  }

  private final DatasetType datasetType;

  public PhysicalDatasetPath(SourceName source, List<FolderName> folderPath, FileName fileName) {
    super(source,  folderPath, fileName);
    this.datasetType = DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
  }

  public PhysicalDatasetPath(SourceName source, List<FolderName> folderPath, FolderName folderName) {
    super(source,  folderPath, folderName);
    this.datasetType = DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
  }

  public PhysicalDatasetPath(SourceFilePath filePath) {
    super(filePath.getRoot(), filePath.getFolderPath(), filePath.getLeaf());
    this.datasetType = DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
  }

  public PhysicalDatasetPath(SourceFolderPath folderPath) {
    super(folderPath.getRoot(), folderPath.getFolderPath(), folderPath.getLeaf());
    this.datasetType = DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
  }

  @JsonCreator
  public PhysicalDatasetPath(String path) {
    super(path);
    this.datasetType = DatasetType.PHYSICAL_DATASET;
  }

  public PhysicalDatasetPath(List<String> path) {
    super(path);
    this.datasetType = DatasetType.PHYSICAL_DATASET;
  }

  public PhysicalDatasetPath(List<String> path, DatasetType type) {
    super(path);
    this.datasetType = type;
  }

  @Override
  public RootEntity getRoot(String name) throws IllegalArgumentException {
    return new SourceName(name);
  }

  @Override
  public LeafEntity getLeaf(String name) throws IllegalArgumentException {
    if (datasetType == DatasetType.PHYSICAL_DATASET) {
      return new PhysicalDatasetName(name);
    }
    if (datasetType == DatasetType.PHYSICAL_DATASET_SOURCE_FILE || name.contains(".")) {
      return new FileName(name);
    }
    return new FolderName(name);
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


  public PhysicalDatasetName getDatasetName() {
    return new PhysicalDatasetName(getLeaf().getName());
  }

  public SourceName getSourceName() {
    return new SourceName(getRoot().getName());
  }

  public FolderName getFolderName() {
    return new FolderName(getLeaf().getName());
  }

  public SourceFilePath toSourceFilePath() {
    return new SourceFilePath(getRoot(), getFolderPath(), getFileName());
  }

  public SourceFolderPath toSourceFolderPath() {
    return new SourceFolderPath(getRoot(), getFolderPath(), getFolderName());
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }

  @Override
  protected String getDefaultUrlPathType() {
    return "dataset";
  }
}
