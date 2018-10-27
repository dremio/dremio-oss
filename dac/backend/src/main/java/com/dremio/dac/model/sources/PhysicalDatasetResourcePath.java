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

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.ResourcePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * "source/{sourceName}/dataset/{source.[folder.]*name}"
 *
 */
public class PhysicalDatasetResourcePath extends ResourcePath {

  private final PhysicalDatasetPath dataset;
  private final SourceName sourceName;

  public PhysicalDatasetResourcePath(SourceName sourceName, PhysicalDatasetPath dataset) {
    this.sourceName = sourceName;
    this.dataset = dataset;
  }

  public PhysicalDatasetResourcePath(NamespacePath path, DatasetType type) {
    PhysicalDatasetPath pdp = new PhysicalDatasetPath(path, type);
    this.sourceName = pdp.getRoot();
    this.dataset = pdp;
  }

  @JsonCreator
  public PhysicalDatasetResourcePath(String pathString) {
    List<String> path = parse(pathString, "source", "dataset");
    if (path.size() != 2) {
      throw new IllegalArgumentException("path should be of form: /source/{sourceName}/dataset/{datasetPath}, found " + pathString);
    }
    this.sourceName = new SourceName(path.get(0));
    this.dataset = new PhysicalDatasetPath(path.get(1));
    if (this.dataset.getRootType() != RootEntity.RootType.SOURCE) {
      throw new IllegalArgumentException("file path does not belong to a source, " + pathString);
    }
  }

  @Override
  public List<String> asPath() {
    return asList("source", sourceName.getName(), "dataset", dataset.toPathString());
  }

  public PhysicalDatasetPath getDataset() {
    return dataset;
  }
}
