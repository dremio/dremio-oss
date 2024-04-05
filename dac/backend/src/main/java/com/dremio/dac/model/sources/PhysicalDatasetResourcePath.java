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

import static java.util.Arrays.asList;

import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.ResourcePath;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.List;

/** "dataset/{source.[folder.]*name}" */
public class PhysicalDatasetResourcePath extends ResourcePath {

  private final PhysicalDatasetPath dataset;

  public PhysicalDatasetResourcePath(PhysicalDatasetPath dataset) {
    this.dataset = dataset;
  }

  public PhysicalDatasetResourcePath(NamespacePath path, DatasetType type) {
    this.dataset = new PhysicalDatasetPath(path, type);
  }

  @JsonCreator
  public PhysicalDatasetResourcePath(String pathString) {
    List<String> path = parse(pathString, "dataset");
    if (path.size() != 1) {
      throw new IllegalArgumentException(
          "path should be of form: /dataset/{datasetPath}, found " + pathString);
    }
    this.dataset = new PhysicalDatasetPath(path.get(0));
  }

  @Override
  public List<String> asPath() {
    return asList("dataset", dataset.toPathString());
  }

  public PhysicalDatasetPath getDataset() {
    return dataset;
  }
}
