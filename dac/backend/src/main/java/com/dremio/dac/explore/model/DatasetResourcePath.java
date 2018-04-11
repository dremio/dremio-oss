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

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.ResourcePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * "/dataset/{space.[folder.]*name}"
 *
 */
public class DatasetResourcePath extends ResourcePath {

  private final DatasetPath dataset;

  public DatasetResourcePath(DatasetPath dataset) {
    this.dataset = dataset;
  }

  @JsonCreator
  public DatasetResourcePath(String pathString) {
    List<String> path = parse(pathString, "dataset");
    if (path.size() != 1) {
      throw new IllegalArgumentException("path should be of form: /dataset/{datasetPath}, found " + pathString);
    }
    this.dataset = new DatasetPath(path.get(0));
    if (this.dataset.getRootType() != RootEntity.RootType.HOME &&
      this.dataset.getRootType() != RootEntity.RootType.SPACE &&
      this.dataset.getRootType() != RootEntity.RootType.TEMP) {
      throw new IllegalArgumentException("file path does not belong to a home or a space, " + pathString);
    }
  }

  @Override
  public List<String> asPath() {
    return asList("dataset", dataset.toPathString());
  }

  public DatasetPath getDataset() {
    return dataset;
  }

  public DatasetVersionResourcePath appendVersion(DatasetVersion version) {
    return new DatasetVersionResourcePath(dataset, version);
  }

  public DatasetVersionResourcePath appendVersion(String version) {
    return appendVersion(new DatasetVersion(version));
  }

  @JsonValue
  @Override
  public String toString() {
    // the json value should be a valid URL
    return dataset.getUrlFromPath(asPath());
  }
}
