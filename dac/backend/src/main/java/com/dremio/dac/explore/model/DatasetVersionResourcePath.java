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
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * The resource path to a given version of a dataset
 *
 */
public final class DatasetVersionResourcePath extends ResourcePath {
  private final DatasetPath dataset;
  private final DatasetVersion version;

  public DatasetVersionResourcePath(DatasetPath dataset, DatasetVersion version) {
    this.dataset = dataset;
    this.version = version;
  }

  @JsonCreator
  public DatasetVersionResourcePath(String path) {
    List<String> parse = parse(path, "dataset", "version");
    this.dataset = new DatasetPath(parse.get(0));
    this.version = new DatasetVersion(parse.get(1));
  }

  public DatasetPath getDataset() {
    return dataset;
  }

  public DatasetVersion getVersion() {
    return version;
  }

  @Override
  public List<?> asPath() {
    return asList("dataset", dataset, "version", version);
  }

  public DatasetResourcePath truncate() {
    return new DatasetResourcePath(dataset);
  }

  public DatasetVersionResourcePath ammendVersion(DatasetVersion nextVersion) {
    return new DatasetVersionResourcePath(this.dataset, nextVersion);
  }

}
