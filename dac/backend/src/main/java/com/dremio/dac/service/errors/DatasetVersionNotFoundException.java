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
package com.dremio.dac.service.errors;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * Thrown when a version is not found for a given dataset
 */
public class DatasetVersionNotFoundException extends NotFoundException {
  private static final long serialVersionUID = 1L;

  private final DatasetPath datasetPath;
  private final DatasetVersion version;

  public DatasetVersionNotFoundException(DatasetPath datasetPath, DatasetVersion version, Exception error) {
    super(new DatasetVersionResourcePath(datasetPath, version), "dataset " + datasetPath.toPathString() + " version " + version, error);
    this.datasetPath = datasetPath;
    this.version = version;
  }

  public DatasetVersionNotFoundException(DatasetPath datasetPath, DatasetVersion version)  {
    super(new DatasetVersionResourcePath(datasetPath, version), "dataset " + datasetPath.toPathString() + " version " + version);
    this.datasetPath = datasetPath;
    this.version = version;
  }

  public DatasetPath getDatasetPath() {
    return datasetPath;
  }

  public DatasetVersion getVersion() {
    return version;
  }
}
