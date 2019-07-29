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
package com.dremio.dac.model.common;

import java.util.List;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.file.FilePath;
import com.dremio.file.SourceFilePath;
import com.dremio.service.namespace.dataset.proto.DatasetType;

/**
 * Utility methods for namespace paths.
 */
public final class NamespacePathUtils {

  /** helper method that returns root entity NamespacePath for the given datasetType */
  public static NamespacePath getNamespacePathForDataType(DatasetType datasetType, List<String> path) {
    switch (datasetType) {
    case VIRTUAL_DATASET:
      return new DatasetPath(path);
    case PHYSICAL_DATASET:
      return new PhysicalDatasetPath(path);
    case PHYSICAL_DATASET_SOURCE_FILE:
      return new SourceFilePath(path);
    case PHYSICAL_DATASET_SOURCE_FOLDER:
      return new SourceFolderPath(path);
    case PHYSICAL_DATASET_HOME_FILE:
      return new FolderPath(path);
    case PHYSICAL_DATASET_HOME_FOLDER:
      return new FilePath(path);
    default:
      throw new RuntimeException("Invalid dataset type");
    }
  }
}

