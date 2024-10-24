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

import type { SonarV3Config } from "../../_internal/types/Config.js";
import type { BaseCatalogReferenceProperties } from "../../interfaces/CatalogReference.js";
import {
  DatasetCatalogReference,
  FileCatalogReference,
  FolderCatalogReference,
  FunctionCatalogReference,
  HomeCatalogReference,
  SourceCatalogReference,
  SpaceCatalogReference,
} from "./CatalogReference.js";

export const catalogReferenceFromProperties = (
  properties: BaseCatalogReferenceProperties & { type: string },
  config: SonarV3Config,
) => {
  switch (properties.type) {
    case "DATASET_DIRECT":
    case "DATASET_PROMOTED":
    case "DATASET_VIRTUAL":
      return new DatasetCatalogReference(properties as any);
    case "FILE":
      return new FileCatalogReference(properties);
    case "FOLDER":
      return new FolderCatalogReference(properties, config);
    case "FUNCTION":
      return new FunctionCatalogReference(properties);
    case "HOME":
      return new HomeCatalogReference(properties, config);
    case "SOURCE":
      return new SourceCatalogReference(properties, config);
    case "SPACE":
      return new SpaceCatalogReference(properties, config);
    default:
      throw new Error("Unknown catalogReference type: " + properties.type);
  }
};
