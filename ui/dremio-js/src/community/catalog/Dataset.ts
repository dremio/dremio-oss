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
import type {
  CommunityDataset,
  CommunityDatasetProperties,
} from "../../interfaces/Dataset.js";
import { DatasetCatalogReference } from "./CatalogReference.js";

import { mappedType, pathString } from "./utils.js";

export class Dataset implements CommunityDataset {
  readonly catalogReference: DatasetCatalogReference;
  readonly createdAt: Date;
  /**
   * @deprecated
   */
  readonly id: string;
  readonly fields: any;
  readonly owner: any;
  /**
   * @deprecated
   */
  readonly path: string[];
  readonly type: CommunityDatasetProperties["type"];
  // eslint-disable-next-line no-unused-private-class-members
  readonly #config: SonarV3Config;
  constructor(
    properties: CommunityDatasetProperties & {
      catalogReference: DatasetCatalogReference;
    },
    config: SonarV3Config,
  ) {
    this.catalogReference = properties.catalogReference;
    this.createdAt = properties.createdAt;
    this.id = properties.id;
    this.fields = properties.fields;
    this.owner = properties.owner;
    this.path = properties.path;
    this.type = properties.type;
    this.#config = config;
  }

  /**
   * @deprecated
   */
  get name(): string {
    return this.path[this.path.length - 1]!;
  }

  /**
   * @deprecated
   */
  pathString = pathString(() => this.path);

  /**
   * @deprecated
   */
  get referenceType() {
    return this.type;
  }

  static fromResource(datasetEntity: any, config: SonarV3Config): Dataset {
    return new Dataset(
      {
        catalogReference: new DatasetCatalogReference({
          id: datasetEntity.id,
          path: datasetEntity.path,
          type: (mappedType as any)[datasetEntity.type],
        }),
        createdAt: new Date(datasetEntity.createdAt),
        fields: datasetEntity.fields,
        id: datasetEntity.id,
        owner: datasetEntity.owner
          ? {
              id: datasetEntity.owner.ownerId,
              type: datasetEntity.owner.ownerType,
            }
          : undefined,
        path: datasetEntity.path,
        type: (mappedType as any)[datasetEntity.type],
      },
      config,
    );
  }
}
