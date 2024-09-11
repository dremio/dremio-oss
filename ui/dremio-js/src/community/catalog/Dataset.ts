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

import type {
  CommunityDataset,
  CommunityDatasetProperties,
} from "../../interfaces/Dataset.js";

import { mappedType, pathString } from "./utils.js";

export class Dataset implements CommunityDataset {
  readonly createdAt: Date;
  readonly id: string;
  readonly fields: any;
  readonly owner: any;
  readonly path: string[];
  readonly type: CommunityDatasetProperties["type"];
  constructor(properties: CommunityDatasetProperties) {
    this.createdAt = properties.createdAt;
    this.id = properties.id;
    this.fields = properties.fields;
    this.owner = properties.owner;
    this.path = properties.path;
    this.type = properties.type;
  }

  get name(): string {
    return this.path[this.path.length - 1]!;
  }

  pathString = pathString(() => this.path);

  static fromResource(datasetEntity: any): Dataset {
    return new Dataset({
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
    });
  }
}
