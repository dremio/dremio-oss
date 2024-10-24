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

import type { CatalogObjectMethods } from "../../interfaces/CatalogObject.js";
import type { CommunityDatasetProperties } from "../../interfaces/Dataset.js";
import { mappedType, pathString } from "./utils.js";

export class VersionedDataset implements CatalogObjectMethods {
  /**
   * @deprecated
   */
  readonly id: string;
  readonly path: string[];
  readonly type: CommunityDatasetProperties["type"];
  constructor(properties: any) {
    this.id = properties.id;
    this.path = properties.path;
    this.type = properties.type;
  }

  get name() {
    return this.path[this.path.length - 1]!;
  }

  pathString = pathString(() => this.path);

  get referenceType() {
    return this.type;
  }

  static fromResource(properties: any) {
    return new VersionedDataset({
      id: properties.id,
      path: properties.path,
      type: (mappedType as any)[properties.type],
    });
  }
}
