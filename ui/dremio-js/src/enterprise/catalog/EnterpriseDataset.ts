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
import type { DatasetCatalogReference } from "../../community/catalog/CatalogReference.js";
import { Dataset } from "../../community/catalog/Dataset.js";
import type {
  CommunityDatasetProperties,
  EnterpriseDataset as EnterpriseDatasetInterface,
} from "../../interfaces/Dataset.js";

export class EnterpriseDataset
  extends Dataset
  implements EnterpriseDatasetInterface
{
  readonly #config: SonarV3Config;

  constructor(
    properties: CommunityDatasetProperties & {
      catalogReference: DatasetCatalogReference;
    },
    config: SonarV3Config,
  ) {
    super(properties, config);
    this.#config = config;
  }

  async grants(): ReturnType<EnterpriseDatasetInterface["grants"]> {
    return this.#config
      .sonarV3Request(`catalog/${this.id}/grants`)
      .then((res) => res.json())
      .then((entity) => ({
        availablePrivileges: entity.availablePrivileges,
        grants: entity.grants.map((grantEntity: any) => ({
          grantee: { id: grantEntity.id, type: grantEntity.granteeType },
          privileges: grantEntity.privileges,
        })),
      }));
  }
}
