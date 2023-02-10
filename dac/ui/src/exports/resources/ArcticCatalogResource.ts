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

import { SmartResource } from "smart-resource";
import { type ArcticCatalog } from "../endpoints/ArcticCatalogs/ArcticCatalog.type";
import { getArcticCatalog } from "../endpoints/ArcticCatalogs/getArcticCatalog";

const catalogResources = new Map<
  string,
  SmartResource<Promise<ArcticCatalog>>
>();

export const getArcticCatalogResource = (catalogId: string) => {
  if (!catalogResources.has(catalogId)) {
    catalogResources.set(
      catalogId,
      new SmartResource(() => getArcticCatalog({ catalogId }))
    );
  }

  return catalogResources.get(catalogId) as SmartResource<
    Promise<ArcticCatalog>
  >;
};
