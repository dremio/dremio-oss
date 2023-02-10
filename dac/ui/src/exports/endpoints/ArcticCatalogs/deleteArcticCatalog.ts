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

import { APIV3Call } from "@app/core/APICall";
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";

export const deleteArcticCatalogUrl = (catalogId: string) =>
  new APIV3Call()
    .projectScope(false)
    .paths(`arctic/catalogs/${catalogId}`)
    .toString();

type DeleteArcticCatalogParams = {
  catalogId: string;
};

export const deleteArcticCatalog = (
  params: DeleteArcticCatalogParams
): Promise<void> =>
  getApiContext()
    .fetch(deleteArcticCatalogUrl(params.catalogId), {
      method: "delete",
    })
    .then(() => undefined);
