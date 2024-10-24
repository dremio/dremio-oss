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

import moize from "moize";
import { DefaultApi, GetEntriesRequest, V2BetaApi } from "../client";
import SwaggerConfig, {
  createSwaggerConfig,
  createSwaggerV2Config,
} from "./SwaggerConfig";
import { DremioV2Api } from "#oss/types/nessie";

//Use default Atlantis project API
const TreeApi = new DefaultApi(SwaggerConfig);

//Get and cache (moize) endpoint-specific API (empty endpoint = default Atlantis API)
export const getTreeApi = moize(
  function (endpoint?: string) {
    return new DefaultApi(
      createSwaggerConfig(endpoint?.replace("/nessie/", "/nessieV1/")),
    );
  },
  {
    maxSize: 10,
  },
);

export const getApiV2 = moize(
  function (endpoint?: string): DremioV2Api {
    // return new V2BetaApi(createSwaggerV2Config(endpoint));
    const v2Api = new V2BetaApi(
      createSwaggerV2Config(endpoint),
    ) as unknown as DremioV2Api;
    v2Api.memoGetDefaultReference = moize.promise(
      () => v2Api.getReferenceByNameV2({ ref: "-" }),
      {
        isDeepEqual: true,
        maxSize: 1000,
      },
    );
    return v2Api;
  },
  {
    maxSize: 50,
  },
);

export function getEntries(req: GetEntriesRequest) {
  return TreeApi.getEntries(req);
}

export default TreeApi;
