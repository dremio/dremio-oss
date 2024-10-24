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

import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { APIV2Call } from "#oss/core/APICall";
// import moize from "moize";

export const getSonarContentsUrl = (params: GetArcticCatalogParams) =>
  new APIV2Call()
    .projectScope(true) // This should be included
    .paths(params.path)
    .params(params.params || {})
    .uncachable()
    .toString();

type GetArcticCatalogParams = {
  path: string;
  params?: { [idx: string]: string };
  entityType: string;
};

// DX-89807: Moize is not invalidating the cancel request from HomeContents.js
export const getSonarContentsResponse = //moize.promise(
  (params: GetArcticCatalogParams, init?: RequestInit): Promise<any> =>
    getApiContext().fetch(getSonarContentsUrl(params), init);
// {
//   isDeepEqual: true,
//   maxAge: 1000,
// }
// );

/**
 *
 * @param params - Path is the catalog path
 * @returns - Contents for the Dremio Catalog (Called `home` in redux and rendered by `HomeContents`)
 */
export const getSonarContents = (
  params: GetArcticCatalogParams,
  init?: RequestInit,
): Promise<any> =>
  getSonarContentsResponse(params, init).then((res: any) => res.clone().json());
