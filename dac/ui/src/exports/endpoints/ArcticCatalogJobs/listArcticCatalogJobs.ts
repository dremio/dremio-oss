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
// @ts-ignore
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { ArcticCatalogJobsResponse } from "./ArcticCatalogJobs.type";

type ListArcticCatalogJobsParams = {
  catalogId: string;
  maxResults: number;
  pageToken?: string;
  filter?: string;
  view?: "SUMMARY" | "FULL";
};

export const listArcticCatalogJobsUrl = (params: ListArcticCatalogJobsParams) =>
  new APIV3Call()
    .projectScope(false)
    .paths(`arctic/catalogs/${params.catalogId}/jobs`)
    .params({ maxResults: params.maxResults })
    .params(params?.pageToken ? { pageToken: params.pageToken } : {})
    .params(params?.filter ? { filter: params?.filter } : {})
    .params({ view: params?.view ?? "FULL" })
    .toString();

export const listArcticCatalogJobs = async (
  params: ListArcticCatalogJobsParams
): Promise<ArcticCatalogJobsResponse> => {
  return getApiContext()
    .fetch(listArcticCatalogJobsUrl(params))
    .then((res: any) => res.json());
};
