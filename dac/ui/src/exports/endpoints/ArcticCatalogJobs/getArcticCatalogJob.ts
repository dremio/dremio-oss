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
import { JobInfo as ArcticCatalogJobResponse } from "./ArcticCatalogJobs.type";

type GetArcticCatalogJobParams = {
  catalogId: string;
  jobId: string;
};

export const getArcticCatalogJobUrl = (params: GetArcticCatalogJobParams) =>
  new APIV3Call()
    .projectScope(false)
    .paths(`arctic/catalogs/${params.catalogId}/jobs/${params.jobId}`)
    .toString();

export const getArcticCatalogJob = async (
  params: GetArcticCatalogJobParams
): Promise<ArcticCatalogJobResponse> => {
  return getApiContext()
    .fetch(getArcticCatalogJobUrl(params))
    .then((res: any) => res.json());
};
