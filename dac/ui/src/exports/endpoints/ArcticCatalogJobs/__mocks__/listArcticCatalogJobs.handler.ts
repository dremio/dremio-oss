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

import { INIT_ARCTIC_JOBS_PAGE_SIZE } from "@app/exports/resources/ArcticCatalogJobsResource";
import { rest } from "msw";
import type { ArcticCatalogJobsResponse } from "../ArcticCatalogJobs.type";
import { listArcticCatalogJobsUrl } from "../listArcticCatalogJobs";
import { sampleArcticCatalogJobs } from "./sampleArcticCatalogJobs";

const originalMockData = sampleArcticCatalogJobs;
let mockData = originalMockData;

export const setMockData = (newMockData: ArcticCatalogJobsResponse) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

export const listArcticCatalogJobsHandler = rest.get(
  listArcticCatalogJobsUrl({
    catalogId: ":catalogId",
    maxResults: INIT_ARCTIC_JOBS_PAGE_SIZE,
    view: "FULL",
  }).replace(`//${window.location.host}`, ""),
  (_req, res, ctx) => {
    const isSecondPage = _req.url.search.includes("secondPage");
    return res(
      ctx.delay(750),
      ctx.json({
        data: isSecondPage
          ? mockData.data
              .filter((_: any, i: number) => i >= 20)
              .map((job: any) => ({
                ...job,
                id:
                  (Math.random() + 1).toString(36).substring(7) +
                  job.id.substring(3, job.id.length - 1),
              }))
          : mockData.data,
        nextPageToken: isSecondPage ? "" : mockData.nextPageToken,
      })
    );
  }
);
