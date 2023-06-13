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

import { rest } from "msw";
import {
  JobInfo as ArcticCatalogJobResponse,
  JobInfoStateEnum,
} from "../ArcticCatalogJobs.type";
import { getArcticCatalogJobUrl } from "../getArcticCatalogJob";
import { sampleArcticCatalogJob } from "./sampleArcticCatalogJob";

const originalMockData = sampleArcticCatalogJob;
let mockData = originalMockData;

export const setMockData = (newMockData: ArcticCatalogJobResponse) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

// currently polling on this endpoint
let counter = 0;

export const getArcticCatalogJobHandler = rest.get(
  getArcticCatalogJobUrl({
    catalogId: ":catalogId",
    jobId: ":jobId",
  }).replace(`//${window.location.host}`, ""),
  (_req, res, ctx) => {
    counter++;
    if (counter === 3) {
      counter = 0;
      return res(
        ctx.delay(750),
        ctx.json({ ...mockData, state: JobInfoStateEnum.COMPLETED })
      );
    } else return res(ctx.delay(750), ctx.json(mockData));
  }
);
