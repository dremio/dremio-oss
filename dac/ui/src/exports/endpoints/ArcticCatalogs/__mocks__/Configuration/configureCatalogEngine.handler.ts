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
import { EngineWrite } from "../../Configuration/CatalogConfiguration.types";
import { getEngineConfigurationUrl } from "../../Configuration/utils";
import { sampleConfigWrite } from "./sampleConfigWrite";

const originalMockData = sampleConfigWrite;
let mockData = originalMockData;

export const setMockData = (newMockData: EngineWrite) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

export const configureCatalogEngine = rest.post(
  decodeURIComponent(
    getEngineConfigurationUrl(":catalogId").replace(
      `//${window.location.host}`,
      ""
    )
  ),
  async (req, res, ctx) => {
    await req.json();
    const newRecord: EngineWrite = {
      ...mockData,
    };
    return res(ctx.delay(1250), ctx.json(newRecord));
  }
);
