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
import { GetResponse } from "../ArcticCatalogPrivileges.types";
import { getArcticCatalogPrivilegesUrl } from "../utils";
import { sampleArcticCatalogPrivilegesResponse } from "./sampleArcticCatalogPrivilegesResponse";

const originalMockData = sampleArcticCatalogPrivilegesResponse;
let mockData = originalMockData;

export const setMockData = (newMockData: GetResponse) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

export const getArcticCatalogPrivilegesHandler = rest.get(
  decodeURIComponent(
    getArcticCatalogPrivilegesUrl(":arcticCatalogId").replace(
      `//${window.location.host}`,
      ""
    )
  ),
  (_req, res, ctx) => {
    return res(ctx.delay(500), ctx.json(mockData));
  }
);
