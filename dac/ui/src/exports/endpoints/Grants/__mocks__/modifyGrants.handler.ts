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
import { GrantsResponse } from "../Grants.types";
import { getGrantsUrl } from "../utils";
import { sampleGrantsResponsePUT } from "./sampleGrantsResponse";

const originalMockData = sampleGrantsResponsePUT;
let mockData = originalMockData;

export const setMockData = (newMockData: GrantsResponse) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

export const modifyGrantsHandler = rest.put(
  decodeURIComponent(getGrantsUrl().replace(`//${window.location.host}`, "")),
  async (req, res, ctx) => {
    return res(ctx.delay(1250), ctx.json(mockData));
  },
);
