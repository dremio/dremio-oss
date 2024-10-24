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
import { type SupportFlagResponse } from "../SupportFlagResponse.type";
import { getSupportFlagUrl } from "../getSupportFlag";
import { booleanSupportFlags } from "./supportFlags";

const originalMockData = booleanSupportFlags;
let mockData = originalMockData;

export const setMockData = (
  newMockData: Record<string, SupportFlagResponse<boolean>>,
) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

export const getSupportFlagHandler = rest.get(
  decodeURIComponent(
    getSupportFlagUrl(":featureId").replace(`//${window.location.host}`, ""),
  ),
  (req, res, ctx) => {
    const id = req.params.featureId as string;
    const response: SupportFlagResponse<boolean> = mockData[id] || {
      id,
      type: "BOOLEAN",
      value: false, // Not mocked, disable by default
    };
    return res(ctx.delay(200), ctx.json(response));
  },
);
