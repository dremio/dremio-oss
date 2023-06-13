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
import type { FeatureFlagResponse } from "../FeatureFlagResponse.type";
import { getFeatureFlagEnabledUrl } from "../getFeatureFlagEnabled";
import { flags } from "./flags";

const originalMockData = flags;
let mockData = originalMockData;

export const setMockData = (
  newMockData: Record<string, FeatureFlagResponse["entitlement"]>
) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

export const getFeatureFlagEnabledHandler = rest.get(
  decodeURIComponent(
    getFeatureFlagEnabledUrl(":featureId").replace(
      `//${window.location.host}`,
      ""
    )
  ),
  (req, res, ctx) => {
    const response: FeatureFlagResponse = {
      entitlement: mockData[req.params.featureId as string] || "ENABLED",
    };
    return res(ctx.delay(200), ctx.json(response));
  }
);
