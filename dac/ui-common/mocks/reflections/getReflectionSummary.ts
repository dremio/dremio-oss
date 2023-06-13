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
  ReflectionSummaries,
  ReflectionSummary,
} from "../../src/sonar/reflections/ReflectionSummary.type";
import { sampleReflections } from "./sampleReflections";

const originalMockData = sampleReflections;
let mockData = originalMockData;

export const setMockData = (newMockData: ReflectionSummary[]) => {
  mockData = newMockData;
};

export const restoreMockData = () => {
  mockData = originalMockData;
};

export const getReflectionSummary = rest.get(
  "/v0/projects/:projectId/reflection-summary",
  (_req, res, ctx) => {
    const response: ReflectionSummaries = {
      canAlterReflections: true,
      data: mockData,
    };
    return res(ctx.delay(150), ctx.json(response));
  }
);
