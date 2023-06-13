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

import { ReflectionSummary } from "../../src/sonar/reflections/ReflectionSummary.type";

export const scheduledReflection: ReflectionSummary = {
  createdAt: new Date("2023-03-22T16:34:52.637Z"),
  updatedAt: new Date("2023-03-22T16:34:52.637Z"),
  id: "c182cd16-5e14-4d55-bad8-703f05bcef8a",
  reflectionType: "AGGREGATION" as any,
  name: "Aggregation Reflection",
  currentSizeBytes: 0,
  outputRecords: -1,
  totalSizeBytes: 0,
  datasetId:
    '{"tableKey":["performance","star","time_dim_v"],"contentId":"0194e844-7c2c-457d-9ef0-6d4abde9399d","versionContext":{"type":"BRANCH","value":"main"}}',
  datasetType: "VIRTUAL_DATASET" as any,
  datasetPath: ["performance", "star", "time_dim_v"],
  status: {
    configStatus: "OK" as any,
    refreshStatus: "SCHEDULED" as any,
    availabilityStatus: "NONE" as any,
    combinedStatus: "CANNOT_ACCELERATE_SCHEDULED" as any,
    refreshMethod: "NONE" as any,
    failureCount: 0,
    lastDataFetchAt: null,
    expiresAt: null,
    lastRefreshDurationMillis: -1,
  },
  consideredCount: 0,
  matchedCount: 0,
  chosenCount: 0,
  chosenJobsLink:
    "/jobs?filters=%7B%22chr%22%3A%5B%22a8fecb50f307-8dab-55d4-41e5-61dc281c%22%5D%2C%22qt%22%3A%5B%22UI%22%2C%22EXTERNAL%22%2C%22ACCELERATION%22%5D%7D",
  isArrowCachingEnabled: false,
  isCanView: true,
  isCanAlter: true,
  isEnabled: true,
};

export const runningReflection: ReflectionSummary = {
  createdAt: new Date("2023-03-22T16:34:52.637Z"),
  updatedAt: new Date("2023-03-22T16:34:52.637Z"),
  id: "6137df2e-85ae-4b2e-8735-727f59ac96c6",
  reflectionType: "AGGREGATION" as any,
  name: "Aggregation Reflection",
  currentSizeBytes: 0,
  outputRecords: -1,
  totalSizeBytes: 0,
  datasetId:
    '{"tableKey":["performance","star","time_dim_v"],"contentId":"0194e844-7c2c-457d-9ef0-6d4abde9399d","versionContext":{"type":"BRANCH","value":"main"}}',
  datasetType: "VIRTUAL_DATASET" as any,
  datasetPath: ["performance", "star", "time_dim_v"],
  status: {
    configStatus: "OK" as any,
    refreshStatus: "RUNNING" as any,
    availabilityStatus: "NONE" as any,
    combinedStatus: "REFRESHING" as any,
    refreshMethod: "NONE" as any,
    failureCount: 0,
    lastDataFetchAt: null,
    expiresAt: null,
    lastRefreshDurationMillis: -1,
  },
  consideredCount: 0,
  matchedCount: 0,
  chosenCount: 0,
  chosenJobsLink:
    "/jobs?filters=%7B%22chr%22%3A%5B%22a8fecb50f307-8dab-55d4-41e5-61dc281c%22%5D%2C%22qt%22%3A%5B%22UI%22%2C%22EXTERNAL%22%2C%22ACCELERATION%22%5D%7D",
  isCanView: true,
  isCanAlter: true,
  isArrowCachingEnabled: false,
  isEnabled: true,
};

export const sampleReflections: ReflectionSummary[] = [
  scheduledReflection,
  runningReflection,
];
