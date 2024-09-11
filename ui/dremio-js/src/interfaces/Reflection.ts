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

import type { CatalogReference } from "./CatalogReference.js";

export type ReflectionProperties = {
  metrics: {
    chosenCount: number;
    consideredCount: number;
    currentSizeBytes: number;
    failureCount: number;
    matchedCount: number;
    outputRecords: number;
    totalSizeBytes: number;
  };
  createdAt: Date;
  dataset: CatalogReference;
  id: string;
  isArrowCachingEnabled: boolean;
  isCanAlter: boolean;
  isCanView: boolean;
  name: string;
  status: {
    availabilityStatus: "AVAILABLE" | "EXPIRED" | "INCOMPLETE" | "NONE";
    combinedStatus:
      | "CAN_ACCELERATE"
      | "CAN_ACCELERATE_WITH_FAILURES"
      | "CANNOT_ACCELERATE_INITIALIZING"
      | "CANNOT_ACCELERATE_MANUAL"
      | "CANNOT_ACCELERATE_SCHEDULED"
      | "DISABLED"
      | "EXPIRED"
      | "FAILED"
      | "INVALID"
      | "INCOMPLETE"
      | "REFRESHING";
    configStatus: "OK" | "INVALID";
    expiresAt: Date;
    isEnabled: boolean;
    lastFailureMessage: string | null;
    lastDataFetchAt: Date | null;
    lastRefreshDuration: Temporal.Duration | null;
    refreshMethod: "INCREMENTAL" | "FULL" | "NONE";
    refreshStatus: "GIVEN_UP" | "MANUAL" | "RUNNING" | "SCHEDULED";
  };
  type: "RAW" | "AGGREGATION";
  updatedAt: Date;
};

export type ReflectionInterface = ReflectionProperties;
