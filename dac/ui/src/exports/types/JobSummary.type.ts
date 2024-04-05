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

import { SqlError } from "@app/pages/ExplorePage/components/SqlEditor/utils/errorUtils";

type FailureInfo = {
  errors: SqlError[];
  message?: string; // not returned for cancelled jobs
  type: string;
};

// only has currently used properties, update as needed
export type JobSummary = {
  accelerated: boolean;
  cancellationInfo?: {
    message: string;
  };
  description: string;
  failureInfo: FailureInfo;
  id: string;
  outputLimited: boolean;
  outputRecords: number;
  spilled: boolean;
  startTime: number;
  state: string;
} & (
  | {
      endTime: number;
      isComplete: true;
    }
  | { isComplete: false }
);
