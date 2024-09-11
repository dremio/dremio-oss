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
export type JobDetails = {
  attemptDetails: Record<string, number | string>[];
  cancellationInfo?: {
    message: string;
  };
  datasetPaths: string[];
  datasetVersion: string;
  description: string;
  duration: number;
  engine?: string;
  failureInfo: FailureInfo;
  id: string;
  isComplete: boolean;
  isOutputLimited: boolean;
  jobStatus: string;
  outputRecords: number;
  queryType: "UI_RUN" | "UI_PREVIEW"; // only two types when jobs are run from the SQL runner
};
