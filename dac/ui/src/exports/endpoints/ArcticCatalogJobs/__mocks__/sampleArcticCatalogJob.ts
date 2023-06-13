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
import {
  JobInfo,
  EngineSize,
  JobState,
  JobInfoTypeEnum,
  OptimizeJobInfo,
} from "../ArcticCatalogJobs.type";

export const sampleArcticCatalogJob: JobInfo = {
  catalogId: "84ce6ad2-649d-4c3e-82d7-18748aee1dd7",
  startedAt: new Date("2022-12-15T21:53:25.134Z"),
  id: "1c82c8db-005b-df1f-cd14-12c02ba14500",
  state: JobState.RUNNING,
  type: JobInfoTypeEnum.OPTIMIZE,
  username: "grace.johnson@dremio.com",
  engineSize: EngineSize.MEDIUMV1,
  metrics: {
    rewrittenDataFiles: 20,
    newDataFiles: 20,
  },
  config: {
    maxFileSize: "128 MB",
    minFileSize: "64 MB",
    minFiles: 3,
    reference: "main",
    tableId: 'Samples."samples.dremio.com"."NYC-taxi-trips-iceberg"',
    targetFileSize: "96 MB",
  },
} as OptimizeJobInfo;
