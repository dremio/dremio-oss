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
import { SonarEventIds } from "dremio-ui-common/sonar/sonarEvents.js";

export const intercomEventIds = {
  [SonarEventIds.job_run]: "Job: Run",
  [SonarEventIds.job_preview]: "Job: Preview",
  [SonarEventIds.source_add_dialog_opened]: "Source: Add",
  [SonarEventIds.source_add_complete]: "Source: Add Complete",
} as const;
