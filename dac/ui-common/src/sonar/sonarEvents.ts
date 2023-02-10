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
import { getTracingContext } from "../contexts/TracingContext";

export enum SonarEventIds {
  job_preview = "job_preview",
  job_run = "job_run",
  source_add_dialog_opened = "source_add_dialog_opened",
  source_add_complete = "source_add_complete",
}

export const sonarEvents = {
  jobPreview() {
    getTracingContext().appEvent(SonarEventIds.job_preview);
  },
  jobRun() {
    getTracingContext().appEvent(SonarEventIds.job_run);
  },
  sourceAddDialogOpened() {
    getTracingContext().appEvent(SonarEventIds.source_add_dialog_opened);
  },
  sourceAddComplete() {
    getTracingContext().appEvent(SonarEventIds.source_add_complete);
  },
} as const;
