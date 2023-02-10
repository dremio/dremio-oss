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
import { projectBase } from "./common";

type JobIdParam = { jobId: string };
type OldJobIdParam = { jobid: string };
type FilterParams = {
  filters?: Record<string, any> | string;
  order?: "ASCENDING" | "DESCENDING";
  sort?: string;
};
type ReflectionParams = {
  reflectionId: string;
};

/** Jobs  */
export const jobs = projectBase.extend((params: FilterParams) => {
  const searchParams = new URLSearchParams();
  if (params.filters && params.filters !== "__INTERNAL_PARAM_IDENTIFIER__") {
    searchParams.set("filters", JSON.stringify(params.filters));
  }
  const searchParamStr = searchParams.toString();
  return `jobs${searchParamStr ? `?${searchParamStr}` : ""}`;
});
export const job = jobs.extend(
  (params: JobIdParam) => `job/${encodeURIComponent(params.jobId)}`
);
export const reflection = jobs.extend(
  (params: ReflectionParams) => `reflection/${params.reflectionId}`
);

/** Old Job  */
export const jobOld = jobs.extend(
  (params: OldJobIdParam) => `job/${params.jobid}`
);
