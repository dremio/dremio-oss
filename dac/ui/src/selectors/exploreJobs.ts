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
import { getLocation } from "selectors/routing";
import { getJobList } from "./jobs";
import { getDatasetVersionFromLocation, getFullDataset } from "./explore";

// This was causing problems in the mocha tests, moving to it's own module
export function getExploreJobId(state: Record<string, any>) {
  // this selector will have to change once we move jobId out of fullDataset and load it prior to metadata
  const location = getLocation(state);
  const version = getDatasetVersionFromLocation(location);
  const fullDataset = getFullDataset(state, version);
  const jobIdFromDataset = fullDataset?.getIn(["jobId", "id"]);

  // a dataset is not returned in the first response of the new_tmp_untitled_sql endpoints
  // so we need to get jobId from the jobList where the last job is the most recently submitted
  const jobListArray = getJobList(state).toArray();
  const jobIdFromList =
    jobListArray?.[jobListArray.length - 1]?.get("id") ?? "";

  return jobIdFromDataset !== jobIdFromList && jobIdFromList
    ? jobIdFromList
    : jobIdFromDataset;
}
