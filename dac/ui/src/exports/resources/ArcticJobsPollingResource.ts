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

import { useResourceSnapshotDeep } from "./../utilities/useDeepResourceSnapshot";
import { listArcticCatalogJobs } from "../endpoints/ArcticCatalogJobs/listArcticCatalogJobs";
import { useEffect, useState } from "react";
// @ts-ignore
import { PollingResource } from "../utilities/PollingResource";
import { INIT_ARCTIC_JOBS_PAGE_SIZE } from "./ArcticCatalogJobsResource";
import { JobState } from "@app/exports/endpoints/ArcticCatalogJobs/ArcticCatalogJobs.type";

export const ActiveArcticJobStates: JobState[] = [
  JobState.QUEUED,
  JobState.SETUP,
  JobState.RUNNING,
  JobState.STARTING,
];

export const usePollingForActiveArcticJobs = (
  catalogId: string,
  hasActiveJobs: boolean,
  fetchPage: () => void
) => {
  const [activeJobs, activeJobsError] = useResourceSnapshotDeep(
    ArcticJobsPollingResource
  );
  const [isPolling, setIsPolling] = useState(false);

  const tearDown = () => {
    ArcticJobsPollingResource.stop();
    setIsPolling(false);
  };

  // End poll on unmount
  useEffect(() => {
    return () => tearDown();
  }, []);

  // Start poll
  useEffect(() => {
    ArcticJobsPollingResource.start(() => {
      return [
        {
          catalogId,
        },
      ];
    });
    setIsPolling(true);
  }, [catalogId]);

  // Refresh page when a job finishes
  useEffect(() => {
    if (activeJobs && activeJobs?.data?.length > 0) {
      fetchPage();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeJobs?.data?.length]);

  // End poll
  useEffect(() => {
    if (activeJobs?.data?.length === 0) {
      if (isPolling) fetchPage();
      tearDown();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeJobs?.data?.length, isPolling, hasActiveJobs]);

  return { activeJobs: activeJobs?.data, activeJobsError, isPolling };
};

export const ArcticJobsPollingResource = new PollingResource(
  (params) =>
    listArcticCatalogJobs({
      ...params,
      view: "FULL",
      maxResults: INIT_ARCTIC_JOBS_PAGE_SIZE,
      filter: `state in (list(${ActiveArcticJobStates.map(
        (state) => `"${state}"`
      ).join(", ")}))`,
    }),
  { pollingInterval: 5000 }
);
