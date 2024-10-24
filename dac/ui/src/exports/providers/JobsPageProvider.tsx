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

import { useCallback, useEffect, useMemo, useState } from "react";
import { useResourceSnapshot } from "smart-resource1/react";

import {
  JobsPollingResource,
  PaginatedJobsResource,
  jobsCache,
} from "../resources/JobsListingResource";
import { withRouter, WithRouterProps } from "react-router";
import {
  parseQueryState,
  getUpdatedStartTimeForQuery,
  isStartTimeOutdated,
} from "dremio-ui-common/utilities/jobs.js";
import localStorageUtils from "#oss/utils/storageUtils/localStorageUtils";
import { JobsQueryParams } from "dremio-ui-common/types/Jobs.types";
import { debounce, isEqual } from "lodash";
import {
  isRunningJob,
  DefaultJobQuery,
  getPageIndexOfCachedJob,
  hasOnlyFrontendFilters,
} from "#oss/pages/JobsPage/jobs-page-utils";
import { usePrevious } from "#oss/utils/jobsUtils";
import { useResourceSnapshotDeep } from "../utilities/useDeepResourceSnapshot";
import { useResourceStatus } from "smart-resource/react";
import { Intervals } from "#oss/pages/JobPage/components/JobsFilters/StartTimeSelect/utils";

const debouncedFetch = debounce(
  (pagesRequested: number, query: JobsQueryParams) =>
    PaginatedJobsResource.fetch(pagesRequested, query),
  500,
  {
    leading: true,
  },
);

/*
 *
 * Hook for jobs, based on filter/query
 *
 */
const useJobs = (query: JobsQueryParams | undefined, location: any) => {
  const jobFromDetails = location?.state?.jobFromDetails;
  const jobs = useResourceSnapshot(PaginatedJobsResource);
  const hasMorePages = !!jobs.value?.next;
  const [pagesRequested, setPagesRequested] = useState(
    jobFromDetails
      ? getPageIndexOfCachedJob(jobs?.value?.jobs || [], jobFromDetails)
      : 1,
  );

  useEffect(() => {
    if (!location?.state?.jobFromDetails) {
      jobsCache.clear();
      PaginatedJobsResource.reset();
    }
  }, [location, query]);

  const getJobs = useCallback(() => {
    if (query) debouncedFetch(pagesRequested, query);
  }, [pagesRequested, query]);

  useEffect(() => {
    getJobs();
  }, [getJobs]);

  return {
    jobs: jobs.value?.jobs,
    jobsErr: jobs.error,
    loadNextPage: useCallback(() => {
      if (jobs.status !== "success" || !hasMorePages) {
        return;
      }
      setPagesRequested((x) => x + 1);
    }, [hasMorePages, jobs.status]),
    loading: jobs.status === "pending",
    pagesRequested,
  };
};

/*
 *
 * Hook for running jobs in the current page
 *
 */
const useRunningJobs = (jobs: any[]) => {
  const runningJobIds = useMemo(
    () =>
      (jobs || [])
        .filter((job: any) => isRunningJob(job.state))
        .map((job: any) => job.id),
    [jobs],
  );

  const previousRunningJobIds = usePrevious(runningJobIds);

  useEffect(() => {
    if (!isEqual(runningJobIds, previousRunningJobIds)) {
      JobsPollingResource.reset();

      if (!runningJobIds.length) {
        return;
      }
      JobsPollingResource.start(() => [{ jobIds: runningJobIds }]);
    }
  }, [runningJobIds, previousRunningJobIds]);

  useEffect(() => JobsPollingResource.reset.bind(JobsPollingResource), []);

  const [result] = useResourceSnapshotDeep(JobsPollingResource);
  const status = useResourceStatus(JobsPollingResource);

  useEffect(() => {
    if (status === "initial" || status === "pending") return;

    const someJobsRunning = (result?.jobs || []).some((job: any) =>
      isRunningJob(job.state),
    );
    if (!someJobsRunning) {
      JobsPollingResource.stop.bind(JobsPollingResource)();
    }
  }, [result, status]);

  return useMemo(
    () =>
      (result?.jobs || []).reduce((acc: any, cur: any) => {
        acc.set(cur.id, cur);
        return acc;
      }, new Map<string, any>()),
    [result],
  );
};

/*
 *
 * Jobs page provider
 *
 */
type JobsPageProviderProps = {
  children: ({
    jobs,
    jobsErr,
    query,
    loadNextPage,
    runningJobs,
    pagesRequested,
    loading,
  }: {
    jobs: any;
    jobsErr: unknown;
    query: JobsQueryParams | undefined;
    loadNextPage: () => void;
    runningJobs: any;
    pagesRequested: number;
    loading: boolean;
  }) => JSX.Element;
} & WithRouterProps;

export const JobsPageProvider = withRouter(
  (props: JobsPageProviderProps): JSX.Element => {
    const { router, location } = props;
    const { query: urlQuery = {}, pathname } = props.location;

    // Handle query precendence for URL/local storage
    useEffect(() => {
      const storageQuery = localStorageUtils?.getJobsFiltersState();
      const allowStorage = hasOnlyFrontendFilters(urlQuery);
      // Used to reset the date-time filter if it has a Last X... value
      const selectedStartTimeType = localStorageUtils?.getJobsDateTimeFilter();
      const selectedStartTime = Intervals().find(
        (item) => item.type === selectedStartTimeType,
      );

      if (!storageQuery && !urlQuery.filters) {
        // Handle init case, no localstorage/URL params
        if (allowStorage)
          localStorageUtils?.setJobsFilters(JSON.stringify(DefaultJobQuery));
        router.replace({
          ...location,
          query: DefaultJobQuery,
          pathname: pathname,
        });
      } else if (!urlQuery.filters && storageQuery) {
        // Handle case where local storage takes precendence
        const updatedQuery = getUpdatedStartTimeForQuery(
          JSON.parse(storageQuery),
          selectedStartTime,
        );
        router.replace({
          ...location,
          query: updatedQuery,
          pathname: pathname,
        });
      } else if (urlQuery.filters) {
        // Otherwise, use URL params
        if (isStartTimeOutdated(urlQuery, selectedStartTime)) {
          const updatedQuery = getUpdatedStartTimeForQuery(
            urlQuery,
            selectedStartTime,
          );
          router.replace({
            ...location,
            query: updatedQuery,
            pathname: pathname,
          });
        } else if (allowStorage)
          localStorageUtils?.setJobsFilters(JSON.stringify(urlQuery));
      }
    }, [urlQuery, router, pathname, location]);

    // Create query used for endpoint
    const curQuery = useMemo(() => {
      return urlQuery?.filters ? parseQueryState(urlQuery) : undefined;
    }, [urlQuery]);

    // Get jobs
    const { jobs, jobsErr, loadNextPage, loading, pagesRequested } = useJobs(
      curQuery,
      location,
    );

    // Get running jobs
    const runningJobs = useRunningJobs(jobs);

    return props.children({
      jobs,
      jobsErr,
      query: curQuery,
      loadNextPage,
      runningJobs,
      loading,
      pagesRequested,
    });
  },
);
