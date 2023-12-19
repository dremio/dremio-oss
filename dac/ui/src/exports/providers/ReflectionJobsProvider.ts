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

import { withRouter, WithRouterProps } from "react-router";
import { parseQueryState } from "dremio-ui-common/utilities/jobs.js";
import { JobsQueryParams } from "dremio-ui-common/types/Jobs.types";
import { DefaultJobQuery } from "@app/pages/JobsPage/jobs-page-utils";
import { PaginatedReflectionJobsResource } from "../resources/ReflectionJobsResource";
import { debounce } from "lodash";

const debouncedFetch = debounce(
  (reflectionId: string, pagesRequested: number, query: JobsQueryParams) =>
    PaginatedReflectionJobsResource.fetch(reflectionId, pagesRequested, query),
  500,
  {
    leading: true,
  }
);

/*
 *
 * Hook for jobs, based on query
 *
 */
const useReflectionJobs = (
  query: JobsQueryParams | undefined,
  reflectionId: string
) => {
  const jobs = useResourceSnapshot(PaginatedReflectionJobsResource);
  const hasMorePages = !!jobs.value?.next;
  const [pagesRequested, setPagesRequested] = useState(1);

  useEffect(() => {
    PaginatedReflectionJobsResource.reset();
  }, []);

  const getJobs = useCallback(() => {
    if (query) debouncedFetch(reflectionId, pagesRequested, query);
  }, [pagesRequested, query, reflectionId]);

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
    pagesRequested,
    loading: jobs.status === "pending",
  };
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
    pagesRequested,
    loading,
  }: {
    jobs: any;
    jobsErr: unknown;
    query: JobsQueryParams | undefined;
    loadNextPage: () => void;
    pagesRequested: number;
    loading: boolean;
  }) => JSX.Element;
} & WithRouterProps;

export const ReflectionJobsProvider = withRouter(
  (props: JobsPageProviderProps): JSX.Element => {
    const { router, location, params } = props;
    const { query: urlQuery = {}, pathname } = props.location;

    // Handle query precendence for URL/local storage
    useEffect(() => {
      if (!urlQuery.filters) {
        router.push({
          ...location,
          query: DefaultJobQuery,
          pathname: pathname,
        });
      }
    }, [urlQuery, router, pathname, location]);

    // Create query used for endpoint
    const curQuery = useMemo(() => {
      return urlQuery.filters ? parseQueryState(urlQuery) : undefined;
    }, [urlQuery]);

    // Get jobs
    const { jobs, jobsErr, loadNextPage, pagesRequested, loading } =
      useReflectionJobs(curQuery, params.reflectionId);

    return props.children({
      jobs: jobs,
      jobsErr,
      query: curQuery,
      loadNextPage,
      pagesRequested,
      loading,
    });
  }
);
