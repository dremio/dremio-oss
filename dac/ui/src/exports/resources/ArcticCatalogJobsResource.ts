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

import { listArcticCatalogJobs } from "../endpoints/ArcticCatalogJobs/listArcticCatalogJobs";
import { useEffect } from "react";
// @ts-ignore
import { usePaginated } from "dremio-ui-common/utilities/usePaginated.js";
import { SmartResource } from "smart-resource";
import {
  ActiveArcticJobStates,
  usePollingForActiveArcticJobs,
} from "./ArcticJobsPollingResource";

export const INIT_ARCTIC_JOBS_PAGE_SIZE = 20;

export const useArcticCatalogJobs = (catalogId: string, filter: string) => {
  const { fetchPage, pages } = usePaginated(listArcticCatalogJobs);
  const lastPage = pages.length > 0 && pages[pages.length - 1];
  const loadingMore = lastPage?.status === "PENDING";

  const hasMorePages =
    !lastPage ||
    (lastPage.status !== "SUCCESS" && lastPage.status !== "ERROR") ||
    !!lastPage?.data?.nextPageToken;

  const nextPageToken = lastPage?.data?.nextPageToken;

  const resetToInitialPage = () =>
    fetchPage(undefined, {
      catalogId: catalogId,
      maxResults: INIT_ARCTIC_JOBS_PAGE_SIZE,
      pageToken: undefined,
      filter: filter ?? "",
    });

  const loadNextPage = () => {
    if (loadingMore || !hasMorePages || nextPageToken === "error") {
      return;
    }
    fetchPage(nextPageToken, {
      catalogId: catalogId,
      maxResults: INIT_ARCTIC_JOBS_PAGE_SIZE,
      pageToken: nextPageToken,
      filter: filter ?? "",
    });
  };

  useEffect(() => {
    resetToInitialPage();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filter]);

  const jobs: any[] = pages
    .flatMap((page: any) => {
      if (page.status !== "SUCCESS") {
        return null;
      }
      return page.data.data;
    })
    .filter(Boolean);
  const hasActiveJobs =
    jobs.filter((j) => ActiveArcticJobStates.includes(j.state)).length > 0;

  const { isPolling } = usePollingForActiveArcticJobs(
    catalogId,
    hasActiveJobs,
    resetToInitialPage
  );

  return {
    jobs,
    isPolling,
    loadNextPage,
    loadingMore,
  };
};

export const ArcticJobsResource = new SmartResource(
  (catalogId, filter, maxResults) =>
    listArcticCatalogJobs({
      catalogId,
      maxResults,
      filter,
    })
);
