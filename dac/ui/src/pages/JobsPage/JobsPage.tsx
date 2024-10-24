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

import { useCallback, useLayoutEffect, useMemo, useRef, useState } from "react";
import { withRouter, WithRouterProps } from "react-router";
import { intl } from "#oss/utils/intl";
import clsx from "clsx";
// @ts-ignore
import DocumentTitle from "react-document-title";

import { SonarSideNav } from "#oss/exports/components/SideNav/SonarSideNav";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";
// @ts-ignore
import { JobsTable } from "dremio-ui-common/sonar/components/JobsTable/JobsTable.js";
// @ts-ignore
import { jobsPageTableColumns } from "dremio-ui-common/sonar/components/JobsTable/jobsPageTableColumns.js";
import JobsFilters from "./components/JobsFilters/JobsFilter";
import { JobsQueryParams } from "dremio-ui-common/types/Jobs.types";
import { getShowHideColumns } from "./components/JobsFilters/utils";
import localStorageUtils from "#oss/utils/storageUtils/localStorageUtils";
import { DatasetCell } from "./components/DatasetCell/DatasetCell";
import { SQLCell } from "./components/SQLCell/SQLCell";
import { JobLink } from "./components/JobLink/JobLink";
// @ts-ignore
import { ContainerSplash } from "dremio-ui-common/components/ContainerSplash.js";
import { JobSortColumns, formatJobQueryState } from "./jobs-page-utils";
import { isNotSoftware } from "dyn-load/utils/versionUtils";

import classes from "./JobsPage.module.less";
import { PageTop } from "dremio-ui-common/components/PageTop.js";
import { SearchTriggerWrapper } from "#oss/exports/searchModal/SearchTriggerWrapper";

const loadingSkeletonRows = Array(10).fill(null);

const renderDataset = (job: any) => <DatasetCell job={job} />;
const renderSQL = (sql: any) => <SQLCell sql={sql} />;
const renderJobLink = (job: any) => <JobLink job={job} />;

type JobsPageProps = {
  jobs: any[];
  jobsErr: any;
  query: JobsQueryParams | undefined;
  loadNextPage: () => void;
  runningJobs: any;
  pagesRequested: number;
  loading: boolean;
} & WithRouterProps;

const JobsPage = ({
  jobs,
  query,
  loadNextPage,
  runningJobs,
  pagesRequested,
  loading,
  router,
  location,
}: JobsPageProps) => {
  const curJobs = jobs ?? loadingSkeletonRows;
  const scrolledRef: any = useRef();
  const [manageColumns, setManageColumns] = useState(getShowHideColumns());

  // Update filters by pushing to URL
  const updateQuery = useCallback(
    (sortedColumns: Map<string, string>) => {
      const sortArray: any = Array.from(sortedColumns)[0];
      if (query) {
        router.replace({
          ...location,
          pathname: location.pathname,
          query: formatJobQueryState({
            filters: query.filters,
            sort: sortArray ? JobSortColumns[sortArray[0]] : query.sort,
            order: sortArray
              ? sortArray[1] === "descending"
                ? "DESCENDING"
                : "ASCENDING"
              : query.order,
          }),
        });
      }
    },
    [router, query, location],
  );

  const columns = useMemo(() => {
    localStorageUtils?.setJobColumns(manageColumns);
    return jobsPageTableColumns(
      renderJobLink,
      renderDataset,
      renderSQL,
      !isNotSoftware(),
    )
      .sort((a: any, b: any) => {
        const colA = manageColumns.find((col: any) => col.id === a.id);
        const colB = manageColumns.find((col: any) => col.id === b.id);
        return colA.sort > colB.sort ? 1 : -1;
      })
      .filter(
        (col: any) =>
          manageColumns.find((tCol: any) => tCol.id === col.id).selected,
      );
  }, [manageColumns]);

  const getRow = useCallback(
    (rowIndex: number) => {
      const data = curJobs?.[rowIndex];
      return {
        id: data?.id || rowIndex,
        data: data ? runningJobs?.get(data.id) || data : null,
      };
    },
    [curJobs, runningJobs],
  );

  useLayoutEffect(() => {
    if (!scrolledRef?.current && location?.state?.jobFromDetails) {
      const el = document.getElementById(location.state.jobFromDetails);
      el?.scrollIntoView?.();
      scrolledRef.current = el;
    }
  });

  return (
    <div className={clsx(classes["jobs-page"])}>
      <DocumentTitle title={intl.formatMessage({ id: "Job.Jobs" })} />
      <SonarSideNav />
      <div
        className={clsx(
          classes["jobs-page__content"],
          "dremio-layout-container --vertical",
        )}
      >
        <PageTop>
          <NavCrumbs />
          <SearchTriggerWrapper className="ml-auto" />
        </PageTop>
        <JobsFilters
          query={query}
          manageColumns={manageColumns}
          setManageColumns={setManageColumns}
        />
        <div className={classes["jobs-page__table"]}>
          {curJobs.length === 0 ? (
            <ContainerSplash title={"No jobs found"} />
          ) : (
            <JobsTable
              columns={columns}
              getRow={getRow}
              onScrolledBottom={loadNextPage}
              rowCount={!loading ? curJobs.length : pagesRequested * 100}
              onColumnsSorted={updateQuery}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default withRouter(JobsPage);
