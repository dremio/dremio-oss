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

import { useCallback, useLayoutEffect, useMemo, useRef } from "react";
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
import { reflectionJobsPageTableColumns } from "dremio-ui-common/sonar/components/JobsTable/reflectionJobsPageTableColumns.js";
import { DatasetCell } from "../JobsPage/components/DatasetCell/DatasetCell";
import { JobLink } from "../JobsPage/components/JobLink/JobLink";
import { SQLCell } from "../JobsPage/components/SQLCell/SQLCell";
// @ts-ignore
import { ContainerSplash } from "dremio-ui-common/components/ContainerSplash.js";

import classes from "./ReflctionJobsPage.module.less";
import { PageTop } from "dremio-ui-common/components/PageTop.js";

const loadingSkeletonRows = Array(10).fill(null);

const renderDataset = (job: any) => <DatasetCell job={job} />;
const renderJobLink = (job: any, reflectionId: string) => (
  <JobLink job={job} reflectionId={reflectionId} />
);
const renderSQL = (sql: string) => <SQLCell sql={sql} />;

type ReflectionJobsPageProps = {
  jobs: any[];
  jobsErr: any;
  loadNextPage: () => void;
  pagesRequested: number;
  loading: boolean;
} & WithRouterProps;

export const ReflectionJobsPage = withRouter(
  ({
    jobs,
    loadNextPage,
    pagesRequested,
    loading,
    location,
    params,
  }: ReflectionJobsPageProps) => {
    const curJobs = jobs ?? loadingSkeletonRows;
    const scrolledRef: any = useRef();
    const reflectionId = params?.reflectionId;

    const columns = useMemo(() => {
      return reflectionJobsPageTableColumns(
        (job: any) => renderJobLink(job, reflectionId),
        renderDataset,
        renderSQL,
      );
    }, [reflectionId]);

    const getRow = useCallback(
      (rowIndex: number) => {
        const data = curJobs?.[rowIndex];
        return {
          id: data?.id || rowIndex,
          data: data ? data : null,
        };
      },
      [curJobs],
    );

    useLayoutEffect(() => {
      if (!scrolledRef?.current && location?.state?.jobFromDetails) {
        const el = document.getElementById(location.state.jobFromDetails);
        el?.scrollIntoView?.();
        scrolledRef.current = el;
      }
    });

    return (
      <div className={clsx(classes["reflection-jobs-page"])}>
        <DocumentTitle title={intl.formatMessage({ id: "Job.Jobs" })} />
        <SonarSideNav />
        <div
          className={clsx(
            classes["reflection-jobs-page__content"],
            "dremio-layout-container --vertical",
          )}
        >
          <PageTop>
            <NavCrumbs />
          </PageTop>
          <div className={classes["reflection-jobs-page__table"]}>
            {curJobs.length === 0 ? (
              <ContainerSplash title={"No jobs found"} />
            ) : (
              <JobsTable
                columns={columns}
                getRow={getRow}
                onScrolledBottom={loadNextPage}
                rowCount={!loading ? curJobs.length : pagesRequested * 100}
              />
            )}
          </div>
        </div>
      </div>
    );
  },
);
