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

import Immutable from "immutable";
import { IntlShape } from "react-intl";
import { ExploreJobsState } from "@app/reducers/explore/exploreJobs";
import { JobSummary } from "@app/exports/types/JobSummary.type";
import jobsUtils, { renderJobStatus } from "@app/utils/jobsUtils";
import timeUtils from "@app/utils/timeUtils";
import SQLCell from "./SQLCell";
import ReflectionIcon from "./ReflectionIcon";
import ColumnCell from "./ColumnCell";
import DurationCell from "./DurationCell";
import RealTimeTimer from "@app/components/RealTimeTimer";

const getFilteredSummaries = (
  jobSummaries: JobSummary[],
  queryFilter: string,
) => {
  if (!queryFilter) {
    return jobSummaries;
  }

  return jobSummaries.filter(
    (summary) =>
      summary.id
        .toLocaleLowerCase()
        .includes(queryFilter.toLocaleLowerCase()) ||
      summary.description
        .toLocaleLowerCase()
        .includes(queryFilter.toLocaleLowerCase()),
  );
};

const getExploreJobIndex = (
  jobIdArray: [string, number][],
  jobId: string,
  index: number,
) => {
  const currentJob = jobIdArray.find((job) => job[0] === jobId);

  if (currentJob?.length) {
    return currentJob[1];
  }

  return index + 1;
};

const renderSQL = (sql: string) => <SQLCell sql={sql} />;

const renderAcceleration = (isAcceleration: boolean) =>
  isAcceleration ? <ReflectionIcon isAcceleration /> : <ColumnCell />;

const renderColumn = (data: string, isNumeric: boolean, className: string) => (
  <ColumnCell data={data} isNumeric={isNumeric} className={className} />
);

const renderDuration = (duration: string | JSX.Element, isSpilled: boolean) => (
  <DurationCell
    duration={duration}
    durationDetails={Immutable.List()}
    isSpilled={isSpilled}
    isFromExplorePage
  />
);
export const getExplorePageTableData = (
  jobSummaries: ExploreJobsState["jobSummaries"],
  allJobDetails: ExploreJobsState["jobDetails"],
  jobIdArray: [string, number][],
  queryFilter: string,
  renderButtons: (
    status: string,
    jobId: string,
    jobAttempts?: number,
  ) => JSX.Element,
  intl: IntlShape,
) => {
  return getFilteredSummaries(
    Object.values(jobSummaries) as JobSummary[],
    queryFilter,
  ).map((summary, index) => {
    const currentJobDetails = allJobDetails[summary.id];

    const jobDuration = currentJobDetails?.isComplete ? (
      jobsUtils.formatJobDuration(currentJobDetails.duration, true)
    ) : (
      <RealTimeTimer
        startTime={summary.startTime}
        formatter={jobsUtils.formatJobDuration}
      />
    );

    return {
      data: {
        jobStatus: {
          node: () => renderJobStatus(summary.state),
          value: summary.state,
        },
        sql: {
          node: () => renderSQL(summary.description),
          value: summary.description,
          tabIndex: getExploreJobIndex(jobIdArray, summary.id, index),
        },
        acceleration: {
          node: () => renderAcceleration(summary.accelerated),
          value: renderAcceleration(summary.accelerated),
        },
        qt: {
          node: () =>
            renderColumn(
              intl.formatMessage({
                id:
                  currentJobDetails?.queryType === "UI_RUN"
                    ? "Job.UIRun"
                    : "Job.UIPreview",
              }),
              false,
              "fullHeight",
            ),
          value: currentJobDetails?.queryType,
        },
        st: {
          node: () =>
            renderColumn(
              timeUtils.formatTime(summary.startTime),
              true,
              "leftAlign",
            ),
          value: timeUtils.formatTime(summary.startTime),
        },
        dur: {
          node: () => renderDuration(jobDuration, summary.spilled),
          value: {
            jobDuration,
            isSpilled: summary.spilled,
          },
        },
        job: {
          node: () => renderColumn(summary.id, false, "fullHeight"),
          value: summary.id,
        },
        buttons: {
          node: () =>
            renderButtons(
              summary.state,
              summary.id,
              currentJobDetails?.attemptDetails.length,
            ),
        },
      },
    };
  });
};
